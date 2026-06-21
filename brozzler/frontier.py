"""
brozzler/frontier.py - RethinkDbFrontier manages crawl jobs, sites and pages

Copyright (C) 2014-2018 Internet Archive

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import copy
import datetime
import re
import time
import uuid
from collections import defaultdict
from typing import Dict, List

import doublethink
import rethinkdb as rdb
import structlog
import urlcanon

import brozzler

r = rdb.RethinkDB()

PRIORITY_ORDER = {"high": 0, "normal": 1, "low": 2}


class UnexpectedDbResult(Exception):
    pass


def filter_claimable_site_ids(
    active_sites: List[Dict],
    reclaim_cooldown: int,
    job_priorities: Dict[str, str] = None,
    max_sites_to_claim=1,
) -> List[str]:
    job_counts = {}
    claimable_sites = []
    now = datetime.datetime.now(datetime.timezone.utc)

    for site in active_sites:
        is_claimable = False

        if not site["claimed"] and site.get("last_disclaimed", 0) <= (
            now - datetime.timedelta(seconds=reclaim_cooldown)
        ):
            is_claimable = True

        if site["claimed"]:
            heartbeat_stale = True
            if "last_heartbeat" in site and site["last_heartbeat"] is not None:
                heartbeat_stale = site["last_heartbeat"] <= (
                    now - datetime.timedelta(seconds=reclaim_cooldown * 3)
                )
            if heartbeat_stale and "last_claimed" in site and site["last_claimed"] <= (
                now - datetime.timedelta(seconds=reclaim_cooldown * 3)
            ):
                is_claimable = True

        if site["claimed"] and "last_claimed" in site and site["last_claimed"] <= (
            now - datetime.timedelta(hours=1)
        ):
            is_claimable = True

        if site["claimed"] and "max_claimed_sites" in site and "job_id" in site:
            if not is_claimable:
                job_id = site["job_id"]
                job_counts[job_id] = job_counts.get(job_id, 0) + 1

        if is_claimable:
            claimable_sites.append(site)

    if job_priorities:
        claimable_sites.sort(
            key=lambda s: PRIORITY_ORDER.get(
                job_priorities.get(s.get("job_id"), "normal"), 1
            )
        )

    site_ids_to_claim = []
    for site in claimable_sites:
        if (
            "max_claimed_sites" in site
            and "job_id" in site
            and job_counts.get(site["job_id"], 0) < site["max_claimed_sites"]
        ):
            site_ids_to_claim.append(site["id"])
            job_counts[site["job_id"]] = job_counts.get(site["job_id"], 0) + 1

        if "max_claimed_sites" not in site or "job_id" not in site:
            site_ids_to_claim.append(site["id"])

        if len(site_ids_to_claim) >= max_sites_to_claim:
            break

    return site_ids_to_claim


class RethinkDbFrontier:
    logger = structlog.get_logger(logger_name=__module__ + "." + __qualname__)

    def __init__(self, rr, shards=None, replicas=None):
        self.rr = rr
        self.shards = shards or len(rr.servers)
        self.replicas = replicas or min(len(rr.servers), 3)
        self._ensure_db()

    def _ensure_db(self):
        db_logger = self.logger.bind(dbname=self.rr.dbname)

        dbs = self.rr.db_list().run()
        if self.rr.dbname not in dbs:
            db_logger.info("creating rethinkdb database")
            self.rr.db_create(self.rr.dbname).run()
        tables = self.rr.table_list().run()
        if "sites" not in tables:
            db_logger.info("creating rethinkdb table 'sites' in database")
            self.rr.table_create(
                "sites", shards=self.shards, replicas=self.replicas
            ).run()
            self.rr.table("sites").index_create(
                "sites_last_disclaimed", [r.row["status"], r.row["last_disclaimed"]]
            ).run()
            self.rr.table("sites").index_create("job_id").run()
        if "pages" not in tables:
            db_logger.info("creating rethinkdb table 'pages' in database")
            self.rr.table_create(
                "pages", shards=self.shards, replicas=self.replicas
            ).run()
            self.rr.table("pages").index_create(
                "priority_by_site",
                [
                    r.row["site_id"],
                    r.row["brozzle_count"],
                    r.row["claimed"],
                    r.row["priority"],
                ],
            ).run()
            self.rr.table("pages").index_create(
                "least_hops",
                [r.row["site_id"], r.row["brozzle_count"], r.row["hops_from_seed"]],
            ).run()
            self.rr.table("pages").index_create(
                "pending_by_site",
                [r.row["site_id"], r.row["pending"], r.row["pending_since"]],
            ).run()
        if "jobs" not in tables:
            db_logger.info("creating rethinkdb table 'jobs' in database")
            self.rr.table_create(
                "jobs", shards=self.shards, replicas=self.replicas
            ).run()
        if "behavior_executions" not in tables:
            db_logger.info("creating rethinkdb table 'behavior_executions'")
            self.rr.table_create(
                "behavior_executions", shards=self.shards, replicas=self.replicas
            ).run()
            self.rr.table("behavior_executions").index_create(
                "by_job_url_regex", [r.row["job_id"], r.row["url_regex"], r.row["timestamp"]]
            ).run()
            self.rr.table("behavior_executions").index_create(
                "by_site", [r.row["site_id"], r.row["timestamp"]]
            ).run()
        if "disabled_behaviors" not in tables:
            db_logger.info("creating rethinkdb table 'disabled_behaviors'")
            self.rr.table_create(
                "disabled_behaviors", shards=self.shards, replicas=self.replicas
            ).run()
            self.rr.table("disabled_behaviors").index_create(
                "by_job_regex", [r.row["job_id"], r.row["url_regex"]]
            ).run()
        if "qps_stats" not in tables:
            db_logger.info("creating rethinkdb table 'qps_stats'")
            self.rr.table_create(
                "qps_stats", shards=self.shards, replicas=self.replicas
            ).run()

    def _vet_result(self, result, **kwargs):
        # self.logger.debug("vetting expected=%s result=%s", kwargs, result)
        # {'replaced': 0, 'errors': 0, 'skipped': 0, 'inserted': 1, 'deleted': 0, 'generated_keys': ['292859c1-4926-4b27-9d87-b2c367667058'], 'unchanged': 0}
        for k in ["replaced", "errors", "skipped", "inserted", "deleted", "unchanged"]:
            if k in kwargs:
                expected = kwargs[k]
            else:
                expected = 0
            if isinstance(expected, list):
                if result.get(k) not in kwargs[k]:
                    raise UnexpectedDbResult(
                        "expected %r to be one of %r in %r" % (k, expected, result)
                    )
            else:
                if result.get(k) != expected:
                    raise UnexpectedDbResult(
                        "expected %r to be %r in %r" % (k, expected, result)
                    )

    def get_active_sites(self) -> List[Dict]:
        active_sites = (
            self.rr.table("sites", read_mode="majority")
            .between(
                ["ACTIVE", r.minval],
                ["ACTIVE", r.maxval],
                index="sites_last_disclaimed",
            )
            .pluck(
                "id",
                "last_disclaimed",
                "claimed",
                "last_claimed",
                "last_heartbeat",
                "job_id",
                "max_claimed_sites",
            )
            .order_by(r.desc("claimed"), "last_disclaimed")
            .run()
        )
        return active_sites

    def _get_job_priorities(self) -> Dict[str, str]:
        jobs = self.rr.table("jobs").pluck("id", "priority").run()
        return {j["id"]: j.get("priority", "normal") for j in jobs}

    def claim_sites(self, n=1, reclaim_cooldown=20, worker_id=None) -> List[Dict]:
        self.logger.debug("claiming up to %s sites to brozzle", n)

        active_sites = self.get_active_sites()
        job_priorities = self._get_job_priorities()
        site_ids_to_claim = filter_claimable_site_ids(
            active_sites, reclaim_cooldown, job_priorities=job_priorities, max_sites_to_claim=n
        )
        update_data = {
            "claimed": True,
            "last_claimed": r.now(),
            "claimed_since": r.now(),
        }
        if worker_id:
            update_data["last_claimed_by"] = worker_id
            update_data["last_heartbeat"] = r.now()

        stale_threshold = r.now().sub(reclaim_cooldown * 3)
        result = (
            self.rr.table("sites", read_mode="majority")
            .get_all(r.args(site_ids_to_claim))
            .update(
                r.branch(
                    r.or_(
                        r.row["claimed"].not_(),
                        r.row["last_claimed"].lt(r.now().sub(60 * 60)),
                        r.and_(
                            r.row["claimed"],
                            r.or_(
                                r.row.has_fields("last_heartbeat").not_(),
                                r.row["last_heartbeat"].lt(stale_threshold),
                            ),
                        ),
                    ),
                    update_data,
                    {},
                ),
                return_changes=True,
            )
        ).run()

        self._vet_result(
            result, replaced=list(range(n + 1)), unchanged=list(range(n + 1))
        )
        sites = []
        for i in range(result["replaced"]):
            if result["changes"][i]["old_val"]["claimed"]:
                self.logger.warning(
                    "re-claimed site that was still marked 'claimed' "
                    "because it was last claimed a long time ago, "
                    "and presumably some error stopped it from "
                    "being disclaimed",
                    last_claimed=result["changes"][i]["old_val"]["last_claimed"],
                )
            site = brozzler.Site(self.rr, result["changes"][i]["new_val"])
            sites.append(site)
        self.logger.debug("claimed %s sites", len(sites))
        if sites:
            return sites
        else:
            raise brozzler.NothingToClaim

    def heartbeat_site(self, site_id, worker_id=None):
        updates = {"last_heartbeat": r.now()}
        if worker_id:
            updates["last_claimed_by"] = worker_id
        result = (
            self.rr.table("sites")
            .get(site_id)
            .update(updates)
            .run()
        )
        return result

    def enforce_time_limit(self, site):
        """
        Raises `brozzler.ReachedTimeLimit` if appropriate.
        """
        if site.time_limit and site.time_limit > 0 and site.elapsed() > site.time_limit:
            self.logger.debug(
                "site FINISHED_TIME_LIMIT!",
                time_limit=site.time_limit,
                elapsed=site.elapsed(),
                site=site,
            )
            raise brozzler.ReachedTimeLimit

    def claim_page(self, site, worker_id):
        result = (
            self.rr.table("pages")
            .between(
                [site.id, 0, r.minval, r.minval],
                [site.id, 0, r.maxval, r.maxval],
                index="priority_by_site",
            )
            .order_by(index=r.desc("priority_by_site"))
            .filter(
                lambda page: r.and_(
                    r.or_(
                        page.has_fields("retry_after").not_(), r.now() > page["retry_after"]
                    ),
                    page.has_fields("pending").not_().or_(page["pending"] == False),
                )
            )
            .limit(1)
            .update(
                {"claimed": True, "last_claimed_by": worker_id, "status": "ACTIVE"},
                return_changes="always",
            )
            .run()
        )
        self._vet_result(result, unchanged=[0, 1], replaced=[0, 1])
        if result["unchanged"] == 0 and result["replaced"] == 0:
            raise brozzler.NothingToClaim
        else:
            return brozzler.Page(self.rr, result["changes"][0]["new_val"])

    def has_outstanding_pages(self, site):
        results_iter = (
            self.rr.table("pages")
            .between(
                [site.id, 0, r.minval, r.minval],
                [site.id, 0, r.maxval, r.maxval],
                index="priority_by_site",
            )
            .limit(1)
            .run()
        )
        return len(list(results_iter)) > 0

    def completed_page(self, site, page):
        page.brozzle_count += 1
        page.claimed = False
        if page.status != "RETRY":
            page.status = "PASSED"
        page.save()
        if page.redirect_url and page.hops_from_seed == 0:
            site.note_seed_redirect(page.redirect_url)
            site.save()

    def active_jobs(self):
        results = self.rr.table("jobs").filter({"status": "ACTIVE"}).run()
        for result in results:
            yield brozzler.Job(self.rr, result)

    def honor_stop_request(self, site):
        """Raises brozzler.CrawlStopped if stop has been requested."""
        site.refresh()
        if site.stop_requested and site.stop_requested <= doublethink.utcnow():
            self.logger.info("stop requested for site", site_id=site.id)
            raise brozzler.CrawlStopped

        if site.job_id:
            job = brozzler.Job.load(self.rr, site.job_id)
            if (
                job
                and job.stop_requested
                and job.stop_requested <= doublethink.utcnow()
            ):
                self.logger.info("stop requested for job", job_id=site.job_id)
                raise brozzler.CrawlStopped

    def _maybe_finish_job(self, job_id):
        """Returns True if job is finished."""
        job = brozzler.Job.load(self.rr, job_id)
        if not job:
            return False
        if job.status.startswith("FINISH"):
            self.logger.warning("%s is already %s", job, job.status)
            return True

        results = self.rr.table("sites").get_all(job_id, index="job_id").run()
        n = 0
        for result in results:
            site = brozzler.Site(self.rr, result)
            if not site.status.startswith("FINISH"):
                results.close()
                return False
            n += 1

        self.logger.info("all %s sites finished, job is FINISHED!", n, job_id=job.id)
        job.finish()
        job.save()
        return True

    def finished(self, site, status):
        self.logger.info("%s %s", status, site)
        site.status = status
        site.claimed = False
        site.last_disclaimed = doublethink.utcnow()
        site.starts_and_stops[-1]["stop"] = doublethink.utcnow()
        site.save()
        if status == "FINISHED_STOP_REQUESTED":
            self.abort_pending_pages(site=site)
            if site.job_id:
                self.abort_pending_pages(job_id=site.job_id)
        if site.job_id:
            self._maybe_finish_job(site.job_id)

    def disclaim_site(self, site, page=None):
        self.logger.info("disclaiming", site=site)
        site.claimed = False
        site.last_disclaimed = doublethink.utcnow()
        if page:
            page.claimed = False
            page.save()
        job = None
        if site.job_id:
            job = brozzler.Job.load(self.rr, site.job_id)
        self.release_pending_pages(site, job=job)
        if not page and not self.has_outstanding_pages(site):
            self.finished(site, "FINISHED")
        else:
            site.save()

    def resume_job(self, job):
        job.status = "ACTIVE"
        job.stop_requested = None
        job.starts_and_stops.append({"start": doublethink.utcnow(), "stop": None})
        job.save()
        for site in self.job_sites(job.id):
            site.status = "ACTIVE"
            site.starts_and_stops.append({"start": doublethink.utcnow(), "stop": None})
            site.save()

    def resume_site(self, site):
        if site.job_id:
            # can't call resume_job since that would resume jobs's other sites
            job = brozzler.Job.load(self.rr, site.job_id)
            job.status = "ACTIVE"
            site.stop_requested = None
            job.starts_and_stops.append({"start": doublethink.utcnow(), "stop": None})
            job.save()
        site.status = "ACTIVE"
        site.starts_and_stops.append({"start": doublethink.utcnow(), "stop": None})
        site.save()

    def _build_fresh_page(self, site, parent_page, url, hops_off=0):
        url_for_crawling = urlcanon.whatwg(url)
        hashtag = (url_for_crawling.hash_sign + url_for_crawling.fragment).decode(
            "utf-8"
        )
        urlcanon.canon.remove_fragment(url_for_crawling)
        page = brozzler.Page(
            self.rr,
            {
                "url": str(url_for_crawling),
                "site_id": site.id,
                "job_id": site.job_id,
                "hops_from_seed": parent_page.hops_from_seed + 1,
                "hop_path": str(parent_page.hop_path if parent_page.hop_path else "")
                + "L",
                "via_page_id": parent_page.id,
                "via_page_url": parent_page.url,
                "hops_off_surt": hops_off,
                "hashtags": [hashtag] if hashtag else [],
            },
        )
        return page

    def _merge_page(self, existing_page, fresh_page):
        """
        Utility method for merging info from `brozzler.Page` instances
        representing the same url but with possibly different metadata.
        """
        existing_page.priority += fresh_page.priority
        existing_page.hashtags = list(
            set((existing_page.hashtags or []) + (fresh_page.hashtags or []))
        )
        existing_page.hops_off = min(existing_page.hops_off, fresh_page.hops_off)

    def _scope_and_enforce_robots(self, site, parent_page, outlinks):
        """
        Returns tuple (
            dict of {page_id: Page} of fresh `brozzler.Page` representing in
                scope links accepted by robots policy,
            set of in scope urls (canonicalized) blocked by robots policy,
            set of out-of-scope urls (canonicalized)).
        """
        pages = {}  # {page_id: Page, ...}
        blocked = set()
        out_of_scope = set()
        for url in outlinks or []:
            url_for_scoping = urlcanon.semantic(url)
            url_for_crawling = urlcanon.whatwg(url)
            decision = site.accept_reject_or_neither(
                url_for_scoping, parent_page=parent_page
            )
            if decision is True:
                hops_off = 0
            elif decision is None:
                decision = parent_page.hops_off < site.scope.get("max_hops_off", 0)
                hops_off = parent_page.hops_off + 1
            if decision is True:
                if brozzler.is_permitted_by_robots(site, str(url_for_crawling)):
                    fresh_page = self._build_fresh_page(
                        site, parent_page, url, hops_off
                    )
                    if fresh_page.id in pages:
                        self._merge_page(pages[fresh_page.id], fresh_page)
                    else:
                        pages[fresh_page.id] = fresh_page
                else:
                    blocked.add(str(url_for_crawling))
            else:
                out_of_scope.add(str(url_for_crawling))
        return pages, blocked, out_of_scope

    def scope_and_schedule_outlinks(self, site, parent_page, outlinks):
        decisions = {"accepted": set(), "blocked": set(), "rejected": set()}
        counts = {"added": 0, "updated": 0, "rejected": 0, "blocked": 0, "pending": 0}

        fresh_pages, blocked, out_of_scope = self._scope_and_enforce_robots(
            site, parent_page, outlinks
        )
        decisions["blocked"] = blocked
        decisions["rejected"] = out_of_scope
        counts["blocked"] += len(blocked)
        counts["rejected"] += len(out_of_scope)

        results = self.rr.table("pages").get_all(*fresh_pages.keys()).run()
        pages = {doc["id"]: brozzler.Page(self.rr, doc) for doc in results}

        for fresh_page in fresh_pages.values():
            decisions["accepted"].add(fresh_page.url)
            if fresh_page.id in pages:
                page = pages[fresh_page.id]
                self._merge_page(page, fresh_page)
                counts["updated"] += 1
            else:
                pages[fresh_page.id] = fresh_page
                counts["added"] += 1

        if parent_page.id in pages:
            self._merge_page(parent_page, pages[parent_page.id])
            del pages[parent_page.id]

        job = None
        if site.job_id:
            job = brozzler.Job.load(self.rr, site.job_id)

        ready_pages = []
        pending_pages = []
        for page in pages.values():
            ok, _ = self.check_rate_limit(site, job)
            if ok:
                ready_pages.append(page)
                self.record_page_scheduled(site, job)
            else:
                pending_pages.append(page)
                counts["pending"] += 1

        pages_list = ready_pages
        for batch in (pages_list[i : i + 50] for i in range(0, len(pages_list), 50)):
            try:
                self.logger.debug("inserting/replacing batch of %s pages", len(batch))
                reql = self.rr.table("pages").insert(batch, conflict="replace")
                self.logger.debug(
                    'running query self.rr.table("pages").insert(%r, '
                    'conflict="replace")',
                    batch,
                )
                reql.run()
            except Exception:
                self.logger.exception(
                    "problem inserting/replacing batch of %s pages",
                    len(batch),
                )

        if pending_pages:
            self.add_to_pending(site, pending_pages)

        parent_page.outlinks = {}
        for k in decisions:
            parent_page.outlinks[k] = list(decisions[k])
        parent_page.save()

        self.logger.info(
            "%s new links added, %s existing links updated, %s links "
            "rejected, %s links blocked by robots, %s links pending from %s",
            counts["added"],
            counts["updated"],
            counts["rejected"],
            counts["blocked"],
            counts["pending"],
            parent_page,
        )

    def reached_limit(self, site, e, page=None):
        self.logger.info("reached_limit", site=site, e=e)
        assert isinstance(e, brozzler.ReachedLimit)
        if page is not None:
            if hasattr(e, "warcprox_meta") and e.warcprox_meta:
                page.warcprox_meta_snapshot = copy.deepcopy(e.warcprox_meta)
            retry_delay = min(135, 60 * (1.5 ** (page.failed_attempts or 0)))
            page.retry_after = doublethink.utcnow() + datetime.timedelta(
                seconds=retry_delay
            )
            page.failed_attempts = (page.failed_attempts or 0) + 1
            page.status = "RETRY"
            page.claimed = False
            page.save()
            self.logger.info(
                "marked page for retry after warcprox 420",
                page=page,
                retry_after=page.retry_after,
            )
        else:
            if (
                site.reached_limit
                and site.reached_limit != e.warcprox_meta["reached-limit"]
            ):
                self.logger.warning(
                    "reached limit %s but site had already reached limit %s",
                    e.warcprox_meta["reached-limit"],
                    self.reached_limit,
                )
            else:
                site.reached_limit = e.warcprox_meta["reached-limit"]
                self.finished(site, "FINISHED_REACHED_LIMIT")

    def job_sites(self, job_id):
        results = self.rr.table("sites").get_all(job_id, index="job_id").run()
        for result in results:
            yield brozzler.Site(self.rr, result)

    def seed_page(self, site_id):
        results = (
            self.rr.table("pages")
            .between(
                [site_id, r.minval, r.minval, r.minval],
                [site_id, r.maxval, r.maxval, r.maxval],
                index="priority_by_site",
            )
            .filter({"hops_from_seed": 0})
            .run()
        )
        pages = list(results)
        if len(pages) > 1:
            self.logger.warning("more than one seed page?", site_id=site_id)
        if len(pages) < 1:
            return None
        return brozzler.Page(self.rr, pages[0])

    def site_pages(self, site_id, brozzled=None):
        """
        Args:
            site_id (str or int):
            brozzled (bool): if true, results include only pages that have
                been brozzled at least once; if false, only pages that have
                not been brozzled; and if None (the default), all pages
        Returns:
            iterator of brozzler.Page
        """
        query = self.rr.table("pages").between(
            [site_id, 1 if brozzled is True else 0, r.minval, r.minval],
            [site_id, 0 if brozzled is False else r.maxval, r.maxval, r.maxval],
            index="priority_by_site",
        )
        self.logger.debug("running query", query=query)
        results = query.run()
        for result in results:
            self.logger.debug("yielding result", result=result)
            yield brozzler.Page(self.rr, result)

    def _update_qps_stats(self, entity_type, entity_id):
        now = time.time()
        window_start = now - 1.0
        result = (
            self.rr.table("qps_stats")
            .get_all([entity_type, entity_id])
            .update(
                lambda row: {
                    "timestamps": row["timestamps"].filter(
                        lambda t: t > window_start
                    ).append(now),
                    "last_update": now,
                },
                non_atomic=True,
            )
            .run()
        )
        if result.get("skipped", 0) > 0 or result.get("replaced", 0) == 0:
            self.rr.table("qps_stats").insert(
                {
                    "id": [entity_type, entity_id],
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "timestamps": [now],
                    "last_update": now,
                }
            ).run()

    def _get_current_qps(self, entity_type, entity_id):
        now = time.time()
        window_start = now - 1.0
        result = (
            self.rr.table("qps_stats").get([entity_type, entity_id]).run()
        )
        if not result:
            return 0
        timestamps = [t for t in result.get("timestamps", []) if t > window_start]
        return len(timestamps)

    def check_rate_limit(self, site, job=None):
        per_site_qps = site.get("per_site_qps")
        if per_site_qps:
            current_site_qps = self._get_current_qps("site", site.id)
            if current_site_qps >= per_site_qps:
                return False, "site_qps"
        if job and job.get("qps_limit"):
            current_job_qps = self._get_current_qps("job", job.id)
            if current_job_qps >= job.qps_limit:
                return False, "job_qps"
        return True, None

    def record_page_scheduled(self, site, job=None):
        self._update_qps_stats("site", site.id)
        if job and job.id:
            self._update_qps_stats("job", job.id)

    def add_to_pending(self, site, pages):
        if not pages:
            return
        now = doublethink.utcnow()
        for page in pages:
            page.pending = True
            page.pending_since = now
            page.status = "PENDING"
        pages_list = list(pages) if isinstance(pages, (list, tuple)) else [pages]
        for batch in (pages_list[i : i + 50] for i in range(0, len(pages_list), 50)):
            self.rr.table("pages").insert(batch, conflict="replace").run()
        self.logger.info(
            "added %s pages to pending queue for site %s", len(pages_list), site.id
        )

    def release_pending_pages(self, site, job=None, max_release=None):
        result = (
            self.rr.table("pages")
            .between([site.id, True, r.minval], [site.id, True, r.maxval], index="pending_by_site")
            .order_by(index="pending_by_site")
            .limit(max_release or 100)
            .run()
        )
        pending_pages = list(result)
        released = []
        for page_doc in pending_pages:
            page = brozzler.Page(self.rr, page_doc)
            ok, _ = self.check_rate_limit(site, job)
            if not ok:
                break
            page.pending = False
            page.pending_since = None
            page.status = "QUEUED"
            page.save()
            self.record_page_scheduled(site, job)
            released.append(page)
        if released:
            self.logger.info(
                "released %s pending pages for site %s", len(released), site.id
            )
        return released

    def abort_pending_pages(self, site=None, job_id=None):
        updates = {"pending": False, "status": "ABORTED"}
        if site:
            result = (
                self.rr.table("pages")
                .between([site.id, True, r.minval], [site.id, True, r.maxval], index="pending_by_site")
                .update(updates)
                .run()
            )
            self.logger.info(
                "aborted %s pending pages for site %s",
                result.get("replaced", 0),
                site.id,
            )
        elif job_id:
            site_ids = list(
                self.rr.table("sites").get_all(job_id, index="job_id")["id"].run()
            )
            for site_id in site_ids:
                result = (
                    self.rr.table("pages")
                    .between([site_id, True, r.minval], [site_id, True, r.maxval], index="pending_by_site")
                    .update(updates)
                    .run()
                )
                self.logger.info(
                    "aborted %s pending pages for site %s",
                    result.get("replaced", 0),
                    site_id,
                )

    def is_behavior_disabled(self, url_regex, job_id=None):
        now = doublethink.utcnow()
        query = self.rr.table("disabled_behaviors")
        if job_id:
            query = query.get_all([job_id, url_regex], index="by_job_regex")
        else:
            query = query.filter({"url_regex": url_regex})
        results = list(query.run())
        for r_doc in results:
            if r_doc.get("disabled_until", now) > now:
                return True, r_doc
        return False, None

    def record_behavior_execution(
        self,
        job_id,
        site_id,
        page_url,
        url_regex,
        duration_ms,
        selectors_matched=0,
        click_count=0,
        success=False,
        error_message=None,
    ):
        execution = brozzler.model.BehaviorExecution(
            self.rr,
            {
                "job_id": job_id,
                "site_id": site_id,
                "page_url": page_url,
                "url_regex": url_regex,
                "duration_ms": duration_ms,
                "selectors_matched": selectors_matched,
                "click_count": click_count,
                "success": success,
                "error_message": error_message,
            },
        )
        execution.save()
        self._check_and_disable_behavior_if_needed(job_id, url_regex)
        return execution

    def _check_and_disable_behavior_if_needed(self, job_id, url_regex):
        now = doublethink.utcnow()
        results = list(
            self.rr.table("behavior_executions")
            .between(
                [job_id, url_regex, r.minval],
                [job_id, url_regex, r.maxval],
                index="by_job_url_regex",
            )
            .order_by(index=r.desc("by_job_url_regex"))
            .limit(5)
            .run()
        )
        if len(results) < 5:
            return
        success_rates = []
        recent_rates = []
        window_results = results
        for i in range(len(window_results)):
            window = window_results[i:]
            if len(window) < 5:
                continue
            successes = sum(1 for r in window if r.get("success"))
            rate = successes / len(window)
            success_rates.append(rate)
            recent_rates.append(rate)
        if len(recent_rates) >= 1 and all(rate < 0.5 for rate in recent_rates[-1:]):
            last_5 = window_results[:5]
            rate = sum(1 for r in last_5 if r.get("success")) / 5
            if rate < 0.5:
                disabled, _ = self.is_behavior_disabled(url_regex, job_id)
                if not disabled:
                    disabled_behavior = brozzler.model.DisabledBehavior(
                        self.rr,
                        {
                            "job_id": job_id,
                            "url_regex": url_regex,
                            "disabled_at": now,
                            "disabled_until": now + datetime.timedelta(hours=24),
                            "reason": "success rate %.2f below 50%% for 5 consecutive runs" % rate,
                            "recent_success_rates": recent_rates,
                        },
                    )
                    disabled_behavior.save()
                    self.logger.warning(
                        "disabled behavior %s for job %s due to low success rate",
                        url_regex,
                        job_id,
                    )

    def get_behavior_success_rates(self, job_id, top_n=10):
        pipeline = (
            self.rr.table("behavior_executions")
            .between(
                [job_id, r.minval, r.minval],
                [job_id, r.maxval, r.maxval],
                index="by_job_url_regex",
            )
            .group("url_regex")
            .ungroup()
            .map(
                lambda g: {
                    "url_regex": g["group"],
                    "total": g["reduction"].count(),
                    "successes": g["reduction"].filter(lambda r: r["success"]).count(),
                }
            )
            .run()
        )
        results = []
        for row in pipeline:
            total = row["total"]
            successes = row["successes"]
            rate = successes / total if total > 0 else 0.0
            results.append(
                {
                    "url_regex": row["url_regex"],
                    "total_count": total,
                    "successes": successes,
                    "success_rate": rate,
                }
            )
        results.sort(key=lambda x: x["success_rate"])
        return results[:top_n]

    def get_disabled_behaviors(self, job_id=None):
        query = self.rr.table("disabled_behaviors")
        if job_id:
            query = query.filter({"job_id": job_id})
        now = doublethink.utcnow()
        results = list(query.run())
        return [r for r in results if r.get("disabled_until", now) > now]

    def replay_behavior(
        self, job_id, url_regex, test_site_id=None, sample_url=None, outlinks=None
    ):
        """
        Record the result of a behavior replay into the test frontier.
        The actual browser execution happens in the CLI layer (brozzler_replay_behavior).

        Args:
            job_id: target job id
            url_regex: the behavior regex that was matched
            test_site_id: optional existing test site id to write outlinks to
            sample_url: the URL that was browsed
            outlinks: iterable of outlink URLs collected during the replay

        Returns:
            dict with behavior info, test_site_id, outlinks count
        """
        job = brozzler.Job.load(self.rr, job_id)
        if not job:
            raise ValueError("job not found: %s" % job_id)
        target_behavior = None
        for behavior in brozzler.behaviors():
            if behavior.get("url_regex") == url_regex:
                target_behavior = behavior
                break
        if not target_behavior:
            raise ValueError("behavior not found for regex: %s" % url_regex)

        if test_site_id:
            test_site = brozzler.Site.load(self.rr, test_site_id)
            if not test_site:
                raise ValueError("test site not found: %s" % test_site_id)
        else:
            test_site = brozzler.Site(
                self.rr,
                {
                    "seed": sample_url
                    or ("test-replay://%s" % url_regex.replace("\\", "")),
                    "job_id": job_id,
                    "test_replay": True,
                    "replayed_url_regex": url_regex,
                },
            )
            test_site.id = str(uuid.uuid4())
            brozzler.new_site(self, test_site)
            test_site_id = test_site.id

        outlinks_list = list(outlinks or [])
        if outlinks_list and sample_url:
            fake_page = brozzler.Page(
                self.rr,
                {
                    "id": "replay-%s-%s"
                    % (
                        url_regex.replace("\\", "").replace("/", "_")[:50],
                        doublethink.utcnow().strftime("%Y%m%d%H%M%S"),
                    ),
                    "site_id": test_site_id,
                    "url": sample_url,
                    "hops_from_seed": 0,
                    "brozzle_count": 0,
                },
            )
            try:
                self.scope_and_schedule_outlinks(test_site, fake_page, outlinks_list)
            except Exception:
                self.logger.exception(
                    "failed to scope/schedule replay outlinks",
                    test_site_id=test_site_id,
                )

        self.logger.info(
            "replay behavior recorded",
            url_regex=url_regex,
            job_id=job_id,
            test_site_id=test_site_id,
            outlinks_count=len(outlinks_list),
        )
        return {
            "behavior": target_behavior,
            "job_id": job_id,
            "url_regex": url_regex,
            "sample_url": sample_url,
            "test_site_id": test_site_id,
            "outlinks": outlinks_list,
            "outlinks_count": len(outlinks_list),
        }
