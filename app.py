"""
Flask dashboard for the Maps Data scraper pipeline.
Run: python app.py  →  http://localhost:5000
"""

import csv
import io
import logging
import os
import queue
import re
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from zoneinfo import ZoneInfo
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, render_template, request

from city_parse import formatted_address_from_item, resolve_city
from email_extract import USER_AGENT as ENRICH_USER_AGENT, env_int, scrape_email_for_website
from maps_item import contact_fields_from_maps_item, iter_search_results
from zip_geocode import us_zip_latlng
from geo_zip import best_listing_zip, listing_matches_search_zip, normalize_zip5
from state_zips import STATE_NAMES, STATE_ZIPS

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
# Use /data (Railway persistent volume) when mounted, otherwise current directory.
_DB_DIR = "/data" if os.path.isdir("/data") else "."
DB_PATH = os.environ.get("DB_PATH", os.path.join(_DB_DIR, "businesses.db"))
_EASTERN = ZoneInfo("America/New_York")


def _now_eastern() -> str:
    return datetime.now(tz=_EASTERN).strftime("%Y-%m-%d %H:%M:%S")
API_HOST    = "maps-data.p.rapidapi.com"
API_BASE    = f"https://{API_HOST}"
PAGE_SIZE   = 20
MAX_PAGES   = 10
MAX_RETRIES = 6
JOB_WORKERS = 5   # concurrent zips per job
ENRICH_WORKERS = env_int("ENRICH_WORKERS", 20)
MX_WORKERS = env_int("MX_WORKERS", 20)
APPEND_ZIP_TO_QUERY = os.environ.get("APPEND_ZIP_TO_QUERY", "1").strip().lower() not in ("0", "false", "no", "off")
# When false (default), do not store info@ / hello@ / etc. if that is all the site exposes.
EMAIL_ALLOW_GENERIC_FALLBACK = os.environ.get("EMAIL_ALLOW_GENERIC_FALLBACK", "0").strip().lower() in (
    "1", "true", "yes", "on",
)
# When false (default), Gmail/Yahoo/etc. stay "clean" — many small businesses use them.
FLAG_FREE_EMAIL_DOMAINS = os.environ.get("FLAG_FREE_EMAIL_DOMAINS", "0").strip().lower() in (
    "1", "true", "yes", "on",
)
# Used only when FLAG_FREE_EMAIL_DOMAINS=1 — keep in sync with email_extract.FREE_EMAIL_PROVIDER_DOMAINS
PERSONAL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "me.com", "mac.com", "live.com", "msn.com", "protonmail.com", "proton.me",
    "googlemail.com", "ymail.com", "gmx.com", "gmx.net", "mail.com", "zoho.com",
}
DETAIL_ENDPOINTS = (
    "/place.php",
    "/place-details.php",
    "/placedetails.php",
    "/place_details.php",
)
DETAIL_PARAM_KEYS = ("place_id", "google_id", "cid")

# Campaign vertical inferred from the search keyword when the caller omits one.
VERTICAL_MAP = {
    "impact windows": "windows", "hurricane shutters": "windows",
    "window replacement": "windows", "impact doors": "windows",
    "plumber": "plumbing", "emergency plumber": "plumbing",
    "hvac contractor": "hvac", "air conditioning repair": "hvac",
    "heating contractor": "hvac", "ac repair": "hvac",
    "window blinds": "blinds", "window treatments": "blinds",
    "plantation shutters": "blinds",
    "kitchen remodeling": "remodeling", "bathroom remodeling": "remodeling",
    "home remodeling contractor": "remodeling",
}


def vertical_for_keyword(keyword: str) -> str:
    """Map a search keyword onto a campaign vertical; unmapped keywords pass through."""
    k = (keyword or "").strip()
    return VERTICAL_MAP.get(k.lower(), k)

# ── In-memory job store ───────────────────────────────────────────────────────
_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()
MAX_JOB_EVENTS  = 200
MAX_STORED_JOBS = 30
_job_queue: queue.Queue = queue.Queue()   # (job_id, api_key) tuples

# Bulk stage actions run as background jobs on the same worker as scrapes.
JOB_TYPE_SCRAPE = "scrape"
JOB_TYPE_BULK_ENRICH = "bulk_enrich"
JOB_TYPE_BULK_CLEAN = "bulk_clean"
_BULK_JOB_TYPES = frozenset({JOB_TYPE_BULK_ENRICH, JOB_TYPE_BULK_CLEAN})
# Rows per commit / progress event. Also the granularity at which a running
# bulk job notices a cancel request.
PROGRESS_CHUNK_ROWS = env_int("PROGRESS_CHUNK_ROWS", 100)
# A capped batch this small finishes well inside an HTTP timeout, so
# /api/pipeline/advance still answers inline for test-sized runs.
BULK_INLINE_MAX_ROWS = env_int("BULK_INLINE_MAX_ROWS", 500)

# ── Place-details probing state (shared across jobs and zips) ─────────────────
_details_cache: dict[str, dict | None] = {}
_details_lock = threading.Lock()
_DETAIL_COMBO: tuple[str, str] | None = None   # (endpoint, param key) once one works
MAX_DETAILS_CACHE = 5000

# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS businesses (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            business_name TEXT,
            address       TEXT,
            phone         TEXT,
            website_url   TEXT,
            email         TEXT,
            rating        REAL,
            review_count  INTEGER,
            category      TEXT,
            zip_code      TEXT,
            search_zip    TEXT,
            city          TEXT,
            created_at    TEXT DEFAULT (datetime('now')),
            pipeline_stage TEXT DEFAULT 'scraped',
            stage_reason   TEXT,
            enriched_at    TEXT,
            cleaned_at     TEXT
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_phone_url
        ON businesses (phone, website_url)
        WHERE phone IS NOT NULL OR website_url IS NOT NULL
    """)
    # Migrate existing DBs that lack the email column
    cols = {row[1] for row in conn.execute("PRAGMA table_info(businesses)")}
    city_was_new = "city" not in cols
    for col, ddl in {
        "email": "ALTER TABLE businesses ADD COLUMN email TEXT",
        "city": "ALTER TABLE businesses ADD COLUMN city TEXT",
        "search_zip": "ALTER TABLE businesses ADD COLUMN search_zip TEXT",
        "pipeline_stage": "ALTER TABLE businesses ADD COLUMN pipeline_stage TEXT DEFAULT 'scraped'",
        "stage_reason": "ALTER TABLE businesses ADD COLUMN stage_reason TEXT",
        "enriched_at": "ALTER TABLE businesses ADD COLUMN enriched_at TEXT",
        "cleaned_at": "ALTER TABLE businesses ADD COLUMN cleaned_at TEXT",
        "created_at": "ALTER TABLE businesses ADD COLUMN created_at TEXT",
        # Which campaign produced the row — `category` holds Maps' types[0], which
        # does not map onto verticals.
        "search_keyword": "ALTER TABLE businesses ADD COLUMN search_keyword TEXT",
        "vertical": "ALTER TABLE businesses ADD COLUMN vertical TEXT",
        # Ad-tech fingerprints captured during the enrichment crawl.
        "runs_google_ads": "ALTER TABLE businesses ADD COLUMN runs_google_ads INTEGER",
        "aw_ids": "ALTER TABLE businesses ADD COLUMN aw_ids TEXT",
        "call_tracking": "ALTER TABLE businesses ADD COLUMN call_tracking TEXT",
        "has_gtm": "ALTER TABLE businesses ADD COLUMN has_gtm INTEGER",
        "has_ga4": "ALTER TABLE businesses ADD COLUMN has_ga4 INTEGER",
        # Every candidate address, not just the one chosen by pick_best_email().
        "all_emails": "ALTER TABLE businesses ADD COLUMN all_emails TEXT",
    }.items():
        if col not in cols:
            conn.execute(ddl)
    conn.execute("UPDATE businesses SET pipeline_stage='scraped' WHERE pipeline_stage IS NULL OR pipeline_stage=''")
    if city_was_new:
        for row in conn.execute(
            "SELECT id, address FROM businesses WHERE address IS NOT NULL AND TRIM(address) != ''"
        ):
            cy = resolve_city(None, row["address"], None)
            if cy:
                conn.execute("UPDATE businesses SET city = ? WHERE id = ?", (cy, row["id"]))
    conn.commit()
    conn.close()


# ── Scraping helpers (self-contained so app.py has no import coupling) ────────

def _api_get(
    url: str,
    params: dict,
    api_key: str,
    *,
    log_context: str = "",
) -> dict | list | None:
    headers = {"x-rapidapi-host": API_HOST, "x-rapidapi-key": api_key}
    delay = 1.0
    ctx = f" {log_context}" if log_context else ""
    for _ in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    log.warning("Maps API non-JSON 200%s: %s", ctx, resp.text[:300])
                    return None
            if resp.status_code == 429:
                time.sleep(delay)
                delay *= 2
                continue
            log.warning(
                "Maps API HTTP %s%s — %s",
                resp.status_code,
                ctx,
                (resp.text or "")[:500],
            )
            return None
        except requests.RequestException as exc:
            log.warning("Maps API request error%s: %s", ctx, exc)
            time.sleep(delay)
            delay *= 2
    log.error("Maps API gave up after retries%s", ctx)
    return None


def _query_for_zip(keyword: str, zip_code: str) -> str:
    q = (keyword or "").strip()
    z = (zip_code or "").strip()
    if not z or not APPEND_ZIP_TO_QUERY:
        return q
    if z in q:
        return q
    return f"{q} {z}".strip()


def _parse(item: dict, zip_code: str) -> dict:
    phone, website = contact_fields_from_maps_item(item)
    types = item.get("types")
    category = types[0] if isinstance(types, list) and types else item.get("type") or item.get("category")
    address = formatted_address_from_item(item)
    if not address:
        raw_addr = item.get("full_address") or item.get("address")
        if isinstance(raw_addr, dict):
            address = raw_addr.get("formatted_address") or raw_addr.get("formatted")
        else:
            address = raw_addr if isinstance(raw_addr, str) else None
    city = resolve_city(item, address, zip_code)
    listing_zip = best_listing_zip(item, address)
    return {
        "business_name": item.get("name") or item.get("title"),
        "address":       address,
        "city":          city,
        "phone":         phone or item.get("phone_number") or item.get("phone"),
        "website_url":   website or item.get("website"),
        "rating":        item.get("rating"),
        "review_count":  item.get("reviews") or item.get("review_count"),
        "category":      category,
        "zip_code":      listing_zip,
        "search_zip":    normalize_zip5(zip_code),
    }


def _extract_place_token(item: dict) -> str | None:
    for k in ("place_id", "google_id", "placeId", "googleId", "cid", "data_id", "business_id"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _first_dict_payload(data: dict | None) -> dict | None:
    if not isinstance(data, dict):
        return None
    if isinstance(data.get("data"), dict):
        return data["data"]
    if isinstance(data.get("result"), dict):
        return data["result"]
    if isinstance(data.get("place"), dict):
        return data["place"]
    if isinstance(data.get("data"), list) and data["data"] and isinstance(data["data"][0], dict):
        return data["data"][0]
    if isinstance(data.get("results"), list) and data["results"] and isinstance(data["results"][0], dict):
        return data["results"][0]
    return data


def _fetch_place_details(place_token: str, api_key: str) -> dict | None:
    """Look up a place, probing endpoints only until one combination works.

    The first successful (endpoint, param-key) pair is pinned for the rest of the
    process, so later lookups cost one API call instead of up to twelve. The cache
    is module-scoped: overlapping ZIPs share it instead of re-probing per ZIP.
    """
    global _DETAIL_COMBO
    with _details_lock:
        if place_token in _details_cache:
            return _details_cache[place_token]
        combo = _DETAIL_COMBO

    combos = [combo] if combo else [
        (ep, key) for ep in DETAIL_ENDPOINTS for key in DETAIL_PARAM_KEYS
    ]
    payload = None
    for ep, key in combos:
        data = _api_get(
            f"{API_BASE}{ep}", {key: place_token, "language": "en"}, api_key,
            log_context="place-detail",
        )
        found = _first_dict_payload(data)
        if isinstance(found, dict) and found:
            payload = found
            if combo is None:
                with _details_lock:
                    if _DETAIL_COMBO is None:
                        _DETAIL_COMBO = (ep, key)
                        log.info("Pinned place-details endpoint %s with param %r", ep, key)
            break

    with _details_lock:
        if len(_details_cache) >= MAX_DETAILS_CACHE:
            for stale in list(_details_cache)[: MAX_DETAILS_CACHE // 2]:
                del _details_cache[stale]
        _details_cache[place_token] = payload
    return payload


def _hydrate_row_location(row: dict, item: dict, zip_code: str, api_key: str) -> dict:
    if row.get("address") and row.get("city"):
        return item
    token = _extract_place_token(item)
    if not token:
        return item
    details = _fetch_place_details(token, api_key)
    if not details:
        return item
    if not row.get("address"):
        row["address"] = formatted_address_from_item(details)
    if not row.get("city"):
        row["city"] = resolve_city(details, row.get("address"), zip_code) or resolve_city(item, row.get("address"), zip_code)
    return details


def _insert(
    conn: sqlite3.Connection,
    lock: threading.Lock,
    row: dict,
    pipeline_stage: str = "scraped",
    stage_reason: str | None = None,
    search_keyword: str | None = None,
    vertical: str | None = None,
) -> bool:
    phone = row.get("phone") or None
    url   = row.get("website_url") or None
    if phone is None and url is None:
        return False
    with lock:
        try:
            conn.execute(
                "INSERT INTO businesses "
                "(business_name, address, city, phone, website_url, rating, review_count, category, zip_code, search_zip, "
                "search_keyword, vertical, pipeline_stage, stage_reason, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row["business_name"], row["address"], row.get("city"), phone, url,
                 row["rating"], row["review_count"], row["category"], row["zip_code"], row.get("search_zip"),
                 search_keyword, vertical, pipeline_stage, stage_reason, _now_eastern()),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def _scrape_zip(
    zip_code: str,
    keyword: str,
    api_key: str,
    conn: sqlite3.Connection,
    lock: threading.Lock,
    job_id: str | None = None,
    vertical: str | None = None,
) -> tuple[int, int, int, int]:
    inserted = skipped = geo_rejected = no_website = 0
    vertical = vertical or vertical_for_keyword(keyword)
    query = _query_for_zip(keyword, zip_code)
    coords = us_zip_latlng(zip_code)
    if not coords:
        log.warning(
            "Could not geocode US zip %s — Maps search will omit lat/lng (often returns empty).",
            zip_code,
        )
        if job_id:
            _job_event(
                job_id,
                "warning",
                "scrape",
                f"ZIP {zip_code}: geocoding failed; try again or check zip. Search may return no rows without coordinates.",
            )
    for page in range(1, MAX_PAGES + 1):
        params: dict[str, object] = {
            "query": query,
            "zipcode": zip_code,
            "country": "us",
            "limit": PAGE_SIZE,
            "offset": (page - 1) * PAGE_SIZE,
            "language": "en",
        }
        if coords:
            params["lat"] = coords[0]
            params["lng"] = coords[1]
            if coords.exact:
                params["zoom"] = 13
        data = _api_get(
            f"{API_BASE}/searchmaps.php",
            params,
            api_key,
            log_context=f"searchmaps zip={zip_code} page={page} query={query!r} latlng={coords!r}",
        )
        if not data:
            msg = f"searchmaps returned no JSON for zip {zip_code} page {page} (check API key and RapidAPI subscription)."
            log.warning(msg)
            if job_id:
                _job_event(job_id, "warning", "scrape", msg)
            break
        results = iter_search_results(data)
        if not results:
            keys = list(data.keys())[:24] if isinstance(data, dict) else [type(data).__name__]
            snippet = str(data)[:400] if isinstance(data, dict) else repr(data)[:200]
            msg = (
                f"No business list in API response for zip {zip_code} page {page}. "
                f"top-level keys={keys!r} snippet={snippet!r}"
            )
            log.warning(msg)
            if job_id:
                _job_event(job_id, "warning", "scrape", msg)
            break
        for item in results:
            row = _parse(item, zip_code)
            item_for_match = _hydrate_row_location(row, item, zip_code, api_key)
            row["zip_code"] = best_listing_zip(item_for_match, row.get("address"))
            expected_zip = normalize_zip5(zip_code)
            strict_exact = os.environ.get("STRICT_EXACT_ZIP", "1").strip().lower() not in ("0", "false", "no", "off")
            exact_ok = bool(row.get("zip_code") and expected_zip and row["zip_code"] == expected_zip)
            geo_ok = listing_matches_search_zip(item_for_match, row.get("address"), zip_code)
            accept = exact_ok if strict_exact else geo_ok
            if not accept:
                if _insert(conn, lock, row, "geo_rejected", "geo_zip_mismatch", keyword, vertical):
                    geo_rejected += 1
                else:
                    skipped += 1
                continue
            if not row.get("website_url"):
                if _insert(conn, lock, row, "no_website_prospect", "no_website", keyword, vertical):
                    no_website += 1
                else:
                    skipped += 1
            elif _insert(conn, lock, row, "scraped", None, keyword, vertical):
                inserted += 1
            else:
                skipped += 1
        log.info(
            "[scrape-job %s] zip=%s page=%d api_rows=%d cumulative new=%d dup=%d off_target=%d no_website=%d query=%r",
            job_id or "-",
            zip_code,
            page,
            len(results),
            inserted,
            skipped,
            geo_rejected,
            no_website,
            query,
        )
        if len(results) < PAGE_SIZE:
            break
    log.info(
        "[scrape-job %s] zip=%s finished cumulative new=%d dup=%d off_target=%d no_website=%d",
        job_id or "-",
        zip_code,
        inserted,
        skipped,
        geo_rejected,
        no_website,
    )
    return inserted, skipped, geo_rejected, no_website


_ENRICH_PROGRESS_KEYS = ("checked", "enriched", "no_email")
_CLEAN_PROGRESS_KEYS = ("checked", "clean", "flagged", "failed")


def _cancel_requested(job_id: str) -> bool:
    with _jobs_lock:
        job = _jobs.get(job_id)
        return bool(job and job.get("cancel_requested"))


def _stage_progress_reporter(
    job_id: str, stage: str, counter_key: str, keys: tuple[str, ...], *, drive_bar: bool
):
    """Build the progress callback _enrich_stage_rows / _clean_stage_rows call.

    Mirrors each batch's counters onto the job record so /api/jobs shows live
    numbers, and logs one event per batch. `drive_bar` moves the job's
    processed/total (used by bulk jobs, where rows are the unit of work); a
    scrape job keeps its zip-based bar and only gains the counters.
    """

    def report(done: int, total: int, stats: dict) -> None:
        snapshot = dict(stats)
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job is None:
                return
            job[counter_key] = snapshot
            if drive_bar:
                job["processed"] = done
                job["total"] = total
        summary = ", ".join(f"{k}={snapshot[k]}" for k in keys if k in snapshot)
        _job_event(job_id, "info", stage, f"Progress {done}/{total} rows — {summary}.")
        log.info("[%s-job %s] progress %d/%d %s", stage, job_id, done, total, summary)

    return report


def _run_job(job_id: str, api_key: str) -> None:
    with _jobs_lock:
        job = _jobs[job_id]
        job["status"] = "running"
        job_type = job.get("type", JOB_TYPE_SCRAPE)
    if job_type in _BULK_JOB_TYPES:
        _run_bulk_stage_job(job_id, job_type)
        return
    _job_event(job_id, "info", "scrape", "Job started.")
    with _jobs_lock:
        j = _jobs[job_id]
    log.info(
        "[scrape-job %s] started keyword=%r vertical=%r zip_count=%d mode=%s",
        job_id,
        j.get("keyword"),
        j.get("vertical"),
        len(j.get("zip_codes") or []),
        j.get("run_mode", "scrape_only"),
    )

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    lock = threading.Lock()

    try:
        with ThreadPoolExecutor(max_workers=JOB_WORKERS) as pool:
            futures = {
                pool.submit(
                    _scrape_zip, z, job["keyword"], api_key, conn, lock, job_id,
                    job.get("vertical"),
                ): z
                for z in job["zip_codes"]
            }
            for future in as_completed(futures):
                ins, skp, geo, nw = future.result()
                with _jobs_lock:
                    job["inserted"]           += ins
                    job["duplicates"]         += skp
                    job["geo_rejected"]        = job.get("geo_rejected", 0) + geo
                    job["no_website_prospect"] = job.get("no_website_prospect", 0) + nw
                    job["processed"]          += 1
                _job_event(
                    job_id, "info", "scrape",
                    f"Processed zip {job['processed']}/{job['total']} (+{ins} new, {skp} dup, {geo} off-target, {nw} no-website).",
                )
        if job.get("run_mode") == "full_pipeline":
            enrich_progress = _stage_progress_reporter(
                job_id, "enrich", "enriched", _ENRICH_PROGRESS_KEYS, drive_bar=False
            )
            _job_event(job_id, "info", "enrich", "Starting enrichment (in-target scraped rows).")
            e_scraped = _enrich_stage_rows(conn, "scraped", limit=None, progress=enrich_progress)
            _job_event(job_id, "info", "enrich", "Starting enrichment (off-target / geo_rejected rows).")
            e_geo = _enrich_stage_rows(conn, "geo_rejected", limit=None, progress=enrich_progress)
            enriched = {
                "checked": e_scraped["checked"] + e_geo["checked"],
                "enriched": e_scraped["enriched"] + e_geo["enriched"],
                "no_email": e_scraped["no_email"] + e_geo["no_email"],
            }
            _job_event(
                job_id, "info", "enrich",
                f"Enrichment complete: checked={enriched['checked']}, enriched={enriched['enriched']}, no_email={enriched['no_email']} "
                f"(scraped {e_scraped['checked']}, geo_rejected {e_geo['checked']}).",
            )
            _job_event(job_id, "info", "clean", "Starting cleaning stage.")
            cleaned = _clean_stage_rows(
                conn, "enriched", limit=None,
                progress=_stage_progress_reporter(
                    job_id, "clean", "cleaned", _CLEAN_PROGRESS_KEYS, drive_bar=False
                ),
            )
            _job_event(
                job_id, "info", "clean",
                f"Cleaning complete: checked={cleaned['checked']}, clean={cleaned['clean']}, flagged={cleaned['flagged']}, failed={cleaned['failed']}.",
            )
            with _jobs_lock:
                job["enriched"] = enriched
                job["cleaned"] = cleaned
        with _jobs_lock:
            job["status"] = "completed"
            fin = dict(job)
        log.info(
            "[scrape-job %s] completed new=%d dup=%d off_target=%d no_website=%d mode=%s",
            job_id,
            fin.get("inserted", 0),
            fin.get("duplicates", 0),
            fin.get("geo_rejected", 0),
            fin.get("no_website_prospect", 0),
            fin.get("run_mode", "scrape_only"),
        )
        _job_event(job_id, "info", "job", "Job completed.")
    except Exception as exc:
        log.exception("Job %s failed", job_id)
        with _jobs_lock:
            job["status"] = "failed"
            job["error"]  = str(exc)
        _job_event(job_id, "error", "job", f"Job failed: {exc}")
    finally:
        with _jobs_lock:
            job["completed_at"] = datetime.utcnow().isoformat()
        conn.close()


def _run_bulk_stage_job(job_id: str, job_type: str) -> None:
    """Run a bulk enrich/clean over a pipeline stage on the shared job worker.

    Same contract as a scrape job: live counters on /api/jobs, one event per
    batch, and a cancel request honoured between batches.
    """
    action = "enrich" if job_type == JOB_TYPE_BULK_ENRICH else "clean"
    counter_key = "enriched" if action == "enrich" else "cleaned"
    keys = _ENRICH_PROGRESS_KEYS if action == "enrich" else _CLEAN_PROGRESS_KEYS

    with _jobs_lock:
        job = _jobs[job_id]
        from_stage = job.get("from_stage") or ""
        limit = job.get("limit")

    scope = f"limit {limit}" if limit else "no limit"
    _job_event(job_id, "info", action, f"Bulk {action} started on stage '{from_stage}' ({scope}).")
    log.info("[bulk-%s job %s] started stage=%s limit=%s", action, job_id, from_stage, limit)

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        runner = _enrich_stage_rows if action == "enrich" else _clean_stage_rows
        result = runner(
            conn,
            from_stage,
            limit,
            progress=_stage_progress_reporter(
                job_id, action, counter_key, keys, drive_bar=True
            ),
            should_cancel=lambda: _cancel_requested(job_id),
        )
        summary = ", ".join(f"{k}={result[k]}" for k in keys if k in result)
        cancelled = bool(result.get("cancelled"))
        with _jobs_lock:
            job = _jobs[job_id]
            job[counter_key] = dict(result)
            job["processed"] = result["checked"]
            job["status"] = "cancelled" if cancelled else "completed"
        if result.get("errors"):
            _job_event(
                job_id, "warning", action,
                f"{result['errors']} row(s) errored and kept their stage — re-run to retry them.",
            )
        if cancelled:
            _job_event(job_id, "info", "job", f"Bulk {action} cancelled after {result['checked']} rows — {summary}.")
        else:
            _job_event(job_id, "info", "job", f"Bulk {action} complete: {summary}.")
        log.info(
            "[bulk-%s job %s] %s stage=%s %s",
            action, job_id, "cancelled" if cancelled else "completed", from_stage, summary,
        )
    except Exception as exc:
        log.exception("Bulk %s job %s failed", action, job_id)
        with _jobs_lock:
            job = _jobs[job_id]
            job["status"] = "failed"
            job["error"] = str(exc)
        _job_event(job_id, "error", "job", f"Bulk {action} failed: {exc}")
    finally:
        with _jobs_lock:
            _jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
        conn.close()


def _signal_columns(signals: dict | None) -> tuple:
    """Flatten detect_signals() output into the UPDATE parameter order."""
    s = signals or {}
    return (
        1 if s.get("runs_google_ads") else 0,
        s.get("aw_ids") or None,
        s.get("call_tracking"),
        1 if s.get("has_gtm") else 0,
        1 if s.get("has_ga4") else 0,
    )


# Rows worth crawling: still in the source stage, has a site, has no email yet.
_ENRICH_CANDIDATE_WHERE = (
    "pipeline_stage = ? AND website_url IS NOT NULL AND (email IS NULL OR email = '')"
)


def _count_stage_rows(
    conn: sqlite3.Connection, action: str, from_stage: str, limit: int | None
) -> int:
    """How many rows a bulk action would touch — used to size the progress bar."""
    where = _ENRICH_CANDIDATE_WHERE if action == "enrich" else "pipeline_stage = ?"
    total = conn.execute(
        f"SELECT COUNT(*) FROM businesses WHERE {where}", (from_stage,)
    ).fetchone()[0]
    return min(total, limit) if limit else total


def _enrich_stage_rows(
    conn: sqlite3.Connection,
    from_stage: str,
    limit: int | None,
    *,
    progress=None,
    should_cancel=None,
) -> dict:
    """Crawl every candidate row in `from_stage` for an email and ad-tech signals.

    Work is done in batches of PROGRESS_CHUNK_ROWS so a long run commits
    incrementally, reports progress, and can stop between batches. `progress` is
    called as progress(done, total, stats) after each batch.
    """
    params = [from_stage]
    query = f"SELECT id, website_url FROM businesses WHERE {_ENRICH_CANDIDATE_WHERE} ORDER BY id ASC"
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(query, params).fetchall()
    total = len(rows)
    stats = {"checked": 0, "enriched": 0, "no_email": 0, "errors": 0, "cancelled": False}
    if not total:
        if progress:
            progress(0, 0, stats)
        return stats

    def task(row):
        row_id = row["id"] if isinstance(row, sqlite3.Row) else row[0]
        website_url = row["website_url"] if isinstance(row, sqlite3.Row) else row[1]
        session = requests.Session()
        session.headers.update({"User-Agent": ENRICH_USER_AGENT})
        try:
            res = scrape_email_for_website(
                website_url,
                session,
                allow_generic_fallback=EMAIL_ALLOW_GENERIC_FALLBACK,
            )
            return row_id, res
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        for start in range(0, total, PROGRESS_CHUNK_ROWS):
            if should_cancel and should_cancel():
                stats["cancelled"] = True
                break
            batch = rows[start : start + PROGRESS_CHUNK_ROWS]
            futures = [pool.submit(task, row) for row in batch]
            for fut in as_completed(futures):
                # One unreachable site must never end a 45k-row run, so every
                # row is accounted for individually. Rows that raise keep their
                # current stage and are picked up by the next run.
                try:
                    row_id, res = fut.result()
                    now = datetime.utcnow().isoformat()
                    sig = _signal_columns(res.signals)
                    if res.email:
                        conn.execute(
                            "UPDATE businesses SET email=?, all_emails=?, pipeline_stage='enriched', stage_reason=NULL, enriched_at=?, "
                            "runs_google_ads=?, aw_ids=?, call_tracking=?, has_gtm=?, has_ga4=? WHERE id=?",
                            (res.email, res.all_emails, now, *sig, row_id),
                        )
                        stats["enriched"] += 1
                    else:
                        reason = res.stage_reason or "no_email_found"
                        # Signals are written here too: a business running ads with no
                        # discoverable email is still worth knowing about.
                        conn.execute(
                            "UPDATE businesses SET all_emails=?, pipeline_stage='enrich_failed', stage_reason=?, enriched_at=?, "
                            "runs_google_ads=?, aw_ids=?, call_tracking=?, has_gtm=?, has_ga4=? WHERE id=?",
                            (res.all_emails, reason, now, *sig, row_id),
                        )
                        stats["no_email"] += 1
                    if res.city:
                        conn.execute(
                            "UPDATE businesses SET city=? WHERE id=? AND (city IS NULL OR TRIM(city)='')",
                            (res.city, row_id),
                        )
                except Exception as exc:
                    stats["errors"] += 1
                    log.warning("Enrichment failed for one row: %s", exc)
                stats["checked"] += 1
            conn.commit()
            if progress:
                progress(stats["checked"], total, stats)
    return stats


@lru_cache(maxsize=20000)
def has_mx(domain: str, timeout: float = 3.0) -> bool:
    """True if the domain can plausibly receive mail.

    Only definitive negatives (no MX *and* no A/AAAA record, or NXDOMAIN) return
    False — timeouts and resolver errors return True so a flaky lookup never
    discards a good lead. MillionVerifier remains the final gate; this only stops
    us paying to verify dead domains.
    """
    domain = (domain or "").strip().lower()
    if not domain or "." not in domain:
        return False
    try:
        import dns.resolver  # optional: dnspython
    except ImportError:
        return True
    resolver = dns.resolver.Resolver()
    resolver.timeout = timeout
    resolver.lifetime = timeout
    try:
        answers = resolver.resolve(domain, "MX")
        if len(answers) > 0:
            return True
    except (dns.resolver.NoAnswer, dns.resolver.NoNameservers):
        pass                        # no MX — fall through to the implicit-MX check
    except dns.resolver.NXDOMAIN:
        return False
    except Exception:
        return True                 # timeout / transient resolver failure
    # RFC 5321 implicit MX: a domain with an address record still accepts mail.
    for rtype in ("A", "AAAA"):
        try:
            if len(resolver.resolve(domain, rtype)) > 0:
                return True
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers):
            continue
        except Exception:
            return True
    return False


def _warm_mx_cache(rows) -> None:
    """Resolve every distinct email domain up front, 20 at a time."""
    domains = {
        (row["email"] or "").strip().lower().split("@")[-1]
        for row in rows
        if (row["email"] or "").strip() and "@" in (row["email"] or "")
    }
    domains.discard("")
    if not domains:
        return
    with ThreadPoolExecutor(max_workers=MX_WORKERS) as pool:
        list(pool.map(has_mx, sorted(domains)))


def _classify_clean_stage(row: sqlite3.Row) -> tuple[str, str | None]:
    email = (row["email"] or "").strip().lower()
    name = (row["business_name"] or "").lower()
    if not email:
        return "clean_failed", "missing_email"
    if "@" not in email or not has_mx(email.split("@")[-1]):
        return "clean_failed", "no_mx"
    if FLAG_FREE_EMAIL_DOMAINS and email.split("@")[-1] in PERSONAL_DOMAINS:
        return "flagged", "personal_email_domain"
    if "permanently closed" in name:
        return "flagged", "permanently_closed"
    return "clean", None


def _clean_stage_rows(
    conn: sqlite3.Connection,
    from_stage: str,
    limit: int | None,
    *,
    progress=None,
    should_cancel=None,
) -> dict:
    """Classify every row in `from_stage` as clean / flagged / clean_failed.

    Batched like _enrich_stage_rows: MX lookups are warmed per batch (has_mx is
    cached, so the total work is unchanged) rather than all up front, which keeps
    progress moving instead of stalling on tens of thousands of DNS lookups.
    """
    query = "SELECT * FROM businesses WHERE pipeline_stage = ? ORDER BY id ASC"
    params = [from_stage]
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(query, params).fetchall()
    total = len(rows)
    stats = {"checked": 0, "clean": 0, "flagged": 0, "failed": 0, "cancelled": False}
    if not total:
        if progress:
            progress(0, 0, stats)
        return stats

    now = datetime.utcnow().isoformat()
    for start in range(0, total, PROGRESS_CHUNK_ROWS):
        if should_cancel and should_cancel():
            stats["cancelled"] = True
            break
        batch = rows[start : start + PROGRESS_CHUNK_ROWS]
        _warm_mx_cache(batch)
        for row in batch:
            stage, reason = _classify_clean_stage(row)
            conn.execute(
                "UPDATE businesses SET pipeline_stage=?, stage_reason=?, cleaned_at=? WHERE id=?",
                (stage, reason, now, row["id"]),
            )
            if stage == "clean":
                stats["clean"] += 1
            elif stage == "flagged":
                stats["flagged"] += 1
            else:
                stats["failed"] += 1
            stats["checked"] += 1
        conn.commit()
        if progress:
            progress(stats["checked"], total, stats)
    return stats


def _job_event(job_id: str, level: str, stage: str, message: str) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        events = job.setdefault("events", [])
        events.append({
            "ts": datetime.utcnow().isoformat(),
            "level": level,
            "stage": stage,
            "message": message,
        })
        if len(events) > MAX_JOB_EVENTS:
            del events[: len(events) - MAX_JOB_EVENTS]


def _prune_old_jobs() -> None:
    """Remove oldest terminal jobs from _jobs, keeping at most MAX_STORED_JOBS."""
    TERMINAL = {"completed", "failed", "cancelled"}
    with _jobs_lock:
        terminal = [j for j in _jobs.values() if j["status"] in TERMINAL]
        if len(terminal) <= MAX_STORED_JOBS:
            return
        terminal.sort(key=lambda j: j.get("started_at") or "")
        for j in terminal[: len(terminal) - MAX_STORED_JOBS]:
            del _jobs[j["id"]]


def _queue_worker() -> None:
    """Single consumer thread: runs one job at a time from _job_queue."""
    while True:
        job_id, api_key = _job_queue.get()
        try:
            with _jobs_lock:
                job = _jobs.get(job_id)
            if job is None:
                continue
            if job.get("status") == "cancelled":
                _job_event(job_id, "info", "job", "Job was cancelled before it started.")
                continue
            _run_job(job_id, api_key)
        except Exception:
            log.exception("Unexpected error in _queue_worker for job %s", job_id)
        finally:
            _job_queue.task_done()
            _prune_old_jobs()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", states=STATE_NAMES)


@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    with _jobs_lock:
        jobs = [
            {k: v for k, v in j.items() if k != "zip_codes"}
            for j in _jobs.values()
        ]
    return jsonify(sorted(jobs, key=lambda x: x["started_at"], reverse=True))


@app.route("/api/jobs/<job_id>/cancel", methods=["POST"])
def cancel_job(job_id: str):
    """Cancel a queued job, or ask a running bulk stage job to stop.

    A running bulk job stops between batches, so rows already processed keep
    their new stage — cancelling costs at most PROGRESS_CHUNK_ROWS of in-flight
    work. Running scrape jobs remain uncancellable.
    """
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        status = job["status"]
        is_bulk = job.get("type") in _BULK_JOB_TYPES
        if status == "queued":
            job["status"] = "cancelled"
            job["completed_at"] = datetime.utcnow().isoformat()
            pending = False
        elif status == "running" and is_bulk:
            job["cancel_requested"] = True
            pending = True
        else:
            return jsonify({"error": f"Cannot cancel a job with status '{status}'"}), 409
    if pending:
        _job_event(job_id, "info", "job", "Cancel requested — stopping after the current batch.")
        return jsonify({"ok": True, "pending": True})
    _job_event(job_id, "info", "job", "Job cancelled by user.")
    return jsonify({"ok": True, "pending": False})


@app.route("/api/jobs/<job_id>/events", methods=["GET"])
def job_events(job_id: str):
    limit = request.args.get("limit", 50, type=int)
    if limit <= 0:
        limit = 50
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        events = job.get("events", [])
        return jsonify({
            "job_id": job_id,
            "count": len(events),
            "events": events[-limit:],
        })


@app.route("/api/jobs", methods=["POST"])
def start_job():
    data        = request.get_json(force=True)
    keyword     = (data.get("keyword")     or "").strip()
    api_key     = (data.get("api_key")     or "").strip()
    state       = (data.get("state")       or "").strip().upper()
    custom_zips = (data.get("custom_zips") or "").strip()
    run_mode    = (data.get("run_mode") or "scrape_only").strip()
    vertical    = (data.get("vertical")    or "").strip()

    if not keyword:
        return jsonify({"error": "Keyword is required"}), 400
    if not api_key:
        return jsonify({"error": "RapidAPI key is required"}), 400

    if custom_zips:
        zips = [z.strip().zfill(5) for z in custom_zips.replace(",", "\n").splitlines() if z.strip()]
    elif state and state in STATE_ZIPS:
        zips = STATE_ZIPS[state]
    else:
        return jsonify({"error": "Select a state or enter custom zip codes"}), 400

    if not zips:
        return jsonify({"error": "No zip codes found"}), 400

    zips = list(dict.fromkeys(zips))  # deduplicate
    if run_mode not in {"scrape_only", "full_pipeline"}:
        return jsonify({"error": "Invalid run mode"}), 400
    vertical = vertical or vertical_for_keyword(keyword)
    job = {
        "id":                  str(uuid.uuid4())[:8],
        "type":                JOB_TYPE_SCRAPE,
        "unit":                "zips",
        "keyword":             keyword,
        "vertical":            vertical,
        "state":               state,
        "zip_codes":           zips,
        "status":              "queued",
        "cancel_requested":    False,
        "total":               len(zips),
        "processed":           0,
        "inserted":            0,
        "duplicates":          0,
        "geo_rejected":        0,
        "no_website_prospect": 0,
        "queued_at":           datetime.utcnow().isoformat(),
        "started_at":          datetime.utcnow().isoformat(),
        "completed_at":        None,
        "error":               None,
        "run_mode":            run_mode,
        "enriched":            None,
        "cleaned":             None,
        "events":              [],
    }

    with _jobs_lock:
        _jobs[job["id"]] = job
    _job_event(
        job["id"], "info", "job",
        f"Job queued ({run_mode}). Keyword='{keyword}'. Vertical='{vertical}'. Target zips={len(zips)}.",
    )

    _job_queue.put((job["id"], api_key))
    return jsonify({"id": job["id"]}), 202


@app.route("/api/stats")
def stats():
    try:
        conn  = get_conn()
        total = conn.execute(
            "SELECT COUNT(*) FROM businesses WHERE pipeline_stage != 'archived'"
        ).fetchone()[0]
        with_email = conn.execute(
            "SELECT COUNT(*) FROM businesses WHERE email IS NOT NULL AND email != '' AND pipeline_stage != 'archived'"
        ).fetchone()[0]
        categories = conn.execute(
            "SELECT category, COUNT(*) cnt FROM businesses "
            "WHERE category IS NOT NULL AND pipeline_stage != 'archived' "
            "GROUP BY category ORDER BY cnt DESC LIMIT 12"
        ).fetchall()
        stage_counts = conn.execute(
            "SELECT pipeline_stage, COUNT(*) cnt FROM businesses GROUP BY pipeline_stage ORDER BY cnt DESC"
        ).fetchall()
        conn.close()
        return jsonify({
            "total":      total,
            "with_email": with_email,
            "categories": [{"name": r["category"], "count": r["cnt"]} for r in categories],
            "stages":     [{"name": r["pipeline_stage"] or "unknown", "count": r["cnt"]} for r in stage_counts],
        })
    except Exception:
        return jsonify({"total": 0, "with_email": 0, "categories": [], "stages": []})


@app.route("/api/categories")
def categories():
    try:
        conn = get_conn()
        cats = conn.execute(
            "SELECT DISTINCT category FROM businesses WHERE category IS NOT NULL ORDER BY category"
        ).fetchall()
        conn.close()
        return jsonify([r["category"] for r in cats])
    except Exception:
        return jsonify([])


@app.route("/api/leads/archive", methods=["POST"])
def archive_leads():
    data        = request.get_json(force=True) or {}
    min_rating  = float(data.get("min_rating",  0) or 0)
    min_reviews = int(  data.get("min_reviews", 0) or 0)
    has_email   = str(  data.get("has_email", "false")).lower() == "true"
    category    = (data.get("category") or "").strip()
    stage       = (data.get("stage")    or "").strip()

    where, params = _build_where(min_rating, min_reviews, has_email, category, stage)

    # Never silently re-archive already-archived rows unless the user
    # explicitly filtered to the archived stage.
    if not stage:
        extra = "pipeline_stage != 'archived'"
        where = f"WHERE {extra}" if not where else f"{where} AND {extra}"

    try:
        conn   = get_conn()
        result = conn.execute(
            f"UPDATE businesses SET pipeline_stage = 'archived' {where}",
            params,
        )
        count = result.rowcount
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "archived": count})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/leads")
def leads():
    page        = request.args.get("page",        1,     type=int)
    per_page    = request.args.get("per_page",    25,    type=int)
    min_rating  = request.args.get("min_rating",  0.0,   type=float)
    min_reviews = request.args.get("min_reviews", 0,     type=int)
    has_email   = request.args.get("has_email",   "false").lower() == "true"
    category    = request.args.get("category",    "")
    stage       = request.args.get("stage",       "")

    where, params = _build_where(min_rating, min_reviews, has_email, category, stage)
    # Hide archived records from the default view; show them only when
    # the user explicitly filters to the "archived" stage.
    if not stage:
        extra = "pipeline_stage != 'archived'"
        where = f"WHERE {extra}" if not where else f"{where} AND {extra}"
    offset = (page - 1) * per_page

    try:
        conn  = get_conn()
        total = conn.execute(f"SELECT COUNT(*) FROM businesses {where}", params).fetchone()[0]
        rows  = conn.execute(
            f"SELECT * FROM businesses {where} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [per_page, offset],
        ).fetchall()
        conn.close()
        return jsonify({
            "total":    total,
            "page":     page,
            "per_page": per_page,
            "pages":    max(1, (total + per_page - 1) // per_page),
            "leads":    [dict(r) for r in rows],
        })
    except Exception as exc:
        return jsonify({"error": str(exc), "total": 0, "page": 1, "pages": 1, "leads": []}), 500


@app.route("/api/export")
def export():
    min_rating  = request.args.get("min_rating",  0.0,   type=float)
    min_reviews = request.args.get("min_reviews", 0,     type=int)
    has_email   = request.args.get("has_email",   "false").lower() == "true"
    category    = request.args.get("category",    "")
    stage       = request.args.get("stage",       "")
    vertical    = request.args.get("vertical",    "")

    where, params = _build_where(min_rating, min_reviews, has_email, category, stage, vertical)
    if not stage:
        extra = "pipeline_stage != 'archived'"
        where = f"WHERE {extra}" if not where else f"{where} AND {extra}"

    # Keep this list and the CSV header below in lock-step.
    columns = [
        "business_name", "address", "city", "phone", "website_url", "email", "all_emails",
        "rating", "review_count", "category", "zip_code", "search_zip",
        "search_keyword", "vertical",
        "runs_google_ads", "aw_ids", "call_tracking", "has_gtm", "has_ga4",
        "pipeline_stage", "stage_reason", "created_at",
    ]
    select_cols = ", ".join(columns)
    if has_email:
        # One row per distinct address — the same business is reached through
        # overlapping ZIPs and through http/https/www URL variants.
        sql = (
            f"SELECT {select_cols} FROM businesses WHERE id IN "
            f"(SELECT MIN(id) FROM businesses {where} GROUP BY LOWER(TRIM(email))) "
            f"ORDER BY id DESC"
        )
    else:
        sql = f"SELECT {select_cols} FROM businesses {where} ORDER BY id DESC"

    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    header = columns[:-1] + ["scraped_at_et"]

    def generate():
        buf = io.StringIO()
        w   = csv.writer(buf)
        w.writerow(header)
        for row in rows:
            w.writerow(list(row))
        yield buf.getvalue()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{timestamp}.csv"},
    )


def _build_where(min_rating: float, min_reviews: int, has_email: bool, category: str, stage: str,
                 vertical: str = ""):
    clauses, params = [], []
    if min_rating > 0:
        clauses.append("rating >= ?");     params.append(min_rating)
    if min_reviews > 0:
        clauses.append("review_count >= ?"); params.append(min_reviews)
    if has_email:
        clauses.append("email IS NOT NULL AND email != ''")
    if category:
        clauses.append("category = ?");    params.append(category)
    if stage:
        clauses.append("pipeline_stage = ?"); params.append(stage)
    if vertical:
        clauses.append("LOWER(vertical) = ?"); params.append(vertical.strip().lower())
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


@app.route("/api/pipeline/advance", methods=["POST"])
def advance_pipeline():
    """Advance a whole pipeline stage — inline for small capped batches, else a job.

    Enriching tens of thousands of rows takes hours, far longer than any HTTP
    timeout, so an uncapped run is queued on the job worker and this returns a
    job_id straight away. A limit of BULK_INLINE_MAX_ROWS or fewer still runs
    inline and returns its counts, so test batches behave as before.
    """
    data = request.get_json(force=True)
    from_stage = (data.get("from_stage") or "").strip()
    action = (data.get("action") or "").strip()  # enrich or clean
    limit = data.get("limit")
    if action not in {"enrich", "clean"}:
        return jsonify({"error": "Action must be 'enrich' or 'clean'"}), 400
    if not from_stage:
        return jsonify({"error": "from_stage is required"}), 400
    try:
        if limit is not None:
            limit = int(limit)
            if limit <= 0:
                limit = None
    except (TypeError, ValueError):
        return jsonify({"error": "limit must be a number"}), 400

    if limit is not None and limit <= BULK_INLINE_MAX_ROWS:
        conn = get_conn()
        try:
            runner = _enrich_stage_rows if action == "enrich" else _clean_stage_rows
            result = runner(conn, from_stage, limit)
            return jsonify({
                "ok": True, "mode": "inline", "action": action,
                "from_stage": from_stage, "result": result,
            })
        finally:
            conn.close()

    conn = get_conn()
    try:
        total = _count_stage_rows(conn, action, from_stage, limit)
    finally:
        conn.close()

    job_type = JOB_TYPE_BULK_ENRICH if action == "enrich" else JOB_TYPE_BULK_CLEAN
    now = datetime.utcnow().isoformat()
    job = {
        "id":               str(uuid.uuid4())[:8],
        "type":             job_type,
        "unit":             "rows",
        # The UI titles a job card with `keyword`; name it after the work.
        "keyword":          f"{action} · {from_stage}",
        "vertical":         "",
        "state":            "",
        "from_stage":       from_stage,
        "limit":            limit,
        "action":           action,
        "status":           "queued",
        "cancel_requested": False,
        "total":            total,
        "processed":        0,
        "inserted":         0,
        "duplicates":       0,
        "queued_at":        now,
        "started_at":       now,
        "completed_at":     None,
        "error":            None,
        "run_mode":         job_type,
        "enriched":         None,
        "cleaned":          None,
        "events":           [],
    }

    with _jobs_lock:
        _jobs[job["id"]] = job
    _job_event(
        job["id"], "info", "job",
        f"Bulk {action} queued for stage '{from_stage}' — {total} candidate row(s)"
        + (f", limit {limit}." if limit else "."),
    )
    _job_queue.put((job["id"], ""))
    return jsonify({
        "ok": True, "mode": "job", "job_id": job["id"], "action": action,
        "from_stage": from_stage, "total": total,
    }), 202


@app.route("/api/stages")
def stages():
    return jsonify([
        "scraped", "geo_rejected", "no_website_prospect",
        "enriched", "enrich_failed", "clean", "flagged", "clean_failed",
        "archived",
    ])


@app.route("/api/download/<path:filename>")
def download_file(filename: str):
    """Serve files written to the data directory (e.g. clean_leads.csv from railway run)."""
    import re
    if not re.fullmatch(r"[\w\-]+\.csv", filename):
        return jsonify({"error": "Invalid filename"}), 400
    data_dir = Path(DB_PATH).parent
    file_path = data_dir / filename
    if not file_path.exists():
        return jsonify({"error": f"{filename} not found — run clean_leads.py first"}), 404
    return Response(
        file_path.read_bytes(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    threading.Thread(target=_queue_worker, daemon=True, name="job-queue-worker").start()
    app.run(
        host="0.0.0.0",
        port=env_int("PORT", 5000),
        debug=False,
    )
