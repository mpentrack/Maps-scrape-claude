"""
Flask dashboard for the Maps Data scraper pipeline.
Run: python app.py  →  http://localhost:5000
"""

import csv
import ctypes
import gc
import io
import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
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
MAX_RETRIES = env_int("MAPS_MAX_RETRIES", 3, maximum=4)
MAPS_REQUEST_TIMEOUT = env_int("MAPS_REQUEST_TIMEOUT", 15, maximum=30)
# Keep the defaults deliberately modest for Railway's memory-constrained
# containers. Every enrichment worker can hold a parsed HTML document, and a
# scrape worker can hold a Maps search + place-details response.
JOB_WORKERS = env_int("JOB_WORKERS", 3, maximum=3)
ENRICH_WORKERS = env_int("ENRICH_WORKERS", 5, maximum=6)
MX_WORKERS = env_int("MX_WORKERS", 5, maximum=8)
MAX_QUEUED_JOBS = env_int("MAX_QUEUED_JOBS", 20, maximum=50)
MAX_ZIPS_PER_JOB = env_int("MAX_ZIPS_PER_JOB", 5000, maximum=5000)
MAX_REQUEST_BYTES = env_int("MAX_REQUEST_BYTES", 262_144, maximum=1024 * 1024)
app.config["MAX_CONTENT_LENGTH"] = MAX_REQUEST_BYTES
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
DETAIL_PROBE_LIMIT = env_int("DETAIL_PROBE_LIMIT", 4, maximum=6)
DETAIL_REQUEST_TIMEOUT = env_int("DETAIL_REQUEST_TIMEOUT", 8, maximum=15)
DETAIL_CIRCUIT_SECONDS = env_int("DETAIL_CIRCUIT_SECONDS", 600, minimum=60, maximum=3600)

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
# SQLite permits many readers but only one writer.  Every write in this process
# shares this lock so the scrape workers, job checkpointing, and HTTP routes do
# not race each other for the volume's single SQLite writer slot.
_db_write_lock = threading.RLock()
MAX_JOB_EVENTS  = 200
MAX_STORED_JOBS = 30
_job_queue: queue.Queue = queue.Queue(maxsize=MAX_QUEUED_JOBS)   # (job_id, api_key) tuples
_queue_worker_thread: threading.Thread | None = None
_watchdog_thread: threading.Thread | None = None
_active_job_id: str | None = None
_worker_progress_monotonic = time.monotonic()
_job_checkpoint_monotonic: dict[str, float] = {}

# Bulk stage actions run as background jobs on the same worker as scrapes.
JOB_TYPE_SCRAPE = "scrape"
JOB_TYPE_BULK_ENRICH = "bulk_enrich"
JOB_TYPE_BULK_CLEAN = "bulk_clean"
_BULK_JOB_TYPES = frozenset({JOB_TYPE_BULK_ENRICH, JOB_TYPE_BULK_CLEAN})
# Rows per commit / progress event. Also the granularity at which a running
# bulk job notices a cancel request.
PROGRESS_CHUNK_ROWS = env_int("PROGRESS_CHUNK_ROWS", 50, maximum=100)
# A capped batch this small finishes well inside an HTTP timeout, so
# /api/pipeline/advance still answers inline for test-sized runs.
BULK_INLINE_MAX_ROWS = env_int("BULK_INLINE_MAX_ROWS", 500)

# ── Place-details probing state (shared across jobs and zips) ─────────────────
_details_cache: dict[str, dict | None] = {}
_details_lock = threading.Lock()
_DETAIL_COMBO: tuple[str, str] | None = None   # (endpoint, param key) once one works
_DETAIL_DISABLED_UNTIL = 0.0
MAX_DETAILS_CACHE = env_int("MAX_DETAILS_CACHE", 250, maximum=500)
JOB_STALL_RESTART_SECONDS = env_int(
    "JOB_STALL_RESTART_SECONDS", 300, minimum=180, maximum=1800
)
JOB_CHECKPOINT_BUSY_TIMEOUT_MS = env_int(
    "JOB_CHECKPOINT_BUSY_TIMEOUT_MS", 250, minimum=50, maximum=1000
)

# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _rss_mb() -> float | None:
    """Return current resident memory on Railway/Linux when available."""
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
    except (OSError, ValueError, IndexError):
        return None


def _release_process_memory(context: str) -> None:
    """Collect parser cycles and return free glibc heap pages to Railway."""
    gc.collect()
    try:
        libc = ctypes.CDLL("libc.so.6")
        malloc_trim = libc.malloc_trim
        malloc_trim.argtypes = [ctypes.c_size_t]
        malloc_trim.restype = ctypes.c_int
        malloc_trim(0)
    except (OSError, AttributeError):
        # macOS and non-glibc development environments do not expose
        # malloc_trim; cyclic collection above still remains effective.
        pass
    rss = _rss_mb()
    if rss is not None:
        log.info("Memory after %s: %.1f MB RSS", context, rss)


def _clear_details_cache(context: str) -> None:
    """Drop Maps response trees as soon as their pipeline phase is finished."""
    with _details_lock:
        cached = len(_details_cache)
        _details_cache.clear()
    _release_process_memory(context)
    if cached:
        log.info("Released %d cached place-detail response(s)", cached)


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
        # Scopes a full-pipeline job to the leads that job actually inserted.
        # Without this, every queued scrape repeatedly crawled the entire global
        # backlog before the next scrape could start.
        "source_job_id": "ALTER TABLE businesses ADD COLUMN source_job_id TEXT",
    }.items():
        if col not in cols:
            conn.execute(ddl)
    # Background stages page by (pipeline_stage, id). Create this after the
    # column migrations so older databases can upgrade safely.
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_businesses_stage_id
        ON businesses (pipeline_stage, id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_businesses_job_stage_id
        ON businesses (source_job_id, pipeline_stage, id)
    """)
    # A watchdog restart can leave only the small set of currently crawling
    # rows in this transient state. Quarantine them instead of retrying the
    # same pathological websites forever and blocking the rest of the queue.
    conn.execute("""
        UPDATE businesses
        SET pipeline_stage='enrich_failed', stage_reason='crawl_interrupted'
        WHERE pipeline_stage='enriching'
    """)
    conn.execute("UPDATE businesses SET pipeline_stage='scraped' WHERE pipeline_stage IS NULL OR pipeline_stage=''")
    if city_was_new:
        for row in conn.execute(
            "SELECT id, address FROM businesses WHERE address IS NOT NULL AND TRIM(address) != ''"
        ):
            cy = resolve_city(None, row["address"], None)
            if cy:
                conn.execute("UPDATE businesses SET city = ? WHERE id = ?", (cy, row["id"]))
    # Job state used to live only in process memory. Keep a compact JSON
    # snapshot on the persistent volume so a Railway restart cannot erase the
    # queue history or which ZIPs already finished.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_runs (
            id           TEXT PRIMARY KEY,
            status       TEXT NOT NULL,
            keyword      TEXT,
            updated_at   TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_runs_updated
        ON job_runs (updated_at DESC)
    """)
    # Stored separately from the public job JSON. This lets Railway requeue
    # accepted work after a process restart without exposing RapidAPI keys via
    # /api/jobs. Secrets are deleted when their job reaches a terminal state.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_secrets (
            job_id     TEXT PRIMARY KEY,
            api_key    TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


# ── Scraping helpers (self-contained so app.py has no import coupling) ────────

def _api_get(
    url: str,
    params: dict,
    api_key: str,
    *,
    log_context: str = "",
    max_attempts: int | None = None,
    request_timeout: int | None = None,
) -> dict | list | None:
    headers = {"x-rapidapi-host": API_HOST, "x-rapidapi-key": api_key}
    delay = 1.0
    ctx = f" {log_context}" if log_context else ""
    attempts = max_attempts if max_attempts is not None else MAX_RETRIES
    timeout = request_timeout if request_timeout is not None else MAPS_REQUEST_TIMEOUT
    for _ in range(attempts):
        try:
            resp = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout,
            )
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
    global _DETAIL_COMBO, _DETAIL_DISABLED_UNTIL
    with _details_lock:
        if place_token in _details_cache:
            return _details_cache[place_token]
        if time.monotonic() < _DETAIL_DISABLED_UNTIL:
            return None
        combo = _DETAIL_COMBO

    combos = [combo] if combo else [
        (ep, key) for ep in DETAIL_ENDPOINTS for key in DETAIL_PARAM_KEYS
    ][:DETAIL_PROBE_LIMIT]
    payload = None
    for ep, key in combos:
        data = _api_get(
            f"{API_BASE}{ep}", {key: place_token, "language": "en"}, api_key,
            log_context="place-detail",
            max_attempts=1,
            request_timeout=DETAIL_REQUEST_TIMEOUT,
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
        if payload is not None:
            _DETAIL_DISABLED_UNTIL = 0.0
        else:
            _DETAIL_DISABLED_UNTIL = time.monotonic() + DETAIL_CIRCUIT_SECONDS
            log.warning(
                "Place-details lookup failed; skipping optional detail lookups for %d seconds",
                DETAIL_CIRCUIT_SECONDS,
            )
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
    source_job_id: str | None = None,
) -> bool:
    phone = row.get("phone") or None
    url   = row.get("website_url") or None
    if phone is None and url is None:
        return False
    with lock, _db_write_lock:
        try:
            conn.execute(
                "INSERT INTO businesses "
                "(business_name, address, city, phone, website_url, rating, review_count, category, zip_code, search_zip, "
                "search_keyword, vertical, pipeline_stage, stage_reason, created_at, source_job_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row["business_name"], row["address"], row.get("city"), phone, url,
                 row["rating"], row["review_count"], row["category"], row["zip_code"], row.get("search_zip"),
                 search_keyword, vertical, pipeline_stage, stage_reason, _now_eastern(), source_job_id),
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
    if job_id:
        _touch_job(job_id)
    log.info("[scrape-job %s] zip=%s started query=%r", job_id or "-", zip_code, query)
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
                if _insert(conn, lock, row, "geo_rejected", "geo_zip_mismatch", keyword, vertical, job_id):
                    geo_rejected += 1
                else:
                    skipped += 1
                continue
            if not row.get("website_url"):
                if _insert(conn, lock, row, "no_website_prospect", "no_website", keyword, vertical, job_id):
                    no_website += 1
                else:
                    skipped += 1
            elif _insert(conn, lock, row, "scraped", None, keyword, vertical, job_id):
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
        if job_id:
            _touch_job(job_id)
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


def _claim_legacy_job_rows(conn: sqlite3.Connection, job: dict) -> int:
    """Attach rows written just before source_job_id existed to their job.

    Commit 84798b4 persisted the job queue before rows carried a job id. This
    narrow, time-and-keyword-bounded migration lets the currently interrupted
    production job finish without sweeping unrelated historical backlog.
    """
    job_id = job.get("id")
    keyword = job.get("keyword")
    first_queued_at = job.get("first_queued_at") or job.get("queued_at")
    if not job_id or not keyword or not first_queued_at:
        return 0
    already_tagged = conn.execute(
        "SELECT 1 FROM businesses WHERE source_job_id=? LIMIT 1", (job_id,)
    ).fetchone()
    if already_tagged:
        return 0
    try:
        queued = datetime.fromisoformat(first_queued_at)
        if queued.tzinfo is None:
            queued = queued.replace(tzinfo=UTC)
        eastern_start = queued.astimezone(_EASTERN).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return 0
    with _db_write_lock:
        cursor = conn.execute(
            """
            UPDATE businesses
            SET source_job_id=?
            WHERE source_job_id IS NULL
              AND search_keyword=?
              AND created_at>=?
              AND pipeline_stage IN ('scraped','geo_rejected','no_website_prospect','enriched','enrich_failed')
            """,
            (job_id, keyword, eastern_start),
        )
        conn.commit()
    if cursor.rowcount:
        log.info("[scrape-job %s] attached %d legacy row(s) to durable job scope", job_id, cursor.rowcount)
    return cursor.rowcount


def _run_job(job_id: str, api_key: str) -> None:
    with _jobs_lock:
        job = _jobs[job_id]
        job["status"] = "running"
        job_type = job.get("type", JOB_TYPE_SCRAPE)
    if job_type in _BULK_JOB_TYPES:
        _run_bulk_stage_job(job_id, job_type)
        return
    _job_event(job_id, "info", "scrape", "Job started.")
    _touch_job(job_id)
    with _jobs_lock:
        j = _jobs[job_id]
        completed_zips = set(j.setdefault("completed_zips", []))
        requested_zips = j.get("pending_zips")
        if requested_zips is None:
            requested_zips = [
                z for z in (j.get("zip_codes") or []) if z not in completed_zips
            ]
        zips_to_run = list(requested_zips)
    log.info(
        "[scrape-job %s] started keyword=%r vertical=%r zip_count=%d mode=%s",
        job_id,
        j.get("keyword"),
        j.get("vertical"),
        len(zips_to_run),
        j.get("run_mode", "scrape_only"),
    )

    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    lock = threading.Lock()

    try:
        with ThreadPoolExecutor(max_workers=JOB_WORKERS) as pool:
            futures = {
                pool.submit(
                    _scrape_zip, z, job["keyword"], api_key, conn, lock, job_id,
                    job.get("vertical"),
                ): z
                for z in zips_to_run
            }
            for future in as_completed(futures):
                zip_code = futures[future]
                ins, skp, geo, nw = future.result()
                with _jobs_lock:
                    job["inserted"]           += ins
                    job["duplicates"]         += skp
                    job["geo_rejected"]        = job.get("geo_rejected", 0) + geo
                    job["no_website_prospect"] = job.get("no_website_prospect", 0) + nw
                    job["processed"]          += 1
                    done = job.setdefault("completed_zips", [])
                    if zip_code not in done:
                        done.append(zip_code)
                _job_event(
                    job_id, "info", "scrape",
                    f"Processed zip {job['processed']}/{job['total']} (+{ins} new, {skp} dup, {geo} off-target, {nw} no-website).",
                )
                _touch_job(job_id)
        # Enrichment never consults Maps place-detail responses.  Keeping these
        # potentially large JSON trees alive through that phase made every
        # full-pipeline run start near Railway's memory ceiling.
        _clear_details_cache("scrape phase")
        with _jobs_lock:
            job.pop("pending_zips", None)
        if job.get("run_mode") == "full_pipeline":
            _claim_legacy_job_rows(conn, job)
            enrich_progress = _stage_progress_reporter(
                job_id, "enrich", "enriched", _ENRICH_PROGRESS_KEYS, drive_bar=False
            )
            _job_event(
                job_id,
                "info",
                "enrich",
                "Starting enrichment for this job's in-target rows. Off-target rows remain saved for optional bulk processing.",
            )
            e_scraped = _enrich_stage_rows(
                conn,
                "scraped",
                limit=None,
                progress=enrich_progress,
                heartbeat=lambda: _touch_job(job_id),
                source_job_id=job_id,
            )
            enriched = dict(e_scraped)
            _job_event(
                job_id, "info", "enrich",
                f"Enrichment complete: checked={enriched['checked']}, enriched={enriched['enriched']}, no_email={enriched['no_email']}.",
            )
            _job_event(job_id, "info", "clean", "Starting cleaning stage.")
            cleaned = _clean_stage_rows(
                conn, "enriched", limit=None,
                progress=_stage_progress_reporter(
                    job_id, "clean", "cleaned", _CLEAN_PROGRESS_KEYS, drive_bar=False
                ),
                source_job_id=job_id,
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
        _persist_job(job_id)
        conn.close()
        # Also clear on failures and scrape-only runs.
        _clear_details_cache("job cleanup")


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

    conn = get_conn()
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
            **({"heartbeat": lambda: _touch_job(job_id)} if action == "enrich" else {}),
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
        _persist_job(job_id)
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
    conn: sqlite3.Connection,
    action: str,
    from_stage: str,
    limit: int | None,
    source_job_id: str | None = None,
) -> int:
    """How many rows a bulk action would touch — used to size the progress bar."""
    where = _ENRICH_CANDIDATE_WHERE if action == "enrich" else "pipeline_stage = ?"
    params: list[object] = [from_stage]
    if source_job_id:
        where += " AND source_job_id = ?"
        params.append(source_job_id)
    total = conn.execute(
        f"SELECT COUNT(*) FROM businesses WHERE {where}", params
    ).fetchone()[0]
    return min(total, limit) if limit else total


def _enrich_stage_rows(
    conn: sqlite3.Connection,
    from_stage: str,
    limit: int | None,
    *,
    progress=None,
    should_cancel=None,
    heartbeat=None,
    source_job_id: str | None = None,
) -> dict:
    """Crawl every candidate row in `from_stage` for an email and ad-tech signals.

    Work is done in batches of PROGRESS_CHUNK_ROWS so a long run commits
    incrementally, reports progress, and can stop between batches. `progress` is
    called as progress(done, total, stats) after each batch.
    """
    total = _count_stage_rows(conn, "enrich", from_stage, limit, source_job_id)
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

    # Keyset pagination is important here. The production database can contain
    # well over 100k rows; fetchall() made a supposedly batched job materialize
    # the entire stage before its first batch and could push Railway over its
    # memory limit. Only one bounded chunk now exists in memory at a time.
    last_id = 0
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        while stats["checked"] < total:
            if should_cancel and should_cancel():
                stats["cancelled"] = True
                break
            # Only keep one worker-width of websites in flight. If every
            # website in a group wedges below Requests' socket layer, the
            # watchdog restarts the process and startup quarantines at most
            # ENRICH_WORKERS rows instead of replaying a 200-row batch forever.
            batch_size = min(PROGRESS_CHUNK_ROWS, ENRICH_WORKERS, total - stats["checked"])
            where = _ENRICH_CANDIDATE_WHERE
            params: list[object] = [from_stage]
            if source_job_id:
                where += " AND source_job_id = ?"
                params.append(source_job_id)
            batch = conn.execute(
                f"SELECT id, website_url FROM businesses "
                f"WHERE {where} AND id > ? "
                "ORDER BY id ASC LIMIT ?",
                (*params, last_id, batch_size),
            ).fetchall()
            if not batch:
                break
            last_id = batch[-1]["id"]
            row_ids = [row["id"] for row in batch]
            placeholders = ",".join("?" for _ in row_ids)
            with _db_write_lock:
                conn.execute(
                    f"UPDATE businesses SET pipeline_stage='enriching', stage_reason='crawl_in_progress' "
                    f"WHERE id IN ({placeholders})",
                    row_ids,
                )
                conn.commit()
            futures = {pool.submit(task, row): row["id"] for row in batch}
            for fut in as_completed(futures):
                # One unreachable site must never end a 45k-row run, so every
                # row is accounted for individually. A row that raises is
                # quarantined so a pathological domain cannot block every
                # later queued job on each retry.
                row_id = futures[fut]
                with _db_write_lock:
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
                        conn.execute(
                            "UPDATE businesses SET pipeline_stage='enrich_failed', "
                            "stage_reason='crawl_error', enriched_at=? WHERE id=?",
                            (datetime.utcnow().isoformat(), row_id),
                        )
                        log.warning("Enrichment failed for one row: %s", exc)
                    stats["checked"] += 1
                    # Release SQLite's writer lock before the durable job heartbeat
                    # opens its own short-lived connection.
                    conn.commit()
                if heartbeat:
                    heartbeat()
            if progress:
                progress(stats["checked"], total, stats)
            # BeautifulSoup builds cyclic object graphs. The extractor
            # decomposes them eagerly; a collection at the batch boundary also
            # prevents allocator growth over multi-hour production runs.
            gc.collect()
            if stats["checked"] % 25 == 0 or stats["checked"] == total:
                _release_process_memory(f"enrichment {stats['checked']}/{total}")
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
    source_job_id: str | None = None,
) -> dict:
    """Classify every row in `from_stage` as clean / flagged / clean_failed.

    Batched like _enrich_stage_rows: MX lookups are warmed per batch (has_mx is
    cached, so the total work is unchanged) rather than all up front, which keeps
    progress moving instead of stalling on tens of thousands of DNS lookups.
    """
    total = _count_stage_rows(conn, "clean", from_stage, limit, source_job_id)
    stats = {"checked": 0, "clean": 0, "flagged": 0, "failed": 0, "cancelled": False}
    if not total:
        if progress:
            progress(0, 0, stats)
        return stats

    now = datetime.utcnow().isoformat()
    last_id = 0
    while stats["checked"] < total:
        if should_cancel and should_cancel():
            stats["cancelled"] = True
            break
        batch_size = min(PROGRESS_CHUNK_ROWS, total - stats["checked"])
        # Cleaning only reads these three columns. Selecting every column for
        # an entire archived stage retained large address/email/signal strings
        # that the classifier never uses.
        where = "pipeline_stage = ?"
        params: list[object] = [from_stage]
        if source_job_id:
            where += " AND source_job_id = ?"
            params.append(source_job_id)
        batch = conn.execute(
            "SELECT id, business_name, email FROM businesses "
            f"WHERE {where} AND id > ? ORDER BY id ASC LIMIT ?",
            (*params, last_id, batch_size),
        ).fetchall()
        if not batch:
            break
        last_id = batch[-1]["id"]
        _warm_mx_cache(batch)
        with _db_write_lock:
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


def _job_snapshot(job: dict) -> dict:
    """Return a JSON-safe copy of a job; API keys are never part of the job."""
    return json.loads(json.dumps(job))


def _persist_job(job_id: str) -> None:
    """Upsert one job snapshot into the persistent Railway volume."""
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        snapshot = _job_snapshot(job)
    updated_at = datetime.utcnow().isoformat()
    try:
        timeout_seconds = JOB_CHECKPOINT_BUSY_TIMEOUT_MS / 1000
        with _db_write_lock:
            with closing(sqlite3.connect(DB_PATH, timeout=timeout_seconds)) as conn:
                conn.execute(f"PRAGMA busy_timeout={JOB_CHECKPOINT_BUSY_TIMEOUT_MS}")
                conn.execute(
                    """
                    INSERT INTO job_runs (id, status, keyword, updated_at, payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        status=excluded.status,
                        keyword=excluded.keyword,
                        updated_at=excluded.updated_at,
                        payload_json=excluded.payload_json
                    """,
                    (
                        job_id,
                        snapshot.get("status") or "unknown",
                        snapshot.get("keyword") or "",
                        updated_at,
                        json.dumps(snapshot, separators=(",", ":")),
                    ),
                )
                conn.commit()
    except sqlite3.Error as exc:
        # Losing one checkpoint must not end the scrape. The next event retries.
        log.warning("Could not persist job %s checkpoint: %s", job_id, exc)


def _delete_persisted_job(job_id: str) -> None:
    try:
        with _db_write_lock:
            with closing(sqlite3.connect(DB_PATH, timeout=5)) as conn:
                conn.execute("DELETE FROM job_runs WHERE id = ?", (job_id,))
                conn.commit()
    except sqlite3.Error as exc:
        log.warning("Could not delete rejected job %s checkpoint: %s", job_id, exc)


def _store_job_secret(job_id: str, api_key: str) -> bool:
    """Persist a scrape credential without ever adding it to public job JSON."""
    if not api_key:
        return True
    try:
        with _db_write_lock:
            with closing(sqlite3.connect(DB_PATH, timeout=5)) as conn:
                conn.execute("PRAGMA busy_timeout=5000")
                conn.execute(
                    """
                    INSERT INTO job_secrets (job_id, api_key, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(job_id) DO UPDATE SET
                        api_key=excluded.api_key,
                        updated_at=excluded.updated_at
                    """,
                    (job_id, api_key, datetime.utcnow().isoformat()),
                )
                conn.commit()
        return True
    except sqlite3.Error as exc:
        log.error("Could not persist recovery credential for job %s: %s", job_id, exc)
        return False


def _delete_job_secret(job_id: str) -> None:
    try:
        with _db_write_lock:
            with closing(sqlite3.connect(DB_PATH, timeout=5)) as conn:
                conn.execute("DELETE FROM job_secrets WHERE job_id=?", (job_id,))
                conn.commit()
    except sqlite3.Error as exc:
        log.warning("Could not delete recovery credential for job %s: %s", job_id, exc)


def _restore_jobs_from_db() -> None:
    """Restore history and automatically requeue jobs with recovery credentials."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT payload_json FROM job_runs ORDER BY updated_at DESC LIMIT ?",
        (MAX_STORED_JOBS,),
    ).fetchall()
    secrets = {
        row["job_id"]: row["api_key"]
        for row in conn.execute("SELECT job_id, api_key FROM job_secrets")
    }
    conn.close()
    restored: dict[str, dict] = {}
    interrupted: list[str] = []
    auto_requeue: list[tuple[str, str]] = []
    now = datetime.utcnow().isoformat()
    for row in reversed(rows):
        try:
            job = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        job_id = job.get("id")
        if not job_id:
            continue
        if job.get("status") in {"queued", "running"}:
            previous = job.get("status")
            events = job.setdefault("events", [])
            is_bulk = job.get("type") in _BULK_JOB_TYPES
            api_key = "" if is_bulk else secrets.get(job_id, "")
            if is_bulk or api_key:
                completed = set(job.setdefault("completed_zips", []))
                if not is_bulk:
                    job["pending_zips"] = [
                        z for z in (job.get("zip_codes") or []) if z not in completed
                    ]
                    job["processed"] = len(completed)
                job["status"] = "queued"
                job["completed_at"] = None
                job["error"] = None
                events.append({
                    "ts": now,
                    "level": "warning",
                    "stage": "job",
                    "message": (
                        f"Process restart detected while {previous}; automatically requeued from the durable checkpoint."
                    ),
                })
                auto_requeue.append((job_id, api_key))
            else:
                job["status"] = "interrupted"
                job["completed_at"] = now
                job["error"] = (
                    f"Railway restarted while this job was {previous}. "
                    "Completed ZIPs are saved; use Resume to continue the remainder."
                )
                events.append({
                    "ts": now,
                    "level": "warning",
                    "stage": "job",
                    "message": "Process restart detected; job paused and can be resumed.",
                })
                interrupted.append(job_id)
            job["events"] = events[-MAX_JOB_EVENTS:]
        restored[job_id] = job
    with _jobs_lock:
        _jobs.clear()
        _jobs.update(restored)
    for job_id in interrupted + [job_id for job_id, _ in auto_requeue]:
        _persist_job(job_id)
    queued_count = 0
    auto_requeue.sort(key=lambda item: restored[item[0]].get("first_queued_at") or restored[item[0]].get("queued_at") or "")
    for job_id, api_key in auto_requeue:
        try:
            _job_queue.put_nowait((job_id, api_key))
            queued_count += 1
        except queue.Full:
            with _jobs_lock:
                job = _jobs[job_id]
                job["status"] = "interrupted"
                job["completed_at"] = now
                job["error"] = "Recovery queue is full; use Resume when capacity is available."
            _persist_job(job_id)
            interrupted.append(job_id)
    recoverable_ids = {
        job_id
        for job_id, job in restored.items()
        if job.get("status") in {"queued", "running", "interrupted"}
    }
    for stale_secret_id in set(secrets) - recoverable_ids:
        _delete_job_secret(stale_secret_id)
    if restored:
        log.info(
            "Restored %d persisted job(s); %d automatically requeued; %d require manual resume",
            len(restored),
            queued_count,
            len(interrupted),
        )


def _touch_job(job_id: str) -> None:
    """Record forward progress for durability and the stall watchdog."""
    global _worker_progress_monotonic
    monotonic_now = time.monotonic()
    now = datetime.utcnow().isoformat()
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job["heartbeat_at"] = now
        if _active_job_id == job_id:
            _worker_progress_monotonic = monotonic_now
        last_checkpoint = _job_checkpoint_monotonic.get(job_id, 0)
        should_checkpoint = monotonic_now - last_checkpoint >= 5
        if should_checkpoint:
            _job_checkpoint_monotonic[job_id] = monotonic_now
    # Per-row enrichment heartbeat updates must be cheap. Persist at most once
    # every five seconds; explicit events still checkpoint immediately.
    if should_checkpoint:
        _persist_job(job_id)


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
    _persist_job(job_id)


def _enqueue_job(job: dict, api_key: str, queued_message: str) -> bool:
    """Store and enqueue a job without allowing an unbounded backlog.

    The old unbounded Queue accepted work faster than the single consumer could
    drain it. A busy or accidentally repeated submission could therefore keep
    growing process memory until Railway killed the container. Callers receive
    False and can return HTTP 429 when the configured backlog is full.
    """
    with _jobs_lock:
        _jobs[job["id"]] = job
    _job_event(job["id"], "info", "job", queued_message)
    if api_key and not _store_job_secret(job["id"], api_key):
        with _jobs_lock:
            _jobs.pop(job["id"], None)
        _delete_persisted_job(job["id"])
        return False
    try:
        _job_queue.put_nowait((job["id"], api_key))
    except queue.Full:
        with _jobs_lock:
            _jobs.pop(job["id"], None)
        _delete_job_secret(job["id"])
        _delete_persisted_job(job["id"])
        log.warning(
            "Rejected job %s because the queue reached MAX_QUEUED_JOBS=%d",
            job["id"], MAX_QUEUED_JOBS,
        )
        return False
    return True


def _prune_old_jobs() -> None:
    """Remove oldest terminal jobs from _jobs, keeping at most MAX_STORED_JOBS."""
    TERMINAL = {"completed", "failed", "cancelled", "interrupted"}
    with _jobs_lock:
        terminal = [j for j in _jobs.values() if j["status"] in TERMINAL]
        if len(terminal) <= MAX_STORED_JOBS:
            return
        terminal.sort(key=lambda j: j.get("started_at") or "")
        for j in terminal[: len(terminal) - MAX_STORED_JOBS]:
            del _jobs[j["id"]]
            _job_checkpoint_monotonic.pop(j["id"], None)


def _job_watchdog() -> None:
    """Restart a wedged process instead of leaving the queue frozen for hours."""
    while True:
        time.sleep(30)
        job_id = _active_job_id
        if not job_id:
            continue
        stalled_for = time.monotonic() - _worker_progress_monotonic
        if stalled_for < JOB_STALL_RESTART_SECONDS:
            continue
        log.critical(
            "Job %s made no forward progress for %.0f seconds; exiting so Railway can restart it",
            job_id,
            stalled_for,
        )
        # The persisted status remains running. Startup converts it to an
        # interrupted/resumable record before accepting more work.
        logging.shutdown()
        os._exit(75)


def _queue_worker() -> None:
    """Single consumer thread: runs one job at a time from _job_queue."""
    global _active_job_id, _worker_progress_monotonic
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
            _active_job_id = job_id
            _worker_progress_monotonic = time.monotonic()
            _run_job(job_id, api_key)
        except Exception:
            log.exception("Unexpected error in _queue_worker for job %s", job_id)
        finally:
            _active_job_id = None
            _job_queue.task_done()
            with _jobs_lock:
                final_status = (_jobs.get(job_id) or {}).get("status")
            if final_status in {"completed", "failed", "cancelled"}:
                _delete_job_secret(job_id)
            _prune_old_jobs()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", states=STATE_NAMES)


@app.route("/health")
def health():
    """Cheap Railway health check that never waits on the busy SQLite file."""
    with _jobs_lock:
        active = sum(j["status"] in {"queued", "running"} for j in _jobs.values())
    worker_alive = bool(_queue_worker_thread and _queue_worker_thread.is_alive())
    stalled_for = (
        round(time.monotonic() - _worker_progress_monotonic, 1)
        if _active_job_id else 0
    )
    stalled = bool(_active_job_id and stalled_for >= JOB_STALL_RESTART_SECONDS)
    response = jsonify({
        "ok": worker_alive and not stalled,
        "worker_alive": worker_alive,
        "active_job_id": _active_job_id,
        "seconds_since_progress": stalled_for,
        "stalled": stalled,
        "active_jobs": active,
        "queue_depth": _job_queue.qsize(),
        "queue_capacity": MAX_QUEUED_JOBS,
    })
    return response, 200 if worker_alive and not stalled else 503


@app.errorhandler(413)
def request_too_large(_error):
    return jsonify({
        "error": f"Request is too large (maximum {MAX_REQUEST_BYTES:,} bytes).",
    }), 413


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
    _delete_job_secret(job_id)
    return jsonify({"ok": True, "pending": False})


@app.route("/api/jobs/<job_id>/resume", methods=["POST"])
def resume_job(job_id: str):
    """Resume an interrupted job without repeating completed ZIPs."""
    data = request.get_json(silent=True) or {}
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        if job.get("status") != "interrupted":
            return jsonify({"error": "Only interrupted jobs can be resumed"}), 409
        job_type = job.get("type", JOB_TYPE_SCRAPE)
        is_bulk = job_type in _BULK_JOB_TYPES
    api_key = (data.get("api_key") or "").strip()
    if not is_bulk and not api_key:
        return jsonify({"error": "RapidAPI key is required to resume this scrape"}), 400

    now = datetime.utcnow().isoformat()
    with _jobs_lock:
        job = _jobs[job_id]
        previous_error = job.get("error")
        job.setdefault("first_queued_at", job.get("queued_at"))
        job["last_interrupted_at"] = job.get("completed_at")
        if is_bulk:
            # Rows completed before the restart already moved stages, so the
            # source-stage count is the exact remaining workload.
            conn = get_conn()
            try:
                remaining_count = _count_stage_rows(
                    conn, job.get("action") or "clean", job.get("from_stage") or "", job.get("limit")
                )
            finally:
                conn.close()
            job["total"] = remaining_count
            job["processed"] = 0
            job["enriched"] = None
            job["cleaned"] = None
            remaining_zips = []
        else:
            completed = set(job.setdefault("completed_zips", []))
            remaining_zips = [
                z for z in (job.get("zip_codes") or []) if z not in completed
            ]
            job["pending_zips"] = remaining_zips
            job["processed"] = len(completed)
        job["status"] = "queued"
        job["error"] = None
        job["completed_at"] = None
        job["queued_at"] = now
        job["started_at"] = now
        job["resume_count"] = int(job.get("resume_count") or 0) + 1
    if api_key and not _store_job_secret(job_id, api_key):
        with _jobs_lock:
            job = _jobs[job_id]
            job["status"] = "interrupted"
            job["error"] = previous_error
        _persist_job(job_id)
        return jsonify({"error": "Could not save the recovery credential; try Resume again"}), 503
    try:
        _job_queue.put_nowait((job_id, api_key))
    except queue.Full:
        with _jobs_lock:
            job = _jobs[job_id]
            job["status"] = "interrupted"
            job["error"] = previous_error
        _delete_job_secret(job_id)
        _persist_job(job_id)
        response = jsonify({"error": "The job queue is full; try Resume again shortly"})
        response.headers["Retry-After"] = "30"
        return response, 429
    _job_event(
        job_id,
        "info",
        "job",
        (
            "Resume queued for the remaining stage rows."
            if is_bulk else
            f"Resume queued with {len(remaining_zips)} ZIP(s) remaining; completed ZIPs will not repeat."
        ),
    )
    return jsonify({"ok": True, "id": job_id, "remaining": len(remaining_zips)}), 202


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
    invalid_zips = [z for z in zips if not re.fullmatch(r"\d{5}", z)]
    if invalid_zips:
        sample = ", ".join(invalid_zips[:3])
        return jsonify({"error": f"ZIP codes must contain exactly five digits: {sample}"}), 400
    if len(zips) > MAX_ZIPS_PER_JOB:
        return jsonify({
            "error": (
                f"This job contains {len(zips):,} ZIP codes; the per-job maximum is "
                f"{MAX_ZIPS_PER_JOB:,}. Split it into smaller jobs."
            ),
        }), 400
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
        "completed_zips":      [],
        "inserted":            0,
        "duplicates":          0,
        "geo_rejected":        0,
        "no_website_prospect": 0,
        "queued_at":           datetime.utcnow().isoformat(),
        "first_queued_at":     datetime.utcnow().isoformat(),
        "started_at":          datetime.utcnow().isoformat(),
        "completed_at":        None,
        "heartbeat_at":        datetime.utcnow().isoformat(),
        "error":               None,
        "run_mode":            run_mode,
        "enriched":            None,
        "cleaned":             None,
        "events":              [],
    }

    queued_message = (
        f"Job queued ({run_mode}). Keyword='{keyword}'. Vertical='{vertical}'. "
        f"Target zips={len(zips)}."
    )
    if not _enqueue_job(job, api_key, queued_message):
        response = jsonify({
            "error": (
                f"The job queue is full ({MAX_QUEUED_JOBS} waiting). "
                "Let an existing job finish or cancel one before adding another."
            ),
        })
        response.headers["Retry-After"] = "30"
        return response, 429
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

    header = columns[:-1] + ["scraped_at_et"]

    def generate():
        # Keep only a small cursor batch and CSV chunk in memory. The previous
        # fetchall() + one giant StringIO could OOM the web process when
        # exporting the six-figure production table while jobs were active.
        conn = get_conn()
        try:
            cursor = conn.execute(sql, params)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(header)
            yield buf.getvalue()
            while True:
                rows = cursor.fetchmany(500)
                if not rows:
                    break
                buf.seek(0)
                buf.truncate(0)
                for row in rows:
                    writer.writerow(list(row))
                yield buf.getvalue()
        finally:
            conn.close()

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
        "first_queued_at":  now,
        "started_at":       now,
        "completed_at":     None,
        "heartbeat_at":     now,
        "error":            None,
        "run_mode":         job_type,
        "enriched":         None,
        "cleaned":          None,
        "events":           [],
    }

    queued_message = (
        f"Bulk {action} queued for stage '{from_stage}' — {total} candidate row(s)"
        + (f", limit {limit}." if limit else ".")
    )
    if not _enqueue_job(job, "", queued_message):
        response = jsonify({
            "error": (
                f"The job queue is full ({MAX_QUEUED_JOBS} waiting). "
                "Let an existing job finish or cancel one before adding another."
            ),
        })
        response.headers["Retry-After"] = "30"
        return response, 429
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
    _restore_jobs_from_db()
    _queue_worker_thread = threading.Thread(
        target=_queue_worker,
        daemon=True,
        name="job-queue-worker",
    )
    _queue_worker_thread.start()
    _watchdog_thread = threading.Thread(
        target=_job_watchdog,
        daemon=True,
        name="job-stall-watchdog",
    )
    _watchdog_thread.start()
    app.run(
        host="0.0.0.0",
        port=env_int("PORT", 5000),
        debug=False,
    )
