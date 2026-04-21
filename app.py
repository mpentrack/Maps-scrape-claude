"""
Flask dashboard for the Maps Data scraper pipeline.
Run: python app.py  →  http://localhost:5000
"""

import csv
import io
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, render_template, request

from city_parse import formatted_address_from_item, resolve_city
from email_extract import USER_AGENT as ENRICH_USER_AGENT, scrape_email_for_website
from maps_item import contact_fields_from_maps_item, iter_search_results
from zip_geocode import us_zip_latlng
from geo_zip import best_listing_zip, listing_matches_search_zip, normalize_zip5
from state_zips import STATE_NAMES, STATE_ZIPS

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
DB_PATH     = os.environ.get("DB_PATH", "businesses.db")
API_HOST    = "maps-data.p.rapidapi.com"
API_BASE    = f"https://{API_HOST}"
PAGE_SIZE   = 20
MAX_PAGES   = 10
MAX_RETRIES = 6
JOB_WORKERS = 5   # concurrent zips per job
ENRICH_WORKERS = int(os.environ.get("ENRICH_WORKERS", "20"))
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

# ── In-memory job store ───────────────────────────────────────────────────────
_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()
MAX_JOB_EVENTS = 200

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


def _fetch_place_details(place_token: str, api_key: str, cache: dict[str, dict | None]) -> dict | None:
    if place_token in cache:
        return cache[place_token]
    for ep in DETAIL_ENDPOINTS:
        for params in (
            {"place_id": place_token, "language": "en"},
            {"google_id": place_token, "language": "en"},
            {"cid": place_token, "language": "en"},
        ):
            data = _api_get(f"{API_BASE}{ep}", params, api_key, log_context="place-detail")
            payload = _first_dict_payload(data)
            if isinstance(payload, dict) and payload:
                cache[place_token] = payload
                return payload
    cache[place_token] = None
    return None


def _hydrate_row_location(row: dict, item: dict, zip_code: str, api_key: str, cache: dict[str, dict | None]) -> dict:
    if row.get("address") and row.get("city"):
        return item
    token = _extract_place_token(item)
    if not token:
        return item
    details = _fetch_place_details(token, api_key, cache)
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
) -> bool:
    phone = row.get("phone") or None
    url   = row.get("website_url") or None
    if phone is None and url is None:
        return False
    with lock:
        try:
            conn.execute(
                "INSERT INTO businesses "
                "(business_name, address, city, phone, website_url, rating, review_count, category, zip_code, search_zip, pipeline_stage, stage_reason) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (row["business_name"], row["address"], row.get("city"), phone, url,
                 row["rating"], row["review_count"], row["category"], row["zip_code"], row.get("search_zip"),
                 pipeline_stage, stage_reason),
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
) -> tuple[int, int, int]:
    inserted = skipped = geo_rejected = 0
    details_cache: dict[str, dict | None] = {}
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
            "lang": "en",
            "zoom": 12,
        }
        if coords:
            params["lat"], params["lng"] = coords[0], coords[1]
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
            item_for_match = _hydrate_row_location(row, item, zip_code, api_key, details_cache)
            row["zip_code"] = best_listing_zip(item_for_match, row.get("address"))
            expected_zip = normalize_zip5(zip_code)
            strict_exact = os.environ.get("STRICT_EXACT_ZIP", "1").strip().lower() not in ("0", "false", "no", "off")
            exact_ok = bool(row.get("zip_code") and expected_zip and row["zip_code"] == expected_zip)
            geo_ok = listing_matches_search_zip(item_for_match, row.get("address"), zip_code)
            accept = exact_ok if strict_exact else geo_ok
            if not accept:
                if _insert(conn, lock, row, "geo_rejected", "geo_zip_mismatch"):
                    geo_rejected += 1
                else:
                    skipped += 1
                continue
            if _insert(conn, lock, row):
                inserted += 1
            else:
                skipped += 1
        log.info(
            "[scrape-job %s] zip=%s page=%d api_rows=%d cumulative new=%d dup=%d off_target=%d query=%r",
            job_id or "-",
            zip_code,
            page,
            len(results),
            inserted,
            skipped,
            geo_rejected,
            query,
        )
        if len(results) < PAGE_SIZE:
            break
    log.info(
        "[scrape-job %s] zip=%s finished cumulative new=%d dup=%d off_target=%d",
        job_id or "-",
        zip_code,
        inserted,
        skipped,
        geo_rejected,
    )
    return inserted, skipped, geo_rejected


def _run_job(job_id: str, api_key: str) -> None:
    with _jobs_lock:
        job = _jobs[job_id]
        job["status"] = "running"
    _job_event(job_id, "info", "scrape", "Job started.")
    with _jobs_lock:
        j = _jobs[job_id]
    log.info(
        "[scrape-job %s] started keyword=%r zip_count=%d mode=%s",
        job_id,
        j.get("keyword"),
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
                pool.submit(_scrape_zip, z, job["keyword"], api_key, conn, lock, job_id): z
                for z in job["zip_codes"]
            }
            for future in as_completed(futures):
                ins, skp, geo = future.result()
                with _jobs_lock:
                    job["inserted"]      += ins
                    job["duplicates"]    += skp
                    job["geo_rejected"]  = job.get("geo_rejected", 0) + geo
                    job["processed"]     += 1
                _job_event(
                    job_id, "info", "scrape",
                    f"Processed zip {job['processed']}/{job['total']} (+{ins} new, {skp} dup, {geo} off-target).",
                )
        if job.get("run_mode") == "full_pipeline":
            _job_event(job_id, "info", "enrich", "Starting enrichment (in-target scraped rows).")
            e_scraped = _enrich_stage_rows(conn, "scraped", limit=None)
            _job_event(job_id, "info", "enrich", "Starting enrichment (off-target / geo_rejected rows).")
            e_geo = _enrich_stage_rows(conn, "geo_rejected", limit=None)
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
            cleaned = _clean_stage_rows(conn, "enriched", limit=None)
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
            "[scrape-job %s] completed new=%d dup=%d off_target=%d mode=%s",
            job_id,
            fin.get("inserted", 0),
            fin.get("duplicates", 0),
            fin.get("geo_rejected", 0),
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


def _enrich_stage_rows(conn: sqlite3.Connection, from_stage: str, limit: int | None) -> dict:
    where = "WHERE pipeline_stage = ? AND website_url IS NOT NULL AND (email IS NULL OR email = '')"
    params = [from_stage]
    query = "SELECT id, website_url FROM businesses " + where + " ORDER BY id ASC"
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(query, params).fetchall()
    if not rows:
        return {"checked": 0, "enriched": 0, "no_email": 0}

    result_lock = threading.Lock()
    enriched = 0
    no_email = 0

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
        futures = [pool.submit(task, row) for row in rows]
        for fut in as_completed(futures):
            row_id, res = fut.result()
            now = datetime.utcnow().isoformat()
            if res.email:
                conn.execute(
                    "UPDATE businesses SET email=?, pipeline_stage='enriched', stage_reason=NULL, enriched_at=? WHERE id=?",
                    (res.email, now, row_id),
                )
                with result_lock:
                    enriched += 1
            else:
                reason = res.stage_reason or "no_email_found"
                conn.execute(
                    "UPDATE businesses SET pipeline_stage='enrich_failed', stage_reason=?, enriched_at=? WHERE id=?",
                    (reason, now, row_id),
                )
                with result_lock:
                    no_email += 1
            conn.commit()
    return {"checked": len(rows), "enriched": enriched, "no_email": no_email}


def _classify_clean_stage(row: sqlite3.Row) -> tuple[str, str | None]:
    email = (row["email"] or "").strip().lower()
    name = (row["business_name"] or "").lower()
    review_count = row["review_count"]
    if not email:
        return "clean_failed", "missing_email"
    if FLAG_FREE_EMAIL_DOMAINS and email.split("@")[-1] in PERSONAL_DOMAINS:
        return "flagged", "personal_email_domain"
    if "permanently closed" in name:
        return "flagged", "permanently_closed"
    if review_count is None or review_count < 5:
        return "flagged", "low_review_count"
    return "clean", None


def _clean_stage_rows(conn: sqlite3.Connection, from_stage: str, limit: int | None) -> dict:
    query = "SELECT * FROM businesses WHERE pipeline_stage = ? ORDER BY id ASC"
    params = [from_stage]
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(query, params).fetchall()
    clean = 0
    flagged = 0
    failed = 0
    now = datetime.utcnow().isoformat()
    for row in rows:
        stage, reason = _classify_clean_stage(row)
        conn.execute(
            "UPDATE businesses SET pipeline_stage=?, stage_reason=?, cleaned_at=? WHERE id=?",
            (stage, reason, now, row["id"]),
        )
        if stage == "clean":
            clean += 1
        elif stage == "flagged":
            flagged += 1
        else:
            failed += 1
    conn.commit()
    return {"checked": len(rows), "clean": clean, "flagged": flagged, "failed": failed}


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
    job = {
        "id":           str(uuid.uuid4())[:8],
        "keyword":      keyword,
        "state":        state,
        "zip_codes":    zips,
        "status":       "pending",
        "total":        len(zips),
        "processed":    0,
        "inserted":     0,
        "duplicates":   0,
        "geo_rejected": 0,
        "started_at":   datetime.utcnow().isoformat(),
        "completed_at": None,
        "error":        None,
        "run_mode":     run_mode,
        "enriched":     None,
        "cleaned":      None,
        "events":       [],
    }

    with _jobs_lock:
        _jobs[job["id"]] = job
    _job_event(job["id"], "info", "job", f"Job created ({run_mode}). Keyword='{keyword}'. Target zips={len(zips)}.")

    threading.Thread(target=_run_job, args=(job["id"], api_key), daemon=True).start()
    return jsonify({"id": job["id"]}), 202


@app.route("/api/stats")
def stats():
    try:
        conn  = get_conn()
        total = conn.execute("SELECT COUNT(*) FROM businesses").fetchone()[0]
        with_email = conn.execute(
            "SELECT COUNT(*) FROM businesses WHERE email IS NOT NULL AND email != ''"
        ).fetchone()[0]
        categories = conn.execute(
            "SELECT category, COUNT(*) cnt FROM businesses "
            "WHERE category IS NOT NULL GROUP BY category ORDER BY cnt DESC LIMIT 12"
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

    where, params = _build_where(min_rating, min_reviews, has_email, category, stage)

    conn = get_conn()
    rows = conn.execute(
        f"SELECT business_name, address, city, phone, website_url, email, "
        f"rating, review_count, category, zip_code, search_zip, pipeline_stage, stage_reason "
        f"FROM businesses {where} ORDER BY id DESC",
        params,
    ).fetchall()
    conn.close()

    def generate():
        buf = io.StringIO()
        w   = csv.writer(buf)
        w.writerow(["business_name","address","city","phone","website_url","email",
                    "rating","review_count","category","zip_code","search_zip","pipeline_stage","stage_reason"])
        for row in rows:
            w.writerow(list(row))
        yield buf.getvalue()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{timestamp}.csv"},
    )


def _build_where(min_rating: float, min_reviews: int, has_email: bool, category: str, stage: str):
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
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


@app.route("/api/pipeline/advance", methods=["POST"])
def advance_pipeline():
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

    conn = get_conn()
    try:
        if action == "enrich":
            result = _enrich_stage_rows(conn, from_stage, limit)
        else:
            result = _clean_stage_rows(conn, from_stage, limit)
        return jsonify({"ok": True, "action": action, "from_stage": from_stage, "result": result})
    finally:
        conn.close()


@app.route("/api/stages")
def stages():
    return jsonify([
        "scraped", "geo_rejected", "enriched", "enrich_failed", "clean", "flagged", "clean_failed",
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
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=False,
    )
