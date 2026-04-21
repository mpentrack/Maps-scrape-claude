"""
Maps Data API scraper — RapidAPI (alexanderxbx/maps-data).

Usage:
    python scraper.py --csv zips.csv --keyword "plumbers" --api-key YOUR_KEY
"""

import argparse
import csv
import logging
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import requests

from city_parse import formatted_address_from_item, resolve_city
from geo_zip import best_listing_zip, listing_matches_search_zip, normalize_zip5
from maps_item import contact_fields_from_maps_item, iter_search_results
from zip_geocode import us_zip_latlng

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_HOST = "maps-data.p.rapidapi.com"
API_BASE = f"https://{API_HOST}"
DB_PATH = "businesses.db"
MAX_WORKERS = 10
PAGE_SIZE = 20          # results per page (API default)
MAX_PAGES = 10          # safety cap per zip
BACKOFF_BASE = 1.0      # seconds; doubles each retry
MAX_RETRIES = 6
APPEND_ZIP_TO_QUERY = os.environ.get("APPEND_ZIP_TO_QUERY", "1").strip().lower() not in ("0", "false", "no", "off")
DETAIL_ENDPOINTS = (
    "/place.php",
    "/place-details.php",
    "/placedetails.php",
    "/place_details.php",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS businesses (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            business_name TEXT,
            address      TEXT,
            city         TEXT,
            phone        TEXT,
            website_url  TEXT,
            rating       REAL,
            review_count INTEGER,
            category     TEXT,
            zip_code     TEXT,
            search_zip   TEXT,
            pipeline_stage TEXT DEFAULT 'scraped',
            stage_reason TEXT,
            created_at   TEXT DEFAULT (datetime('now'))
        )
    """)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(businesses)")}
    for col, ddl in [
        ("city", "ALTER TABLE businesses ADD COLUMN city TEXT"),
        ("search_zip", "ALTER TABLE businesses ADD COLUMN search_zip TEXT"),
        ("pipeline_stage", "ALTER TABLE businesses ADD COLUMN pipeline_stage TEXT DEFAULT 'scraped'"),
        ("stage_reason", "ALTER TABLE businesses ADD COLUMN stage_reason TEXT"),
    ]:
        if col not in cols:
            conn.execute(ddl)
    # Composite unique index for dedup logic
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_phone_url
        ON businesses (phone, website_url)
        WHERE phone IS NOT NULL OR website_url IS NOT NULL
    """)
    conn.commit()
    return conn


_db_lock = Lock()


def insert_business(
    conn: sqlite3.Connection,
    row: dict,
    pipeline_stage: str = "scraped",
    stage_reason: str | None = None,
) -> bool:
    """Insert row; return True if inserted, False if duplicate."""
    phone = row.get("phone") or None
    url   = row.get("website_url") or None

    # Manual dedup when both are NULL (index won't catch it)
    if phone is None and url is None:
        return False

    with _db_lock:
        try:
            conn.execute(
                """
                INSERT INTO businesses
                    (business_name, address, city, phone, website_url,
                     rating, review_count, category, zip_code, search_zip, pipeline_stage, stage_reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row.get("business_name"),
                    row.get("address"),
                    row.get("city"),
                    phone,
                    url,
                    row.get("rating"),
                    row.get("review_count"),
                    row.get("category"),
                    row.get("zip_code"),
                    row.get("search_zip"),
                    pipeline_stage,
                    stage_reason,
                ),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # duplicate


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers(api_key: str) -> dict:
    return {
        "x-rapidapi-host": API_HOST,
        "x-rapidapi-key": api_key,
    }


def _get_with_backoff(url: str, params: dict, api_key: str) -> dict | None:
    delay = BACKOFF_BASE
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                headers=_headers(api_key),
                params=params,
                timeout=30,
            )
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    log.warning("Non-JSON 200 from %s: %s", url, resp.text[:300])
                    return None
            if resp.status_code == 429:
                log.warning("Rate-limited (429). Waiting %.1fs (attempt %d/%d).", delay, attempt, MAX_RETRIES)
                time.sleep(delay)
                delay *= 2
                continue
            log.error("HTTP %s for %s — %s", resp.status_code, url, (resp.text or "")[:500])
            return None
        except requests.RequestException as exc:
            log.warning("Request error: %s. Waiting %.1fs.", exc, delay)
            time.sleep(delay)
            delay *= 2
    log.error("Gave up after %d attempts for %s", MAX_RETRIES, url)
    return None


def _query_for_zip(keyword: str, zip_code: str) -> str:
    q = (keyword or "").strip()
    z = (zip_code or "").strip()
    if not z or not APPEND_ZIP_TO_QUERY:
        return q
    if z in q:
        return q
    return f"{q} {z}".strip()


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_result(item: dict, zip_code: str) -> dict:
    phone, website = contact_fields_from_maps_item(item)
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
        "category":      (item.get("types") or [""])[0] if isinstance(item.get("types"), list) else item.get("type") or item.get("category"),
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
            data = _get_with_backoff(f"{API_BASE}{ep}", params, api_key)
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


# ---------------------------------------------------------------------------
# Per-zip scrape
# ---------------------------------------------------------------------------

def scrape_zip(zip_code: str, keyword: str, api_key: str, conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Return (inserted, skipped, geo_rejected) counts for one zip code."""
    inserted = skipped = geo_rejected = 0
    details_cache: dict[str, dict | None] = {}
    query = _query_for_zip(keyword, zip_code)
    coords = us_zip_latlng(zip_code)
    if not coords:
        log.warning(
            "Could not geocode US zip %s — omitting lat/lng on Maps search (often empty).",
            zip_code,
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
        data = _get_with_backoff(f"{API_BASE}/searchmaps.php", params, api_key)

        if data is None:
            break

        results = iter_search_results(data)
        if not results:
            log.warning(
                "No business list in search response zip=%s page=%d keys=%s",
                zip_code,
                page,
                list(data.keys())[:24] if isinstance(data, dict) else type(data).__name__,
            )
            break

        for item in results:
            row = _parse_result(item, zip_code)
            item_for_match = _hydrate_row_location(row, item, zip_code, api_key, details_cache)
            row["zip_code"] = best_listing_zip(item_for_match, row.get("address"))
            expected_zip = normalize_zip5(zip_code)
            strict_exact = os.environ.get("STRICT_EXACT_ZIP", "1").strip().lower() not in ("0", "false", "no", "off")
            exact_ok = bool(row.get("zip_code") and expected_zip and row["zip_code"] == expected_zip)
            geo_ok = listing_matches_search_zip(item_for_match, row.get("address"), zip_code)
            accept = exact_ok if strict_exact else geo_ok
            if not accept:
                if insert_business(conn, row, "geo_rejected", "geo_zip_mismatch"):
                    geo_rejected += 1
                else:
                    skipped += 1
                continue
            if insert_business(conn, row):
                inserted += 1
            else:
                skipped += 1

        log.info(
            "ZIP %-10s page %2d — +%d new, %d dup, %d off-target",
            zip_code, page, inserted, skipped, geo_rejected,
        )

        if len(results) < PAGE_SIZE:
            break  # last page

    return inserted, skipped, geo_rejected


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_zips(csv_path: str) -> list[str]:
    zips = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            for cell in row:
                val = cell.strip()
                if val and val != "zip_code" and val != "zipcode":
                    zips.append(val.zfill(5))
    return list(dict.fromkeys(zips))  # deduplicate, preserve order


def main():
    parser = argparse.ArgumentParser(description="Scrape Maps Data API by zip code.")
    parser.add_argument("--csv",     required=True, help="Path to CSV file of US zip codes.")
    parser.add_argument("--keyword", required=True, help='Search keyword, e.g. "plumbers".')
    parser.add_argument("--api-key", required=True, help="RapidAPI key.")
    parser.add_argument("--db",      default=DB_PATH, help=f"SQLite output path (default: {DB_PATH}).")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Thread pool size (default: 10).")
    args = parser.parse_args()

    if not Path(args.csv).exists():
        parser.error(f"CSV file not found: {args.csv}")

    conn = init_db(args.db)
    zips = load_zips(args.csv)
    log.info("Loaded %d zip codes. Keyword: '%s'", len(zips), args.keyword)

    total_inserted = total_skipped = total_geo = 0
    lock = Lock()

    def task(zip_code):
        return zip_code, scrape_zip(zip_code, args.keyword, args.api_key, conn)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(task, z): z for z in zips}
        for future in as_completed(futures):
            z = futures[future]
            try:
                _, (ins, skp, geo) = future.result()
                with lock:
                    total_inserted += ins
                    total_skipped  += skp
                    total_geo      += geo
            except Exception as exc:
                log.error("ZIP %s failed: %s", z, exc)

    log.info(
        "Done. Total inserted: %d | duplicates skipped: %d | off-target skipped: %d | DB: %s",
        total_inserted, total_skipped, total_geo, args.db,
    )
    conn.close()


if __name__ == "__main__":
    main()
