"""
Maps Data API scraper — RapidAPI (alexanderxbx/maps-data).

Usage:
    python scraper.py --csv zips.csv --keyword "plumbers" --api-key YOUR_KEY
"""

import argparse
import csv
import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import requests

from city_parse import resolve_city
from geo_zip import listing_matches_search_zip

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
            created_at   TEXT DEFAULT (datetime('now'))
        )
    """)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(businesses)")}
    if "city" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN city TEXT")
    # Composite unique index for dedup logic
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_phone_url
        ON businesses (phone, website_url)
        WHERE phone IS NOT NULL OR website_url IS NOT NULL
    """)
    conn.commit()
    return conn


_db_lock = Lock()


def insert_business(conn: sqlite3.Connection, row: dict) -> bool:
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
                     rating, review_count, category, zip_code)
                VALUES (?,?,?,?,?,?,?,?,?)
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
                return resp.json()
            if resp.status_code == 429:
                log.warning("Rate-limited (429). Waiting %.1fs (attempt %d/%d).", delay, attempt, MAX_RETRIES)
                time.sleep(delay)
                delay *= 2
                continue
            log.error("HTTP %s for %s — skipping.", resp.status_code, url)
            return None
        except requests.RequestException as exc:
            log.warning("Request error: %s. Waiting %.1fs.", exc, delay)
            time.sleep(delay)
            delay *= 2
    log.error("Gave up after %d attempts for %s", MAX_RETRIES, url)
    return None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_result(item: dict, zip_code: str) -> dict:
    raw_addr = item.get("full_address") or item.get("address")
    if isinstance(raw_addr, dict):
        address = raw_addr.get("formatted_address") or raw_addr.get("formatted")
    else:
        address = raw_addr if isinstance(raw_addr, str) else None
    city = resolve_city(item, address, zip_code)
    return {
        "business_name": item.get("name") or item.get("title"),
        "address":       address,
        "city":          city,
        "phone":         item.get("phone_number") or item.get("phone"),
        "website_url":   item.get("website"),
        "rating":        item.get("rating"),
        "review_count":  item.get("reviews") or item.get("review_count"),
        "category":      (item.get("types") or [""])[0] if isinstance(item.get("types"), list) else item.get("type") or item.get("category"),
        "zip_code":      zip_code,
    }


# ---------------------------------------------------------------------------
# Per-zip scrape
# ---------------------------------------------------------------------------

def scrape_zip(zip_code: str, keyword: str, api_key: str, conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Return (inserted, skipped, geo_rejected) counts for one zip code."""
    inserted = skipped = geo_rejected = 0

    for page in range(1, MAX_PAGES + 1):
        params = {
            "query": keyword,
            "zipcode": zip_code,
            "country": "us",
            "limit": PAGE_SIZE,
            "offset": (page - 1) * PAGE_SIZE,
            "language": "en",
        }
        data = _get_with_backoff(f"{API_BASE}/searchmaps.php", params, api_key)

        if data is None:
            break

        # The API may return results under different keys depending on version
        results = (
            data.get("data")
            or data.get("results")
            or data.get("businesses")
            or []
        )

        if not results:
            break

        for item in results:
            row = _parse_result(item, zip_code)
            if not listing_matches_search_zip(item, row.get("address"), zip_code):
                geo_rejected += 1
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
