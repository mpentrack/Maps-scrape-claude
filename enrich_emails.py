"""
Email enrichment — reads website_url from SQLite, scrapes emails, writes back.

Usage:
    python enrich_emails.py
    python enrich_emails.py --db businesses.db --workers 20
    python enrich_emails.py --allow-generic-fallback
"""

import argparse
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests

from email_extract import USER_AGENT, scrape_email_for_website

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = "businesses.db"
MAX_WORKERS = 20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def ensure_email_column(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(businesses)")}
    if "email" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN email TEXT")
        conn.commit()
        log.info("Added 'email' column to businesses table.")


def fetch_pending(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    rows = conn.execute(
        "SELECT id, website_url FROM businesses "
        "WHERE website_url IS NOT NULL AND (email IS NULL OR email = '') "
        "AND COALESCE(pipeline_stage, 'scraped') != 'geo_rejected'"
    ).fetchall()
    return rows


_db_lock = Lock()


def save_email(conn: sqlite3.Connection, row_id: int, email: str) -> None:
    with _db_lock:
        conn.execute("UPDATE businesses SET email = ? WHERE id = ?", (email, row_id))
        conn.commit()


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def scrape_email(website_url: str, *, allow_generic_fallback: bool) -> str | None:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    try:
        res = scrape_email_for_website(
            website_url,
            session,
            allow_generic_fallback=allow_generic_fallback,
        )
        return res.email
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Enrich businesses DB with emails.")
    parser.add_argument("--db", default=DB_PATH, help=f"SQLite DB path (default: {DB_PATH}).")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Thread pool size (default: 20).")
    parser.add_argument(
        "--allow-generic-fallback",
        action="store_true",
        help="Store info@/hello@/etc. when no better address exists (default: off).",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, check_same_thread=False)
    ensure_email_column(conn)

    pending = fetch_pending(conn)
    log.info("%d URLs to process.", len(pending))

    found = skipped = 0
    counter_lock = Lock()

    def task(row_id: int, url: str) -> tuple[int, str | None]:
        email = scrape_email(url, allow_generic_fallback=args.allow_generic_fallback)
        return row_id, email

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(task, rid, url): (rid, url) for rid, url in pending}
        for future in as_completed(futures):
            rid, url = futures[future]
            try:
                row_id, email = future.result()
                if email:
                    save_email(conn, row_id, email)
                    log.info("  %-50s -> %s", url[:50], email)
                    with counter_lock:
                        found += 1
                else:
                    log.debug("No email found: %s", url)
                    with counter_lock:
                        skipped += 1
            except Exception as exc:
                log.error("Failed %s: %s", url, exc)
                with counter_lock:
                    skipped += 1

    log.info("Done. Emails found: %d | none found: %d", found, skipped)
    conn.close()


if __name__ == "__main__":
    main()
