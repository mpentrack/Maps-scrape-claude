"""
Email enrichment — reads website_url from SQLite, scrapes emails, writes back.

Usage:
    python enrich_emails.py
    python enrich_emails.py --db businesses.db --workers 20
"""

import argparse
import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = "businesses.db"
MAX_WORKERS = 20
REQUEST_TIMEOUT = 5          # seconds
MAX_ATTEMPTS = 2             # per URL before giving up
SUBPAGES = ["/contact", "/contact-us", "/about", "/about-us"]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

GENERIC_PREFIXES = {"info", "hello", "support", "contact", "admin",
                    "sales", "help", "noreply", "no-reply", "webmaster",
                    "enquiries", "enquiry", "office", "mail", "team"}

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

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
        "WHERE website_url IS NOT NULL AND (email IS NULL OR email = '')"
    ).fetchall()
    return rows


_db_lock = Lock()


def save_email(conn: sqlite3.Connection, row_id: int, email: str) -> None:
    with _db_lock:
        conn.execute("UPDATE businesses SET email = ? WHERE id = ?", (email, row_id))
        conn.commit()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def _get(url: str) -> requests.Response | None:
    """Fetch URL with up to MAX_ATTEMPTS tries; return None on terminal failure."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = SESSION.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code == 404:
                log.debug("404 — %s", url)
                return None                  # don't retry 404s
            if resp.status_code in (429, 503):
                wait = 2 ** attempt
                log.warning("HTTP %s on %s, waiting %ds", resp.status_code, url, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.Timeout:
            log.debug("Timeout (attempt %d/%d) — %s", attempt, MAX_ATTEMPTS, url)
        except requests.RequestException as exc:
            log.debug("Error (attempt %d/%d) %s — %s", attempt, MAX_ATTEMPTS, url, exc)
    return None


# ---------------------------------------------------------------------------
# Email extraction
# ---------------------------------------------------------------------------

def _extract_emails_from_html(html: str) -> list[str]:
    """Return all unique emails found in HTML text (href + body)."""
    soup = BeautifulSoup(html, "html.parser")

    candidates: list[str] = []

    # mailto: links first — most reliable
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.lower().startswith("mailto:"):
            addr = href[7:].split("?")[0].strip()
            if EMAIL_RE.fullmatch(addr):
                candidates.append(addr.lower())

    # regex sweep over visible text
    for match in EMAIL_RE.finditer(soup.get_text(" ")):
        candidates.append(match.group(0).lower())

    # deduplicate, preserve order
    seen: set[str] = set()
    unique: list[str] = []
    for e in candidates:
        if e not in seen:
            seen.add(e)
            unique.append(e)
    return unique


def _is_generic(email: str) -> bool:
    prefix = email.split("@")[0].lower()
    return prefix in GENERIC_PREFIXES


def _best_email(emails: list[str]) -> str | None:
    """Return the best non-generic email; fall back to first generic if nothing else."""
    specific = [e for e in emails if not _is_generic(e)]
    if specific:
        return specific[0]
    if emails:
        return emails[0]          # only generics found — better than nothing
    return None


def _normalize_url(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def scrape_email(website_url: str) -> str | None:
    base = _normalize_url(website_url)
    pages_to_try = [base] + [urljoin(base + "/", p.lstrip("/")) for p in SUBPAGES]

    all_emails: list[str] = []

    for url in pages_to_try:
        resp = _get(url)
        if resp is None:
            continue
        emails = _extract_emails_from_html(resp.text)
        all_emails.extend(emails)

        best = _best_email(all_emails)
        if best and not _is_generic(best):
            return best          # stop early once we have a specific address

    return _best_email(all_emails)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Enrich businesses DB with emails.")
    parser.add_argument("--db",      default=DB_PATH, help=f"SQLite DB path (default: {DB_PATH}).")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Thread pool size (default: 20).")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, check_same_thread=False)
    ensure_email_column(conn)

    pending = fetch_pending(conn)
    log.info("%d URLs to process.", len(pending))

    found = skipped = 0
    counter_lock = Lock()

    def task(row_id: int, url: str) -> tuple[int, str | None]:
        email = scrape_email(url)
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
