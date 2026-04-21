"""
clean_leads.py — Quality-filter businesses.db and export two CSVs.

Hard filters (record excluded from both outputs):
  • email IS NULL
  • email domain on personal blocklist (gmail, yahoo, etc.)
  • business_name contains 'permanently closed' (case-insensitive)
  • duplicate email (keep earliest record by id)
  • website_url returns HTTP 404

Soft filter (record goes to flagged_leads.csv, not clean_leads.csv):
  • review_count < 5 or NULL  →  flag_reason = "low_review_count"

Outputs:
  clean_leads.csv   — passed every filter, reviews ≥ 5
  flagged_leads.csv — passed hard filters, reviews < 5 or unknown

Usage:
    python clean_leads.py
    python clean_leads.py --db businesses.db --workers 20 --out-dir ./output
"""

import argparse
import csv
import logging
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import phonenumbers
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH     = "businesses.db"
MAX_WORKERS = 20
URL_TIMEOUT = 5      # seconds per HEAD/GET request
URL_RETRIES = 2      # attempts before marking as dead

PERSONAL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "me.com", "mac.com", "live.com", "msn.com",
    "ymail.com", "googlemail.com", "protonmail.com", "proton.me",
}

CLOSED_PATTERN = re.compile(r"permanently\s+closed", re.IGNORECASE)

LOW_REVIEW_THRESHOLD = 5

OUTPUT_COLS = [
    "id", "business_name", "address", "city", "phone", "website_url",
    "email", "rating", "review_count", "category", "zip_code",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def load_records(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cols = {row[1] for row in conn.execute("PRAGMA table_info(businesses)")}
    if "city" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN city TEXT")
        conn.commit()
    rows = conn.execute(
        "SELECT id, business_name, address, city, phone, website_url, email, "
        "rating, review_count, category, zip_code "
        "FROM businesses "
        "WHERE email IS NOT NULL AND email != '' "
        "ORDER BY id ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ---------------------------------------------------------------------------
# Phone normalisation
# ---------------------------------------------------------------------------

def normalize_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    # Strip common noise characters before parsing
    cleaned = re.sub(r"[^\d+\-().\s]", "", raw).strip()
    try:
        parsed = phonenumbers.parse(cleaned, "US")
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(
                parsed, phonenumbers.PhoneNumberFormat.E164
            )
    except phonenumbers.NumberParseException:
        pass
    return raw  # return original if we can't parse — don't discard the lead

# ---------------------------------------------------------------------------
# URL 404 checking
# ---------------------------------------------------------------------------

_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})


def _normalize_url(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def is_404(url: str) -> bool:
    """Return True only if we are confident the URL is a 404 (dead page)."""
    url = _normalize_url(url)
    for attempt in range(1, URL_RETRIES + 1):
        try:
            # HEAD first — cheap; most servers honour it
            resp = _session.head(
                url, timeout=URL_TIMEOUT, allow_redirects=True
            )
            if resp.status_code == 404:
                return True
            if resp.status_code == 405:
                # HEAD not allowed — fall back to GET
                resp = _session.get(
                    url, timeout=URL_TIMEOUT, allow_redirects=True, stream=True
                )
                resp.close()
                return resp.status_code == 404
            # Any other status (200, 301, 403, 500, etc.) — keep the record
            return False
        except requests.Timeout:
            log.debug("Timeout (attempt %d/%d) — %s", attempt, URL_RETRIES, url)
            time.sleep(1)
        except requests.RequestException:
            # DNS failure, connection refused, SSL error, etc. — not a 404
            return False
    return False  # timed out both times — benefit of the doubt


def check_urls_parallel(records: list[dict], workers: int) -> tuple[list[dict], int]:
    """Return (surviving_records, dead_count). Removes confirmed-404 records."""
    has_url      = [r for r in records if r.get("website_url")]
    no_url       = [r for r in records if not r.get("website_url")]
    dead_ids: set[int] = set()

    log.info("Checking %d URLs for 404s with %d workers…", len(has_url), workers)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(is_404, r["website_url"]): r["id"] for r in has_url}
        done = 0
        for future in as_completed(futures):
            row_id = futures[future]
            if future.result():
                dead_ids.add(row_id)
            done += 1
            if done % 50 == 0:
                log.info("  URL check progress: %d/%d", done, len(has_url))

    alive = [r for r in has_url if r["id"] not in dead_ids]
    return alive + no_url, len(dead_ids)

# ---------------------------------------------------------------------------
# In-memory filters
# ---------------------------------------------------------------------------

def filter_personal_emails(records: list[dict]) -> tuple[list[dict], int]:
    kept = []
    for r in records:
        domain = (r["email"] or "").split("@")[-1].lower().strip()
        if domain in PERSONAL_DOMAINS:
            continue
        kept.append(r)
    removed = len(records) - len(kept)
    return kept, removed


def filter_permanently_closed(records: list[dict]) -> tuple[list[dict], int]:
    kept = [r for r in records if not CLOSED_PATTERN.search(r["business_name"] or "")]
    return kept, len(records) - len(kept)


def dedup_by_email(records: list[dict]) -> tuple[list[dict], int]:
    """Records are already sorted by id ASC — first seen wins."""
    seen: set[str] = set()
    kept = []
    for r in records:
        key = (r["email"] or "").lower().strip()
        if key in seen:
            continue
        seen.add(key)
        kept.append(r)
    return kept, len(records) - len(kept)

# ---------------------------------------------------------------------------
# Split clean vs flagged
# ---------------------------------------------------------------------------

def split_by_trust(records: list[dict]) -> tuple[list[dict], list[dict]]:
    clean, flagged = [], []
    for r in records:
        rc = r.get("review_count")
        if rc is None or rc < LOW_REVIEW_THRESHOLD:
            r["flag_reason"] = "low_review_count"
            flagged.append(r)
        else:
            clean.append(r)
    return clean, flagged

# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_csv(path: str, records: list[dict], extra_cols: list[str] | None = None) -> None:
    cols = OUTPUT_COLS + (extra_cols or [])
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    log.info("Wrote %d records → %s", len(records), path)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and filter leads from SQLite DB.")
    parser.add_argument("--db",       default=DB_PATH, help=f"SQLite path (default: {DB_PATH})")
    parser.add_argument("--workers",  type=int, default=MAX_WORKERS, help="URL-check thread pool size")
    parser.add_argument("--out-dir",  default=".",   help="Directory for output CSVs (default: .)")
    parser.add_argument("--skip-url-check", action="store_true",
                        help="Skip the 404 URL check (faster, less thorough)")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. Load ────────────────────────────────────────────────────────────
    log.info("Loading records from %s…", args.db)
    records = load_records(args.db)
    log.info("  Loaded: %d records with email", len(records))
    start_count = len(records)

    # ── 2. Personal email domains ──────────────────────────────────────────
    records, n = filter_personal_emails(records)
    log.info("  After domain filter:      %d  (−%d personal domains)", len(records), n)

    # ── 3. Permanently closed ─────────────────────────────────────────────
    records, n = filter_permanently_closed(records)
    log.info("  After closed filter:      %d  (−%d closed businesses)", len(records), n)

    # ── 4. Deduplicate emails ─────────────────────────────────────────────
    records, n = dedup_by_email(records)
    log.info("  After email dedup:        %d  (−%d duplicate emails)", len(records), n)

    # ── 5. 404 URL check ──────────────────────────────────────────────────
    if not args.skip_url_check:
        records, n = check_urls_parallel(records, args.workers)
        log.info("  After 404 URL check:      %d  (−%d dead URLs)", len(records), n)
    else:
        log.info("  404 URL check skipped.")

    # ── 6. Normalise phone numbers ────────────────────────────────────────
    for r in records:
        r["phone"] = normalize_phone(r["phone"])

    # ── 7. Split clean vs flagged ─────────────────────────────────────────
    clean, flagged = split_by_trust(records)
    log.info("  Clean leads (reviews ≥ %d): %d", LOW_REVIEW_THRESHOLD, len(clean))
    log.info("  Flagged leads (low trust):  %d", len(flagged))

    # ── 8. Write CSVs ─────────────────────────────────────────────────────
    write_csv(str(out_dir / "clean_leads.csv"),   clean)
    write_csv(str(out_dir / "flagged_leads.csv"),  flagged, extra_cols=["flag_reason"])

    # ── Summary ───────────────────────────────────────────────────────────
    total_out = len(clean) + len(flagged)
    excluded  = start_count - total_out
    log.info("")
    log.info("═══ Summary ═══════════════════════════════")
    log.info("  Input records (with email): %d", start_count)
    log.info("  Excluded (hard filters):    %d", excluded)
    log.info("  clean_leads.csv:            %d", len(clean))
    log.info("  flagged_leads.csv:          %d", len(flagged))
    log.info("  Total output:               %d", total_out)
    log.info("═══════════════════════════════════════════")


if __name__ == "__main__":
    main()
