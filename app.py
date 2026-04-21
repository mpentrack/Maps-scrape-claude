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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from flask import Flask, Response, jsonify, render_template, request

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
ENRICH_WORKERS = 12
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE)
PERSONAL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "me.com", "mac.com", "live.com", "msn.com", "protonmail.com", "proton.me",
}
SUBPAGES = ["/contact", "/contact-us", "/about", "/about-us"]

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
    for col, ddl in {
        "email": "ALTER TABLE businesses ADD COLUMN email TEXT",
        "pipeline_stage": "ALTER TABLE businesses ADD COLUMN pipeline_stage TEXT DEFAULT 'scraped'",
        "stage_reason": "ALTER TABLE businesses ADD COLUMN stage_reason TEXT",
        "enriched_at": "ALTER TABLE businesses ADD COLUMN enriched_at TEXT",
        "cleaned_at": "ALTER TABLE businesses ADD COLUMN cleaned_at TEXT",
    }.items():
        if col not in cols:
            conn.execute(ddl)
    conn.execute("UPDATE businesses SET pipeline_stage='scraped' WHERE pipeline_stage IS NULL OR pipeline_stage=''")
    conn.commit()
    conn.close()


# ── Scraping helpers (self-contained so app.py has no import coupling) ────────

def _api_get(url: str, params: dict, api_key: str):
    headers = {"x-rapidapi-host": API_HOST, "x-rapidapi-key": api_key}
    delay = 1.0
    for _ in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                time.sleep(delay)
                delay *= 2
                continue
            return None
        except requests.RequestException:
            time.sleep(delay)
            delay *= 2
    return None


def _parse(item: dict, zip_code: str) -> dict:
    types = item.get("types")
    category = types[0] if isinstance(types, list) and types else item.get("type") or item.get("category")
    return {
        "business_name": item.get("name") or item.get("title"),
        "address":       item.get("full_address") or item.get("address"),
        "phone":         item.get("phone_number") or item.get("phone"),
        "website_url":   item.get("website"),
        "rating":        item.get("rating"),
        "review_count":  item.get("reviews") or item.get("review_count"),
        "category":      category,
        "zip_code":      zip_code,
    }


def _insert(conn: sqlite3.Connection, lock: threading.Lock, row: dict) -> bool:
    phone = row.get("phone") or None
    url   = row.get("website_url") or None
    if phone is None and url is None:
        return False
    with lock:
        try:
            conn.execute(
                "INSERT INTO businesses "
                "(business_name, address, phone, website_url, rating, review_count, category, zip_code, pipeline_stage) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (row["business_name"], row["address"], phone, url,
                 row["rating"], row["review_count"], row["category"], row["zip_code"], "scraped"),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def _scrape_zip(zip_code: str, keyword: str, api_key: str,
                conn: sqlite3.Connection, lock: threading.Lock) -> tuple[int, int]:
    inserted = skipped = 0
    for page in range(1, MAX_PAGES + 1):
        data = _api_get(
            f"{API_BASE}/searchmaps.php",
            {"query": keyword, "zipcode": zip_code, "country": "us",
             "limit": PAGE_SIZE, "offset": (page - 1) * PAGE_SIZE, "language": "en"},
            api_key,
        )
        if not data:
            break
        results = data.get("data") or data.get("results") or data.get("businesses") or []
        if not results:
            break
        for item in results:
            if _insert(conn, lock, _parse(item, zip_code)):
                inserted += 1
            else:
                skipped += 1
        if len(results) < PAGE_SIZE:
            break
    return inserted, skipped


def _run_job(job_id: str, api_key: str) -> None:
    with _jobs_lock:
        job = _jobs[job_id]
        job["status"] = "running"
    _job_event(job_id, "info", "scrape", "Job started.")

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    lock = threading.Lock()

    try:
        with ThreadPoolExecutor(max_workers=JOB_WORKERS) as pool:
            futures = {
                pool.submit(_scrape_zip, z, job["keyword"], api_key, conn, lock): z
                for z in job["zip_codes"]
            }
            for future in as_completed(futures):
                ins, skp = future.result()
                with _jobs_lock:
                    job["inserted"]   += ins
                    job["duplicates"] += skp
                    job["processed"]  += 1
                _job_event(job_id, "info", "scrape", f"Processed zip {job['processed']}/{job['total']} (+{ins} new, {skp} dup).")
        if job.get("run_mode") == "full_pipeline":
            _job_event(job_id, "info", "enrich", "Starting enrichment stage.")
            enriched = _enrich_stage_rows(conn, "scraped", limit=None)
            _job_event(
                job_id, "info", "enrich",
                f"Enrichment complete: checked={enriched['checked']}, enriched={enriched['enriched']}, no_email={enriched['no_email']}.",
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


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def _extract_email_candidates(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    found = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.lower().startswith("mailto:"):
            addr = href[7:].split("?")[0].strip().lower()
            if EMAIL_RE.fullmatch(addr):
                found.append(addr)
    for m in EMAIL_RE.finditer(soup.get_text(" ")):
        found.append(m.group(0).lower())
    dedup = []
    seen = set()
    for email in found:
        if email not in seen:
            seen.add(email)
            dedup.append(email)
    return dedup


def _pick_best_email(emails: list[str]) -> str | None:
    if not emails:
        return None
    non_generic = [e for e in emails if e.split("@")[0] not in {"info", "hello", "contact", "support", "sales"}]
    return non_generic[0] if non_generic else emails[0]


def _scrape_site_email(website_url: str) -> str | None:
    base = _normalize_url(website_url)
    if not base:
        return None
    urls = [base] + [urljoin(base + "/", p.lstrip("/")) for p in SUBPAGES]
    all_emails = []
    for url in urls:
        try:
            resp = requests.get(url, timeout=6, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code >= 400:
                continue
            all_emails.extend(_extract_email_candidates(resp.text))
            best = _pick_best_email(all_emails)
            if best and best.split("@")[0] not in {"info", "hello", "contact", "support", "sales"}:
                return best
        except requests.RequestException:
            continue
    return _pick_best_email(all_emails)


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
        return row_id, _scrape_site_email(website_url)

    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        futures = [pool.submit(task, row) for row in rows]
        for fut in as_completed(futures):
            row_id, email = fut.result()
            now = datetime.utcnow().isoformat()
            if email:
                conn.execute(
                    "UPDATE businesses SET email=?, pipeline_stage='enriched', stage_reason=NULL, enriched_at=? WHERE id=?",
                    (email, now, row_id),
                )
                with result_lock:
                    enriched += 1
            else:
                conn.execute(
                    "UPDATE businesses SET pipeline_stage='enrich_failed', stage_reason='no_email_found', enriched_at=? WHERE id=?",
                    (now, row_id),
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
    if email.split("@")[-1] in PERSONAL_DOMAINS:
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
        f"SELECT business_name, address, phone, website_url, email, "
        f"rating, review_count, category, zip_code, pipeline_stage, stage_reason FROM businesses {where} ORDER BY id DESC",
        params,
    ).fetchall()
    conn.close()

    def generate():
        buf = io.StringIO()
        w   = csv.writer(buf)
        w.writerow(["business_name","address","phone","website_url","email",
                    "rating","review_count","category","zip_code","pipeline_stage","stage_reason"])
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
        "scraped", "enriched", "enrich_failed", "clean", "flagged", "clean_failed",
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
