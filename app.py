"""
Flask dashboard for the Maps Data scraper pipeline.
Run: python app.py  →  http://localhost:5000
"""

import csv
import io
import os
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from flask import Flask, Response, jsonify, render_template, request

from state_zips import STATE_NAMES, STATE_ZIPS

app = Flask(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
DB_PATH     = os.environ.get("DB_PATH", "businesses.db")
API_HOST    = "maps-data.p.rapidapi.com"
API_BASE    = f"https://{API_HOST}"
PAGE_SIZE   = 20
MAX_PAGES   = 10
MAX_RETRIES = 6
JOB_WORKERS = 5   # concurrent zips per job

# ── In-memory job store ───────────────────────────────────────────────────────
_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()

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
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_phone_url
        ON businesses (phone, website_url)
        WHERE phone IS NOT NULL OR website_url IS NOT NULL
    """)
    # Migrate existing DBs that lack the email column
    cols = {row[1] for row in conn.execute("PRAGMA table_info(businesses)")}
    if "email" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN email TEXT")
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
                "(business_name, address, phone, website_url, rating, review_count, category, zip_code) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (row["business_name"], row["address"], phone, url,
                 row["rating"], row["review_count"], row["category"], row["zip_code"]),
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

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
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
        with _jobs_lock:
            job["status"] = "completed"
    except Exception as exc:
        with _jobs_lock:
            job["status"] = "failed"
            job["error"]  = str(exc)
    finally:
        with _jobs_lock:
            job["completed_at"] = datetime.utcnow().isoformat()
        conn.close()


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


@app.route("/api/jobs", methods=["POST"])
def start_job():
    data        = request.get_json(force=True)
    keyword     = (data.get("keyword")     or "").strip()
    api_key     = (data.get("api_key")     or "").strip()
    state       = (data.get("state")       or "").strip().upper()
    custom_zips = (data.get("custom_zips") or "").strip()

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
    }

    with _jobs_lock:
        _jobs[job["id"]] = job

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
        conn.close()
        return jsonify({
            "total":      total,
            "with_email": with_email,
            "categories": [{"name": r["category"], "count": r["cnt"]} for r in categories],
        })
    except Exception:
        return jsonify({"total": 0, "with_email": 0, "categories": []})


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

    where, params = _build_where(min_rating, min_reviews, has_email, category)
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

    where, params = _build_where(min_rating, min_reviews, has_email, category)

    conn = get_conn()
    rows = conn.execute(
        f"SELECT business_name, address, phone, website_url, email, "
        f"rating, review_count, category, zip_code FROM businesses {where} ORDER BY id DESC",
        params,
    ).fetchall()
    conn.close()

    def generate():
        buf = io.StringIO()
        w   = csv.writer(buf)
        w.writerow(["business_name","address","phone","website_url","email",
                    "rating","review_count","category","zip_code"])
        for row in rows:
            w.writerow(list(row))
        yield buf.getvalue()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{timestamp}.csv"},
    )


def _build_where(min_rating: float, min_reviews: int, has_email: bool, category: str):
    clauses, params = [], []
    if min_rating > 0:
        clauses.append("rating >= ?");     params.append(min_rating)
    if min_reviews > 0:
        clauses.append("review_count >= ?"); params.append(min_reviews)
    if has_email:
        clauses.append("email IS NOT NULL AND email != ''")
    if category:
        clauses.append("category = ?");    params.append(category)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    app.run(host="127.0.0.1", port=5000, debug=False)
