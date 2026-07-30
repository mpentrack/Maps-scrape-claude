#!/usr/bin/env python3
"""Drive the deployed pipeline API end to end, without babysitting the UI.

The browser UI starts a bulk stage action and then depends on someone watching
it: a proxy timeout mid-poll leaves the page showing `Unexpected token 'u'`
(an HTML gateway error parsed as JSON) and the run looks dead even though the
job worker is still grinding through rows. This driver talks to the same API,
retries network errors, treats a non-JSON body as a transient proxy hiccup, and
keeps polling until the job actually reaches a terminal state.

Usage:
    export APP_URL=https://your-app.example.com
    python run_pipeline.py status
    python run_pipeline.py enrich <stage>
    python run_pipeline.py clean
    python run_pipeline.py export <path>
    python run_pipeline.py all [path]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime

try:
    import requests
except ImportError:  # pragma: no cover - dependency is declared in requirements.txt
    sys.exit("run_pipeline.py needs the 'requests' package: pip install requests")


# Poll cadence for a running job. Bulk runs take hours, so this is a slow,
# cheap heartbeat rather than a tight loop.
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))

# One-shot calls (stats, advance, export) retry this many times before giving up.
HTTP_ATTEMPTS = int(os.environ.get("HTTP_ATTEMPTS", "5"))
BACKOFF_START = 2.0
BACKOFF_MAX = 60.0

# A poll that fails is never fatal on its own — the platform proxy times out
# long before the job does. Only a long unbroken run of failures means the app
# is genuinely gone.
POLL_FAILURE_LIMIT = int(os.environ.get("POLL_FAILURE_LIMIT", "40"))

# (connect, read) timeouts. Export of a large table can be slow to first byte.
TIMEOUT = (15, 180)

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}

# Stage order used by `all`, matching the pipeline's own progression.
ALL_STEPS = [
    ("enrich", "enrich_failed"),
    ("enrich", "geo_rejected"),
    ("enrich", "archived"),
    ("clean", "enriched"),
]


class TransientError(Exception):
    """A failure worth retrying: connection reset, 5xx, proxy timeout page."""


class PipelineError(Exception):
    """A failure the caller should surface and exit on."""


# --------------------------------------------------------------------------- #
# HTTP plumbing
# --------------------------------------------------------------------------- #

def base_url() -> str:
    url = (os.environ.get("APP_URL") or "").strip().rstrip("/")
    if not url:
        raise PipelineError(
            "APP_URL is not set. Point it at the deployed app, e.g.\n"
            "    export APP_URL=https://your-app.example.com"
        )
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


_session = requests.Session()


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _raw_request(method: str, path: str, *, params=None, json_body=None, stream=False):
    """Single HTTP attempt. Raises TransientError for anything retryable."""
    url = f"{base_url()}{path}"
    try:
        resp = _session.request(
            method, url, params=params, json=json_body, stream=stream, timeout=TIMEOUT
        )
    except requests.RequestException as exc:
        raise TransientError(f"{type(exc).__name__}: {exc}") from exc

    # 5xx and friends are the gateway/app being briefly unavailable, not a bad
    # request — the same class of failure as a dropped connection.
    if resp.status_code >= 500 or resp.status_code in (408, 429):
        resp.close()
        raise TransientError(f"HTTP {resp.status_code} from {path}")
    return resp


def _parse_json(resp):
    """Parse a JSON body, treating a non-JSON body as transient.

    A gateway timeout hands back an HTML error page. The UI feeds that straight
    into JSON.parse and dies with `Unexpected token 'u'`; here it is just a
    retryable blip, because the job behind it is still running.
    """
    try:
        return resp.json()
    except (ValueError, json.JSONDecodeError):
        snippet = " ".join((resp.text or "")[:120].split())
        raise TransientError(
            f"non-JSON response (HTTP {resp.status_code}, "
            f"{resp.headers.get('Content-Type', 'unknown type')}): {snippet or '<empty body>'}"
        )


def request_json(method: str, path: str, *, params=None, json_body=None,
                 attempts: int = HTTP_ATTEMPTS, expect_missing: bool = False):
    """Request a JSON endpoint, retrying transient failures with backoff.

    Returns None for a 404 when `expect_missing` is set, so a caller can probe
    for an endpoint that may not exist on the deployed build.
    """
    delay = BACKOFF_START
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = _raw_request(method, path, params=params, json_body=json_body)
            if expect_missing and resp.status_code == 404:
                return None
            data = _parse_json(resp)
        except TransientError as exc:
            last = exc
            if attempt < attempts:
                _log(f"  retry {attempt}/{attempts - 1} on {path} — {exc}; waiting {delay:.0f}s")
                time.sleep(delay)
                delay = min(delay * 2, BACKOFF_MAX)
            continue

        if resp.status_code >= 400:
            msg = data.get("error") if isinstance(data, dict) else None
            raise PipelineError(f"HTTP {resp.status_code} from {path}: {msg or data}")
        return data

    if attempts == 1:
        raise PipelineError(f"{path}: {last}")
    raise PipelineError(f"{path} failed after {attempts} attempts — {last}")


# --------------------------------------------------------------------------- #
# API helpers
# --------------------------------------------------------------------------- #

def fetch_stage_counts() -> dict[str, int]:
    stats = request_json("GET", "/api/stats")
    return {s["name"]: s["count"] for s in stats.get("stages", [])}


def print_stage_counts(counts: dict[str, int], title: str = "stage counts") -> None:
    print(f"\n{title}:", flush=True)
    if not counts:
        print("  (no rows)", flush=True)
        return
    width = max(len(name) for name in counts)
    for name, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {name:<{width}}  {count:>9,}", flush=True)
    print(f"  {'TOTAL':<{width}}  {sum(counts.values()):>9,}", flush=True)


def print_stage_diff(before: dict[str, int], after: dict[str, int], title: str) -> None:
    print(f"\n{title}:", flush=True)
    names = sorted(set(before) | set(after))
    changed = [n for n in names if before.get(n, 0) != after.get(n, 0)]
    if not changed:
        print("  (no stage counts changed)", flush=True)
        return
    width = max(len(n) for n in changed)
    for name in changed:
        b, a = before.get(name, 0), after.get(name, 0)
        print(f"  {name:<{width}}  {b:>9,} -> {a:>9,}  ({a - b:+,})", flush=True)


def _job_counters(job: dict) -> str:
    """Flatten whichever counter dict this job type reports."""
    stats = job.get("enriched") if isinstance(job.get("enriched"), dict) else None
    if stats is None and isinstance(job.get("cleaned"), dict):
        stats = job["cleaned"]
    if not stats:
        return ""
    return " ".join(f"{k}={v}" for k, v in stats.items() if k != "cancelled")


def get_job(job_id: str, *, attempts: int = HTTP_ATTEMPTS) -> dict:
    """Fetch one job.

    Prefers GET /api/jobs/<id>; falls back to filtering the list endpoint, which
    is all the currently deployed build exposes.
    """
    job = request_json("GET", f"/api/jobs/{job_id}", attempts=attempts, expect_missing=True)
    if isinstance(job, dict) and job.get("id"):
        return job

    jobs = request_json("GET", "/api/jobs", attempts=attempts)
    for entry in jobs or []:
        if entry.get("id") == job_id:
            return entry
    raise PipelineError(f"job {job_id} not found on the server")


def print_job_events(job_id: str, limit: int = 8) -> None:
    try:
        payload = request_json("GET", f"/api/jobs/{job_id}/events",
                               params={"limit": limit}, attempts=2)
    except PipelineError as exc:
        _log(f"  (could not read job events: {exc})")
        return
    events = (payload or {}).get("events") or []
    if not events:
        return
    print(f"  last {len(events)} event(s):", flush=True)
    for ev in events:
        print(f"    [{ev.get('level', 'info')}] {ev.get('stage', '')}: {ev.get('message', '')}",
              flush=True)


def start_bulk(action: str, from_stage: str) -> dict:
    """Kick off an uncapped bulk action, which the server runs as a job."""
    _log(f"POST /api/pipeline/advance  action={action} from_stage={from_stage} limit=none")
    return request_json(
        "POST", "/api/pipeline/advance",
        json_body={"from_stage": from_stage, "action": action, "limit": None},
    )


def poll_job(job_id: str) -> dict:
    """Poll until the job reaches a terminal state. Raises on failure."""
    failures = 0
    last_seen = ""
    while True:
        try:
            job = get_job(job_id, attempts=1)
            failures = 0
        except (TransientError, PipelineError) as exc:
            failures += 1
            if failures >= POLL_FAILURE_LIMIT:
                raise PipelineError(
                    f"job {job_id}: {failures} consecutive failed polls "
                    f"(~{failures * POLL_SECONDS // 60} min) — last error: {exc}"
                )
            _log(f"  poll failed ({failures}/{POLL_FAILURE_LIMIT}), job likely still running — {exc}")
            time.sleep(POLL_SECONDS)
            continue

        status = job.get("status", "unknown")
        processed, total = job.get("processed") or 0, job.get("total") or 0
        pct = f" {processed / total * 100:5.1f}%" if total else ""
        counters = _job_counters(job)
        line = f"  job {job_id} {status:<9} {processed:>7,}/{total:<7,}{pct}"
        if counters:
            line += f"  {counters}"
        # Only reprint when something moved, so an idle queue stays quiet.
        if line != last_seen or status in TERMINAL_STATUSES:
            _log(line)
            last_seen = line

        if status in TERMINAL_STATUSES:
            print_job_events(job_id)
            if status == "failed":
                raise PipelineError(f"job {job_id} failed: {job.get('error') or 'unknown error'}")
            if status == "cancelled":
                raise PipelineError(f"job {job_id} was cancelled before finishing")
            return job

        time.sleep(POLL_SECONDS)


def run_bulk_step(action: str, from_stage: str) -> dict | None:
    """Start a bulk action and see it through. Returns the finished job, if any."""
    resp = start_bulk(action, from_stage)

    # A capped batch runs inline; an uncapped one always comes back as a job.
    if resp.get("mode") == "inline":
        _log(f"  ran inline: {resp.get('result')}")
        return None

    job_id = resp.get("job_id")
    if not job_id:
        raise PipelineError(f"no job_id in advance response: {resp}")
    _log(f"  queued job {job_id} — {resp.get('total', '?')} candidate row(s)")

    if not resp.get("total"):
        _log("  nothing to do for this stage (0 candidates), still waiting for the job to close out")
    return poll_job(job_id)


def download_export(path: str) -> int:
    """Save the has_email export to `path` and return its data row count."""
    _log(f"GET /api/export?has_email=true -> {path}")
    delay = BACKOFF_START
    last: Exception | None = None
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        try:
            resp = _raw_request("GET", "/api/export",
                                params={"has_email": "true"}, stream=True)
            if resp.status_code >= 400:
                raise PipelineError(f"HTTP {resp.status_code} from /api/export: {resp.text[:200]}")
            parent = os.path.dirname(os.path.abspath(path))
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)
            break
        except (TransientError, requests.RequestException) as exc:
            last = exc
            if attempt < HTTP_ATTEMPTS:
                _log(f"  retry {attempt}/{HTTP_ATTEMPTS - 1} on /api/export — {exc}; waiting {delay:.0f}s")
                time.sleep(delay)
                delay = min(delay * 2, BACKOFF_MAX)
            continue
    else:
        raise PipelineError(f"/api/export failed after {HTTP_ATTEMPTS} attempts — {last}")

    # Count with the csv module so quoted newlines inside a field don't inflate it.
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        rows = sum(1 for _ in csv.reader(fh))
    data_rows = max(rows - 1, 0)  # drop the header
    size_kb = os.path.getsize(path) / 1024
    _log(f"  saved {path} — {data_rows:,} row(s), {size_kb:,.1f} KB")
    return data_rows


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #

def cmd_status(_args) -> int:
    print_stage_counts(fetch_stage_counts())
    return 0


def cmd_enrich(args) -> int:
    before = fetch_stage_counts()
    print_stage_counts(before, f"stage counts before enrich '{args.stage}'")
    run_bulk_step("enrich", args.stage)
    after = fetch_stage_counts()
    print_stage_diff(before, after, f"enrich '{args.stage}' moved")
    print_stage_counts(after, "stage counts after")
    return 0


def cmd_clean(_args) -> int:
    before = fetch_stage_counts()
    print_stage_counts(before, "stage counts before clean 'enriched'")
    run_bulk_step("clean", "enriched")
    after = fetch_stage_counts()
    print_stage_diff(before, after, "clean 'enriched' moved")
    print_stage_counts(after, "stage counts after")
    return 0


def cmd_export(args) -> int:
    download_export(args.path)
    return 0


def cmd_all(args) -> int:
    path = args.path or f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    start = time.time()

    counts = fetch_stage_counts()
    print_stage_counts(counts, "stage counts at start")
    summary: list[tuple[str, str]] = []

    for index, (action, stage) in enumerate(ALL_STEPS, start=1):
        print(f"\n{'=' * 68}\nstep {index}/{len(ALL_STEPS) + 1}: {action} '{stage}'\n{'=' * 68}",
              flush=True)
        before = counts
        run_bulk_step(action, stage)
        counts = fetch_stage_counts()
        print_stage_diff(before, counts, f"{action} '{stage}' moved")
        print_stage_counts(counts, "stage counts now")

        moved = {n: counts.get(n, 0) - before.get(n, 0)
                 for n in set(before) | set(counts)
                 if counts.get(n, 0) != before.get(n, 0)}
        summary.append((
            f"{action} {stage}",
            ", ".join(f"{n} {d:+,}" for n, d in sorted(moved.items())) or "no change",
        ))

    print(f"\n{'=' * 68}\nstep {len(ALL_STEPS) + 1}/{len(ALL_STEPS) + 1}: export\n{'=' * 68}",
          flush=True)
    rows = download_export(path)
    summary.append(("export", f"{rows:,} row(s) -> {path}"))

    print(f"\n{'=' * 68}\nrun summary ({time.time() - start:,.0f}s)\n{'=' * 68}", flush=True)
    width = max(len(name) for name, _ in summary)
    for name, detail in summary:
        print(f"  {name:<{width}}  {detail}", flush=True)
    print_stage_counts(counts, "final stage counts")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="run_pipeline.py",
        description="Drive the deployed pipeline API end to end (base URL from $APP_URL).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="print every pipeline stage with its count").set_defaults(
        func=cmd_status)

    p_enrich = sub.add_parser("enrich", help="bulk-enrich a stage and follow the job")
    p_enrich.add_argument("stage", help="source stage, e.g. enrich_failed")
    p_enrich.set_defaults(func=cmd_enrich)

    sub.add_parser("clean", help="bulk-clean the 'enriched' stage and follow the job").set_defaults(
        func=cmd_clean)

    p_export = sub.add_parser("export", help="download the has_email CSV export")
    p_export.add_argument("path", help="where to write the CSV")
    p_export.set_defaults(func=cmd_export)

    p_all = sub.add_parser(
        "all", help="enrich enrich_failed, geo_rejected, archived; clean enriched; export")
    p_all.add_argument("path", nargs="?", default=None,
                       help="export path (default: leads_<timestamp>.csv)")
    p_all.set_defaults(func=cmd_all)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except PipelineError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print("\ninterrupted — the server-side job keeps running; re-poll with `status`",
              file=sys.stderr, flush=True)
        return 130


if __name__ == "__main__":
    sys.exit(main())
