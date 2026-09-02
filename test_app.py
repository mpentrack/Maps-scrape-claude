import os
import queue
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import app
import email_extract
from email_extract import EmailScrapeResult


class StageBatchingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = app.DB_PATH
        app.DB_PATH = str(Path(self.temp_dir.name) / "businesses.db")
        app.init_db()

    def tearDown(self):
        app.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_enrichment_reads_one_bounded_batch_at_a_time(self):
        conn = app.get_conn()
        conn.executemany(
            "INSERT INTO businesses (business_name, website_url, pipeline_stage) VALUES (?, ?, 'scraped')",
            [(f"Business {i}", f"https://example{i}.com") for i in range(105)],
        )
        conn.commit()
        statements = []
        conn.set_trace_callback(statements.append)
        progress = []

        def fake_scrape(_website_url, _session, *, allow_generic_fallback):
            self.assertFalse(allow_generic_fallback)
            return EmailScrapeResult("owner@example.com", None)

        with (
            mock.patch.object(app, "PROGRESS_CHUNK_ROWS", 25),
            mock.patch.object(app, "ENRICH_WORKERS", 4),
            mock.patch.object(app, "scrape_email_for_website", side_effect=fake_scrape),
        ):
            result = app._enrich_stage_rows(
                conn,
                "scraped",
                None,
                progress=lambda done, total, _stats: progress.append((done, total)),
            )

        batch_selects = [
            sql for sql in statements
            if sql.startswith("SELECT id, website_url FROM businesses")
        ]
        self.assertEqual(result["checked"], 105)
        self.assertEqual(result["enriched"], 105)
        self.assertEqual(progress, [(25, 105), (50, 105), (75, 105), (100, 105), (105, 105)])
        self.assertEqual(len(batch_selects), 5)
        self.assertTrue(all("LIMIT" in sql for sql in batch_selects))
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM businesses WHERE pipeline_stage='enriched'").fetchone()[0],
            105,
        )
        conn.close()

    def test_cleaning_selects_only_needed_columns_in_bounded_batches(self):
        conn = app.get_conn()
        conn.executemany(
            "INSERT INTO businesses (business_name, email, pipeline_stage) VALUES (?, ?, 'enriched')",
            [(f"Business {i}", f"owner{i}@example.com") for i in range(103)],
        )
        conn.commit()
        statements = []
        conn.set_trace_callback(statements.append)
        progress = []

        with (
            mock.patch.object(app, "PROGRESS_CHUNK_ROWS", 25),
            mock.patch.object(app, "_warm_mx_cache"),
            mock.patch.object(app, "has_mx", return_value=True),
        ):
            result = app._clean_stage_rows(
                conn,
                "enriched",
                None,
                progress=lambda done, total, _stats: progress.append((done, total)),
            )

        batch_selects = [
            sql for sql in statements
            if sql.startswith("SELECT id, business_name, email FROM businesses")
        ]
        self.assertEqual(result["checked"], 103)
        self.assertEqual(result["clean"], 103)
        self.assertEqual(progress, [(25, 103), (50, 103), (75, 103), (100, 103), (103, 103)])
        self.assertEqual(len(batch_selects), 5)
        self.assertTrue(all("LIMIT" in sql for sql in batch_selects))
        self.assertFalse(any(sql.startswith("SELECT * FROM businesses") for sql in statements))
        conn.close()


class QueueBackpressureTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = app.DB_PATH
        app.DB_PATH = str(Path(self.temp_dir.name) / "businesses.db")
        app.init_db()
        self.original_queue = app._job_queue
        self.original_max = app.MAX_QUEUED_JOBS
        app._job_queue = queue.Queue(maxsize=1)
        app.MAX_QUEUED_JOBS = 1
        with app._jobs_lock:
            app._jobs.clear()
        self.client = app.app.test_client()

    def tearDown(self):
        with app._jobs_lock:
            app._jobs.clear()
        app._job_queue = self.original_queue
        app.MAX_QUEUED_JOBS = self.original_max
        app.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_full_queue_returns_429_without_retaining_rejected_job(self):
        payload = {
            "keyword": "plumber",
            "api_key": "test-key",
            "custom_zips": "06101",
            "run_mode": "scrape_only",
        }
        first = self.client.post("/api/jobs", json=payload)
        second = self.client.post("/api/jobs", json=payload)

        self.assertEqual(first.status_code, 202)
        self.assertEqual(second.status_code, 429)
        self.assertEqual(second.headers["Retry-After"], "30")
        self.assertIn("queue is full", second.get_json()["error"].lower())
        with app._jobs_lock:
            self.assertEqual(len(app._jobs), 1)

    def test_invalid_zip_is_rejected_before_enqueue(self):
        response = self.client.post("/api/jobs", json={
            "keyword": "plumber",
            "api_key": "test-key",
            "custom_zips": "not-a-zip",
        })
        self.assertEqual(response.status_code, 400)
        self.assertTrue(app._job_queue.empty())


class ResponseMemoryLimitTests(unittest.TestCase):
    def test_env_int_enforces_hard_maximum(self):
        with mock.patch.dict(os.environ, {"TEST_WORKERS": "99"}):
            self.assertEqual(
                email_extract.env_int("TEST_WORKERS", 3, maximum=4),
                4,
            )

    def test_page_contacts_share_one_html_parse(self):
        html = """
        <html><head>
          <script type="application/ld+json">
            {"address": {"addressLocality": "Hartford"}}
          </script>
        </head><body><a href="mailto:owner@example.com">Email</a></body></html>
        """
        real_parser = email_extract.BeautifulSoup
        with mock.patch.object(email_extract, "BeautifulSoup", wraps=real_parser) as parser:
            emails, city = email_extract.extract_page_contacts(html)

        self.assertEqual(parser.call_count, 1)
        self.assertEqual(emails, ["owner@example.com"])
        self.assertEqual(city, "Hartford")

    def test_http_body_is_streamed_and_truncated_at_configured_limit(self):
        class FakeResponse:
            status_code = 200
            headers = {"Content-Type": "text/html; charset=utf-8"}
            encoding = "utf-8"
            closed = False

            def raise_for_status(self):
                return None

            def iter_content(self, chunk_size):
                self.chunk_size = chunk_size
                yield b"abcdefgh"
                yield b"ijklmnop"

            def close(self):
                self.closed = True

        response = FakeResponse()

        class FakeSession:
            def get(self, _url, **kwargs):
                self.kwargs = kwargs
                return response

        session = FakeSession()
        with mock.patch.object(email_extract, "MAX_RESPONSE_BYTES", 10):
            text = email_extract.http_get_text(session, "https://example.com")

        self.assertEqual(text, "abcdefghij")
        self.assertTrue(session.kwargs["stream"])
        self.assertTrue(response.closed)

    def test_whois_fallback_is_disabled_by_default_for_bounded_runtime(self):
        session = mock.Mock()
        with (
            mock.patch.object(email_extract, "ENABLE_WHOIS_FALLBACK", False),
            mock.patch.object(email_extract, "http_get_text", return_value="<html><body>No email</body></html>"),
            mock.patch.object(email_extract, "whois_email_for_domain", side_effect=AssertionError("WHOIS ran")),
        ):
            result = email_extract.scrape_email_for_website(
                "https://example.com",
                session,
                allow_generic_fallback=False,
            )
        self.assertIsNone(result.email)


class PersistentJobTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = app.DB_PATH
        self.original_queue = app._job_queue
        app.DB_PATH = str(Path(self.temp_dir.name) / "businesses.db")
        app._job_queue = queue.Queue(maxsize=5)
        app.init_db()
        with app._jobs_lock:
            app._jobs.clear()

    def tearDown(self):
        with app._jobs_lock:
            app._jobs.clear()
        app._job_queue = self.original_queue
        app.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_restart_preserves_job_and_resume_skips_completed_zips(self):
        job = {
            "id": "persist1",
            "type": app.JOB_TYPE_SCRAPE,
            "status": "running",
            "keyword": "plumber",
            "vertical": "plumbing",
            "state": "CT",
            "zip_codes": ["06101", "06103"],
            "completed_zips": ["06101"],
            "total": 2,
            "processed": 1,
            "inserted": 5,
            "duplicates": 2,
            "geo_rejected": 0,
            "no_website_prospect": 0,
            "run_mode": "scrape_only",
            "queued_at": "2026-09-01T12:00:00",
            "started_at": "2026-09-01T12:00:00",
            "completed_at": None,
            "error": None,
            "events": [],
        }
        with app._jobs_lock:
            app._jobs[job["id"]] = job
        app._persist_job(job["id"])
        with app._jobs_lock:
            app._jobs.clear()

        app._restore_jobs_from_db()
        with app._jobs_lock:
            restored = dict(app._jobs[job["id"]])
        self.assertEqual(restored["status"], "interrupted")
        self.assertEqual(restored["completed_zips"], ["06101"])

        response = app.app.test_client().post(
            f"/api/jobs/{job['id']}/resume",
            json={"api_key": "test-key"},
        )
        self.assertEqual(response.status_code, 202)
        self.assertEqual(app._job_queue.get_nowait(), (job["id"], "test-key"))
        with app._jobs_lock:
            resumed = dict(app._jobs[job["id"]])
        self.assertEqual(resumed["status"], "queued")
        self.assertEqual(resumed["pending_zips"], ["06103"])
        self.assertEqual(resumed["processed"], 1)

    def test_maps_api_retries_are_bounded(self):
        with (
            mock.patch.object(app.requests, "get", side_effect=app.requests.Timeout) as get,
            mock.patch.object(app.time, "sleep"),
            mock.patch.object(app, "MAX_RETRIES", 3),
            mock.patch.object(app, "MAPS_REQUEST_TIMEOUT", 15),
        ):
            result = app._api_get("https://example.test", {}, "key")
        self.assertIsNone(result)
        self.assertEqual(get.call_count, 3)
        self.assertTrue(all(call.kwargs["timeout"] == 15 for call in get.call_args_list))

    def test_bulk_completion_timestamp_survives_restart(self):
        job = {
            "id": "bulk1",
            "type": app.JOB_TYPE_BULK_CLEAN,
            "status": "queued",
            "keyword": "clean · missing_stage",
            "from_stage": "missing_stage",
            "action": "clean",
            "limit": None,
            "total": 0,
            "processed": 0,
            "cleaned": None,
            "events": [],
            "queued_at": "2026-09-01T12:00:00",
            "started_at": "2026-09-01T12:00:00",
            "completed_at": None,
        }
        with app._jobs_lock:
            app._jobs[job["id"]] = job
        app._run_bulk_stage_job(job["id"], app.JOB_TYPE_BULK_CLEAN)
        with app._jobs_lock:
            app._jobs.clear()
        app._restore_jobs_from_db()
        with app._jobs_lock:
            restored = dict(app._jobs[job["id"]])
        self.assertEqual(restored["status"], "completed")
        self.assertIsNotNone(restored["completed_at"])

    def test_failed_optional_detail_probe_opens_circuit(self):
        with (
            mock.patch.object(app, "_DETAIL_COMBO", None),
            mock.patch.object(app, "_DETAIL_DISABLED_UNTIL", 0.0),
            mock.patch.object(app, "DETAIL_PROBE_LIMIT", 4),
            mock.patch.object(app, "DETAIL_CIRCUIT_SECONDS", 600),
            mock.patch.object(app.time, "monotonic", side_effect=[100.0, 100.0, 101.0]),
            mock.patch.object(app, "_api_get", return_value=None) as api_get,
        ):
            self.assertIsNone(app._fetch_place_details("place-one", "key"))
            self.assertEqual(api_get.call_count, 4)
            self.assertIsNone(app._fetch_place_details("place-two", "key"))
            self.assertEqual(api_get.call_count, 4)


class RuntimeHealthAndExportTests(unittest.TestCase):
    def setUp(self):
        self.client = app.app.test_client()

    def test_health_tracks_queue_worker_without_touching_database(self):
        worker = mock.Mock()
        worker.is_alive.return_value = True
        with (
            mock.patch.object(app, "_queue_worker_thread", worker),
            mock.patch.object(app, "get_conn", side_effect=AssertionError("health touched SQLite")),
        ):
            response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["worker_alive"])

    def test_export_streams_cursor_batches_without_fetchall(self):
        columns_per_row = 22

        class FakeCursor:
            def __init__(self):
                self.calls = 0

            def fetchmany(self, size):
                self.calls += 1
                self.size = size
                if self.calls == 1:
                    return [tuple(["value"] * columns_per_row)]
                return []

            def fetchall(self):
                raise AssertionError("export used fetchall")

        cursor = FakeCursor()

        class FakeConnection:
            closed = False

            def execute(self, _sql, _params):
                return cursor

            def close(self):
                self.closed = True

        connection = FakeConnection()
        with mock.patch.object(app, "get_conn", return_value=connection):
            response = self.client.get("/api/export")
            body = response.data

        self.assertEqual(response.status_code, 200)
        self.assertEqual(cursor.size, 500)
        self.assertGreaterEqual(cursor.calls, 2)
        self.assertTrue(connection.closed)
        self.assertIn(b"value", body)


if __name__ == "__main__":
    unittest.main()
