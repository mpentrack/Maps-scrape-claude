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


if __name__ == "__main__":
    unittest.main()
