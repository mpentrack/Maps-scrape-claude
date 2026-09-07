import tempfile
import unittest
import sqlite3
from pathlib import Path

from benchmark_enrichment import (
    canonical_domain,
    classify_existing_email,
    is_campaign_eligible,
    load_instantly_exclusions,
    select_cohort,
)
from enrichment_providers import _openweb_email_contacts, _unique_emails


class BenchmarkHelpersTests(unittest.TestCase):
    def test_canonical_domain_handles_url_and_email(self):
        self.assertEqual(canonical_domain("https://www.Example.com/path"), "example.com")
        self.assertEqual(canonical_domain("Owner@Example.com"), "example.com")

    def test_quality_rejects_template_and_placeholder_addresses(self):
        self.assertEqual(
            classify_existing_email("hash@sentry-next.wixpress.com", "https://realco.com"),
            "junk",
        )
        self.assertEqual(
            classify_existing_email("%20info@realco.com", "https://realco.com"),
            "junk",
        )
        self.assertEqual(
            classify_existing_email("info@realco.com", "https://realco.com"),
            "generic",
        )
        self.assertEqual(
            classify_existing_email("jane@realco.com", "https://realco.com"),
            "named_site_domain",
        )

    def test_recursive_email_extraction_deduplicates(self):
        payload = {"emails": ["Jane@Example.com", {"value": "jane@example.com"}]}
        self.assertEqual(_unique_emails(payload), ["jane@example.com"])

    def test_openweb_contacts_preserve_per_email_source(self):
        payload = {"emails": [{"value": "jane@example.com", "sources": ["https://example.com/team"]}]}
        contacts = _openweb_email_contacts(payload)
        self.assertEqual(contacts[0].source_url, "https://example.com/team")

    def test_campaign_eligibility_rejects_public_and_corporate_records(self):
        self.assertEqual(
            is_campaign_eligible({"category": "Park"}, "capecanaveral.gov"),
            (False, "noncommercial_domain"),
        )
        self.assertEqual(
            is_campaign_eligible({"category": "Hotel"}, "hilton.com"),
            (False, "corporate_chain_domain"),
        )
        self.assertEqual(
            is_campaign_eligible({"category": "HVAC contractor"}, "localair.com"),
            (True, "eligible"),
        )

    def test_instantly_exclusions_accept_flexible_headers(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "leads.csv"
            path.write_text(
                "Email,Website\nowner@example.com,https://www.example.com/contact\n",
                encoding="utf-8",
            )
            emails, domains = load_instantly_exclusions(str(path))
        self.assertIn("owner@example.com", emails)
        self.assertIn("example.com", domains)

    def test_cohort_uses_listing_zip_not_search_zip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "businesses.db"
            conn = sqlite3.connect(path)
            conn.execute(
                "CREATE TABLE businesses (id INTEGER PRIMARY KEY, business_name TEXT, "
                "website_url TEXT, email TEXT, city TEXT, zip_code TEXT, search_zip TEXT, "
                "rating REAL, review_count INTEGER, category TEXT, search_keyword TEXT, "
                "vertical TEXT, pipeline_stage TEXT, stage_reason TEXT)"
            )
            conn.executemany(
                "INSERT INTO businesses VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (1, "Local", "https://local.example", None, "Melbourne", "32901", "32901", 5, 10, "x", "x", "x", "scraped", None),
                    (2, "Distant", "https://distant.example", None, "Portland", "97201", "32901", 5, 20, "x", "x", "x", "archived", "geo_zip_mismatch"),
                ],
            )
            conn.commit()
            conn.close()
            cohort = select_cohort(
                str(path), limit=10, excluded_domains=set(), zip_prefixes=["329"],
                max_domain_locations=5,
            )
        self.assertEqual([row["business_name"] for row in cohort], ["Local"])


if __name__ == "__main__":
    unittest.main()
