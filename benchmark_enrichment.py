"""Benchmark contact enrichment on an automatically selected Railway cohort.

The input database is read-only. Paid APIs are called only with --execute.
Credentials are read only from environment variables and are never exported.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

from enrichment_providers import (
    ProviderResult,
    anymail_decision_maker,
    findymail_domain_contact,
    openwebninja_contacts,
)


GENERIC_LOCAL_PARTS = {
    "admin", "contact", "customerservice", "hello", "help", "info", "mail",
    "marketing", "office", "sales", "service", "support", "team",
}
JUNK_DOMAINS = {
    "domain.com", "email.com", "example.com", "godaddy.com", "namebright.com",
    "namerider.com", "sentry-next.wixpress.com", "wix-domains.com",
}
JUNK_DOMAIN_FRAGMENTS = (
    "domaincontrol", "domains.siteground", "domainsbyproxy", "privacy",
    "registrar", "sentry-next.wixpress",
)
NONCOMMERCIAL_DOMAIN_SUFFIXES = (".gov", ".edu", ".mil")
NONPROSPECT_CATEGORY_TERMS = {
    "cemetery", "church", "city hall", "community center", "county government",
    "cruise terminal", "fire station", "government office", "library", "museum", "park",
    "police department", "public school", "school", "university",
}
# Corporate brand domains tend to return headquarters contacts rather than the
# operator of the local Maps listing. Keep this short and benchmark-specific.
CORPORATE_CHAIN_DOMAINS = {
    "choicehotels.com", "hilton.com", "ihg.com", "marriott.com",
    "serviceexperts.com",
}


def canonical_domain(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip().lower()
    if "@" in candidate and not candidate.startswith(("http://", "https://")):
        candidate = candidate.rsplit("@", 1)[-1]
    if not candidate.startswith(("http://", "https://")):
        candidate = "https://" + candidate
    host = (urlparse(candidate).hostname or "").lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def classify_existing_email(email: str | None, website_url: str | None) -> str:
    if not email or "@" not in email:
        return "missing"
    value = email.strip().lower()
    local, domain = value.rsplit("@", 1)
    site_domain = canonical_domain(website_url)
    if (
        domain in JUNK_DOMAINS
        or any(fragment in domain for fragment in JUNK_DOMAIN_FRAGMENTS)
        or local in {"example", "filler", "user", "you", "your", "john.doe"}
        or local.startswith("%20")
        or re.fullmatch(r"[0-9a-f]{24,}", local)
    ):
        return "junk"
    if local in GENERIC_LOCAL_PARTS:
        return "generic"
    if site_domain and (domain == site_domain or domain.endswith("." + site_domain)):
        return "named_site_domain"
    return "other"


def is_campaign_eligible(row: dict, domain: str) -> tuple[bool, str]:
    """Reject records unlikely to represent a reachable local decision maker."""
    if domain.endswith(NONCOMMERCIAL_DOMAIN_SUFFIXES):
        return False, "noncommercial_domain"
    if domain in CORPORATE_CHAIN_DOMAINS:
        return False, "corporate_chain_domain"
    category_text = " ".join(
        str(row.get(key) or "").lower()
        for key in ("category", "search_keyword", "vertical")
    )
    if any(term in category_text for term in NONPROSPECT_CATEGORY_TERMS):
        return False, "nonprospect_category"
    return True, "eligible"


def load_instantly_exclusions(path: str | None) -> tuple[set[str], set[str]]:
    emails: set[str] = set()
    domains: set[str] = set()
    if not path:
        return emails, domains
    with open(path, newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            normalized = {str(k).strip().lower(): v for k, v in row.items() if k}
            for key, value in normalized.items():
                if not value:
                    continue
                if "email" in key and "@" in str(value):
                    email = str(value).strip().lower()
                    emails.add(email)
                    domain = canonical_domain(email)
                    if domain:
                        domains.add(domain)
                if key in {"website", "website_url", "domain", "company_domain"}:
                    domain = canonical_domain(str(value))
                    if domain:
                        domains.add(domain)
    return emails, domains


def select_cohort(
    db_path: str,
    *,
    limit: int,
    excluded_domains: set[str],
    zip_prefixes: list[str],
    max_domain_locations: int = 5,
) -> list[dict]:
    conn = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    domain_counts: Counter = Counter()
    for record in conn.execute(
        "SELECT website_url FROM businesses WHERE website_url IS NOT NULL "
        "AND TRIM(website_url) != ''"
    ):
        counted_domain = canonical_domain(record[0])
        if counted_domain:
            domain_counts[counted_domain] += 1
    query = (
        "SELECT id, business_name, website_url, email, city, zip_code, search_zip, "
        "rating, review_count, category, search_keyword, vertical, pipeline_stage, "
        "stage_reason FROM businesses WHERE website_url IS NOT NULL "
        "AND TRIM(website_url) != '' "
        "AND COALESCE(stage_reason, '') != 'geo_zip_mismatch' "
        "AND COALESCE(pipeline_stage, '') != 'geo_rejected'"
    )
    params: list[object] = []
    if zip_prefixes:
        clauses = []
        for prefix in zip_prefixes:
            # search_zip is the ZIP used in the Maps query, not proof of the
            # listing's location. Using it here can admit distant results.
            clauses.append("COALESCE(zip_code, '') LIKE ?")
            params.append(prefix + "%")
        query += " AND (" + " OR ".join(clauses) + ")"
    query += " ORDER BY COALESCE(review_count, 0) DESC, id ASC LIMIT ?"
    params.append(max(limit * 30, 1000))
    rows = conn.execute(query, params).fetchall()
    conn.close()

    selected: list[dict] = []
    seen_domains: set[str] = set()
    for raw in rows:
        row = dict(raw)
        domain = canonical_domain(row.get("website_url"))
        if (
            not domain
            or domain in excluded_domains
            or domain in seen_domains
            or domain_counts[domain] > max_domain_locations
        ):
            continue
        eligible, _ = is_campaign_eligible(row, domain)
        if not eligible:
            continue
        quality = classify_existing_email(row.get("email"), row.get("website_url"))
        if quality == "named_site_domain":
            continue
        row["domain"] = domain
        row["domain_location_count"] = domain_counts[domain]
        row["existing_email_quality"] = quality
        selected.append(row)
        seen_domains.add(domain)
        if len(selected) >= limit:
            break
    return selected


def flatten_results(cohort: list[dict], results: dict[int, list[ProviderResult]]) -> list[dict]:
    rows: list[dict] = []
    for business in cohort:
        provider_results = results.get(int(business["id"]), [])
        if not provider_results:
            rows.append({**business, "provider": "", "outcome": "not_run"})
            continue
        for result in provider_results:
            if not result.contacts:
                rows.append({
                    **business,
                    "provider": result.provider,
                    "outcome": result.outcome,
                    "credits_charged": result.credits_charged,
                    "http_status": result.http_status or "",
                    "provider_error": result.error or "",
                })
                continue
            for contact in result.contacts:
                rows.append({
                    **business,
                    "provider": result.provider,
                    "outcome": result.outcome,
                    "credits_charged": result.credits_charged,
                    "found_email": contact.email,
                    "email_status": contact.email_status,
                    "full_name": contact.full_name or "",
                    "first_name": contact.first_name or "",
                    "last_name": contact.last_name or "",
                    "job_title": contact.job_title or "",
                    "linkedin_url": contact.linkedin_url or "",
                    "source_url": contact.source_url or "",
                    "source_context": contact.source_context or "",
                    "provider_error": result.error or "",
                    "http_status": result.http_status or "",
                })
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--instantly-csv")
    parser.add_argument("--output-dir", default="benchmark_output")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--zip-prefix", action="append", default=[])
    parser.add_argument(
        "--max-domain-locations",
        type=int,
        default=5,
        help="Exclude likely chains with more than this many DB rows on one domain.",
    )
    parser.add_argument(
        "--providers",
        default="openwebninja,anymailfinder,findymail",
        help="Comma-separated provider names.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Call providers. Without this flag the command is a free dry run.",
    )
    parser.add_argument(
        "--max-credits",
        type=float,
        default=0,
        help="Hard cross-provider credit ceiling. Required with --execute.",
    )
    args = parser.parse_args()

    _, excluded_domains = load_instantly_exclusions(args.instantly_csv)
    cohort = select_cohort(
        args.db,
        limit=max(1, min(args.limit, 1000)),
        excluded_domains=excluded_domains,
        zip_prefixes=args.zip_prefix,
        max_domain_locations=max(1, args.max_domain_locations),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "benchmark_cohort.csv", cohort)

    provider_names = [name.strip() for name in args.providers.split(",") if name.strip()]
    max_credits = {
        "openwebninja": len(cohort),
        "anymailfinder": len(cohort) * 2,
        "findymail": len(cohort),
    }
    if not args.execute:
        print(json.dumps({
            "mode": "dry_run",
            "cohort_size": len(cohort),
            "providers": provider_names,
            "maximum_credits_if_every_call_succeeds": {
                name: max_credits.get(name) for name in provider_names
            },
            "cohort_csv": str(output_dir / "benchmark_cohort.csv"),
        }, indent=2))
        return
    if args.max_credits <= 0:
        raise SystemExit("--execute requires a positive --max-credits ceiling")

    keys = {
        "openwebninja": os.environ.get("OPENWEBNINJA_API_KEY"),
        "anymailfinder": os.environ.get("ANYMAILFINDER_API_KEY"),
        "findymail": os.environ.get("FINDYMAIL_API_KEY"),
    }
    missing = [name for name in provider_names if not keys.get(name)]
    if missing:
        raise SystemExit("Missing environment variable(s) for: " + ", ".join(missing))

    results: dict[int, list[ProviderResult]] = {}
    total_credits = 0.0
    maximum_call_cost = {"openwebninja": 1.0, "anymailfinder": 2.0, "findymail": 2.0}
    for index, business in enumerate(cohort, 1):
        domain = business["domain"]
        business_results: list[ProviderResult] = []
        print(f"[{index}/{len(cohort)}] {business['business_name']} ({domain})")
        for provider in provider_names:
            if total_credits + maximum_call_cost.get(provider, 0.0) > args.max_credits:
                result = ProviderResult(provider, "budget_exhausted")
                business_results.append(result)
                print(f"  {provider}: budget_exhausted")
                continue
            if provider == "openwebninja":
                result = openwebninja_contacts(domain, keys[provider] or "")
            elif provider == "anymailfinder":
                result = anymail_decision_maker(domain, keys[provider] or "")
            elif provider == "findymail":
                result = findymail_domain_contact(domain, keys[provider] or "")
            else:
                raise SystemExit(f"Unknown provider: {provider}")
            business_results.append(result)
            total_credits += result.credits_charged
            print(
                f"  {provider}: {result.outcome}; "
                f"contacts={len(result.contacts)}; credits={result.credits_charged}"
            )
        results[int(business["id"])] = business_results

    rows = flatten_results(cohort, results)
    write_csv(output_dir / "benchmark_results.csv", rows)
    outcomes = Counter(
        (result.provider, result.outcome)
        for values in results.values() for result in values
    )
    credits: Counter = Counter()
    contacts: Counter = Counter()
    for values in results.values():
        for result in values:
            credits[result.provider] += result.credits_charged
            contacts[result.provider] += len(result.contacts)
    summary = {
        "cohort_size": len(cohort),
        "provider_outcomes": {
            f"{provider}:{outcome}": count
            for (provider, outcome), count in outcomes.items()
        },
        "credits_charged": dict(credits),
        "contacts_found": dict(contacts),
        "credit_ceiling": args.max_credits,
        "total_credits_charged": sum(credits.values()),
    }
    (output_dir / "benchmark_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
