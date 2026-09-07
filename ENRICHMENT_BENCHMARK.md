# Decision-maker enrichment benchmark

This benchmark reads the scraper database in SQLite read-only mode, excludes
companies already represented in an Instantly export, and tests enrichment
providers on plausible local businesses. API keys are read from environment
variables and never written to output.

## Current result (September 2026)

On ten eligible Brevard/Indian River businesses:

- Anymail Finder: 5 verified named contacts, 10 credits charged.
- Findymail: 0 contacts; 1 employee-search credit charged in the corrected
  three-company retest.
- OpenWeb Ninja: HTTP 403 because the account was not subscribed to the Website
  Contacts Scraper API; no credits charged.

The current production order is therefore:

1. Existing free website crawler.
2. Anymail Finder decision-maker lookup for records still lacking a named email.
3. Keep unmatched records for later providers; do not guess email patterns.

OpenWeb Ninja can be re-tested ahead of Anymail Finder after the Website
Contacts Scraper API is enabled in its dashboard. Its returned addresses are
publicly sourced but not mailbox-verified, so verification is still required.

## Safe usage

Create environment variables in the runtime or deployment secret manager:

```bash
export OPENWEBNINJA_API_KEY='...'
export ANYMAILFINDER_API_KEY='...'
export FINDYMAIL_API_KEY='...'
```

Always dry-run first:

```bash
python benchmark_enrichment.py \
  --db businesses.db \
  --instantly-csv leads.csv \
  --zip-prefix 329 \
  --limit 10
```

Live calls require both `--execute` and an explicit credit ceiling:

```bash
python benchmark_enrichment.py \
  --db businesses.db \
  --instantly-csv leads.csv \
  --zip-prefix 329 \
  --limit 10 \
  --providers anymailfinder \
  --execute \
  --max-credits 20
```

Outputs go to `benchmark_output/`, which is ignored by Git. Provider errors,
HTTP statuses, source URLs, roles, and reported credit use remain auditable in
the result CSV and summary JSON.
