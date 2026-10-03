# Website crawl isolation

Dashboard enrichment runs each website in `crawl_worker.py`, with at most three
children in flight. The parent enforces a 120-second wall-clock deadline covering
DNS, redirects, response streaming, HTML parsing and optional WHOIS. On timeout
it kills and reaps the child, commits that lead as `enrich_failed` with reason
`crawl_timeout`, and continues the same job. Successful leads retain their email
and advertising signals. Timeout leads remain saved for review or a later retry.

`CRAWL_TIMEOUT_SECONDS` accepts 10–150 seconds and `ENRICH_WORKERS` accepts 1–3.
These bounds keep crawl deadlines below the minimum 180-second job watchdog and
limit simultaneous parser memory. `/health` reports the active isolation mode,
deadline and worker count. Job progress includes the `errors` counter and logs
identify failed row IDs and source jobs.

The durable SQLite queue resumes incomplete jobs after deployment. Completed
ZIPs are not repeated. Rows in `enriching` at a container interruption retain the
existing `crawl_interrupted` recovery handling. This isolation applies to both
full-pipeline dashboard jobs and dashboard bulk enrichment, not the standalone
`enrich_emails.py` CLI.

Verification: `python3 -m unittest -q` includes a real localhost HTTP server that
stalls its response. The test verifies the child is killed and reaped, the next
healthy lead enriches, and cleaning completes without restarting the service.
