"""One isolated website crawl. The parent enforces the wall-clock deadline."""

import json
import sys
from dataclasses import asdict

import requests

from email_extract import USER_AGENT, scrape_email_for_website


def main():
    payload = json.load(sys.stdin)
    with requests.Session() as session:
        session.headers.update({"User-Agent": USER_AGENT})
        result = scrape_email_for_website(
            payload["website_url"], session,
            allow_generic_fallback=payload["allow_generic_fallback"],
        )
    print(json.dumps(asdict(result)))


if __name__ == "__main__":
    main()
