"""
Shared email discovery helpers for website HTML (used by app.py and enrich_emails.py).
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Local parts treated as generic / role inboxes — prefer any other address on the same site.
GENERIC_LOCAL_PREFIXES: frozenset[str] = frozenset({
    "info", "hello", "support", "contact", "sales", "admin",
    "help", "noreply", "no-reply", "donotreply", "do-not-reply", "webmaster",
    "enquiries", "enquiry", "office", "mail", "team", "media", "press",
    "billing", "accounts", "service", "customerservice", "customer",
    "careers", "jobs", "inquiries", "inquiry", "general",
})

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

SUBPAGES = [
    "/contact", "/contact-us", "/about", "/about-us",
    "/team", "/meet-the-team", "/location", "/locations",
]

REQUEST_TIMEOUT = 5
MAX_ATTEMPTS = 2


@dataclass(frozen=True)
class EmailScrapeResult:
    email: str | None
    """Set when a stored address is chosen (respecting generic fallback policy)."""
    stage_reason: str | None
    """When email is None: 'no_email_found' or 'only_generic_email'."""


# Domains that are almost never real mailbox hosts in scraped HTML.
_JUNK_EMAIL_DOMAIN_SUFFIXES = (
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
    ".css", ".js", ".json", ".woff", ".woff2",
)


def normalize_website_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u.rstrip("/")


def email_local_base(email: str) -> str:
    local = email.split("@", 1)[0].lower().strip()
    return local.split("+", 1)[0]


def is_generic_email(email: str) -> bool:
    return email_local_base(email) in GENERIC_LOCAL_PREFIXES


def is_plausible_email(email: str) -> bool:
    dom = email.split("@", -1)[-1].lower()
    return not dom.endswith(_JUNK_EMAIL_DOMAIN_SUFFIXES)


def pick_best_email(emails: list[str], *, allow_generic_fallback: bool) -> str | None:
    if not emails:
        return None
    filtered = [e for e in emails if is_plausible_email(e)]
    non_generic = [e for e in filtered if not is_generic_email(e)]
    if non_generic:
        return non_generic[0]
    if allow_generic_fallback and filtered:
        return filtered[0]
    return None


def _walk_json_for_emails(obj: Any, out: list[str]) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            _walk_json_for_emails(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _walk_json_for_emails(item, out)
    elif isinstance(obj, str):
        for m in EMAIL_RE.finditer(obj):
            out.append(m.group(0).lower())


def extract_email_candidates(html: str) -> list[str]:
    """Collect unique emails: mailto, JSON-LD, visible text, stripped HTML, and common attributes."""
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if not isinstance(href, str):
            continue
        if href.lower().startswith("mailto:"):
            addr = unquote(href[7:].split("?")[0].strip())
            if EMAIL_RE.fullmatch(addr):
                candidates.append(addr.lower())

    for script in soup.find_all("script", attrs={"type": lambda t: t and "ld+json" in str(t).lower()}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            _walk_json_for_emails(json.loads(raw), candidates)
        except json.JSONDecodeError:
            continue

    for s in soup(["script", "style", "noscript"]):
        s.decompose()

    text = soup.get_text(" ")
    for m in EMAIL_RE.finditer(text):
        candidates.append(m.group(0).lower())

    html_remain = str(soup)
    for m in EMAIL_RE.finditer(html_remain):
        candidates.append(m.group(0).lower())

    for tag in soup.find_all(True):
        for attr in ("data-email", "data-mail", "data-contact-email", "content"):
            val = tag.attrs.get(attr)
            if isinstance(val, str) and "@" in val:
                for m in EMAIL_RE.finditer(val):
                    candidates.append(m.group(0).lower())

    seen: set[str] = set()
    unique: list[str] = []
    for e in candidates:
        e = e.strip().lower()
        if e and e not in seen and is_plausible_email(e):
            seen.add(e)
            unique.append(e)
    return unique


def http_get_text(session: requests.Session, url: str) -> str | None:
    """GET with retries; None on hard failure. Skips retrying 404."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 503):
                wait = 2**attempt
                log.debug("HTTP %s on %s, retry in %ss", resp.status_code, url, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except requests.Timeout:
            log.debug("Timeout (%d/%d) — %s", attempt, MAX_ATTEMPTS, url)
        except requests.RequestException as exc:
            log.debug("Request error (%d/%d) %s — %s", attempt, MAX_ATTEMPTS, url, exc)
    return None


def scrape_email_for_website(
    website_url: str,
    session: requests.Session,
    *,
    allow_generic_fallback: bool,
) -> EmailScrapeResult:
    base = normalize_website_url(website_url)
    if not base:
        return EmailScrapeResult(None, "no_email_found")
    urls = [base] + [urljoin(base + "/", p.lstrip("/")) for p in SUBPAGES]
    all_emails: list[str] = []
    any_page_loaded = False
    for url in urls:
        html = http_get_text(session, url)
        if html is None:
            continue
        any_page_loaded = True
        all_emails.extend(extract_email_candidates(html))
        best = pick_best_email(all_emails, allow_generic_fallback=allow_generic_fallback)
        if best and (allow_generic_fallback or not is_generic_email(best)):
            return EmailScrapeResult(best, None)
    best = pick_best_email(all_emails, allow_generic_fallback=allow_generic_fallback)
    if best:
        return EmailScrapeResult(best, None)
    if not any_page_loaded:
        return EmailScrapeResult(None, "no_email_found")
    plausible = [e for e in all_emails if is_plausible_email(e)]
    if not plausible:
        return EmailScrapeResult(None, "no_email_found")
    if not allow_generic_fallback and all(is_generic_email(e) for e in plausible):
        return EmailScrapeResult(None, "only_generic_email")
    return EmailScrapeResult(None, "no_email_found")
