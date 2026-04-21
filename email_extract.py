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
from urllib.parse import unquote, urljoin, urlparse

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


# Consumer / free-mail hosts — kept as candidates but ranked below same-site addresses.
FREE_EMAIL_PROVIDER_DOMAINS: frozenset[str] = frozenset({
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "me.com", "mac.com", "live.com", "msn.com",
    "protonmail.com", "proton.me", "googlemail.com", "ymail.com",
    "gmx.com", "gmx.net", "mail.com", "zoho.com",
})

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


def _site_host_for_match(website_url: str) -> str:
    """Hostname used for same-site email preference (no scheme/path, no leading www.)."""
    if not (website_url or "").strip():
        return ""
    try:
        host = (urlparse(normalize_website_url(website_url)).hostname or "").lower()
    except ValueError:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def email_domain_matches_site(email: str, site_host: str) -> bool:
    """True if the mailbox domain is the site host or a plausible parent/child match."""
    if not site_host:
        return False
    dom = email.split("@")[-1].lower()
    if dom == site_host:
        return True
    if dom.endswith("." + site_host) and len(dom) > len(site_host):
        return True
    if site_host.endswith("." + dom) and len(site_host) > len(dom):
        return True
    return False


def is_free_email_provider(email: str) -> bool:
    return email.split("@")[-1].lower() in FREE_EMAIL_PROVIDER_DOMAINS


def _email_priority_tier(email: str, site_host: str) -> int:
    """
    Lower = better. Same-site always beats off-site; free-mail hosts sink below
    corporate domains unless the address is on the business's own domain.
    """
    gen = is_generic_email(email)
    on_site = email_domain_matches_site(email, site_host)
    free = is_free_email_provider(email)
    if not gen and on_site:
        return 0
    if gen and on_site:
        return 1
    if not gen and not free:
        return 2
    if not gen and free:
        return 3
    if gen and not free:
        return 4
    return 5


def pick_best_email(
    emails: list[str],
    *,
    allow_generic_fallback: bool,
    website_url: str | None = None,
) -> str | None:
    if not emails:
        return None
    site_host = _site_host_for_match(website_url or "")

    seen: set[str] = set()
    ordered: list[str] = []
    for e in emails:
        e = e.strip().lower()
        if not e or e in seen or not is_plausible_email(e):
            continue
        seen.add(e)
        ordered.append(e)
    if not ordered:
        return None

    usable = [e for e in ordered if allow_generic_fallback or not is_generic_email(e)]
    if not usable:
        return None

    best_tier = min(_email_priority_tier(e, site_host) for e in usable)
    for e in ordered:
        if e not in usable:
            continue
        if _email_priority_tier(e, site_host) == best_tier:
            return e
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
        best = pick_best_email(
            all_emails,
            allow_generic_fallback=allow_generic_fallback,
            website_url=website_url,
        )
        if best and (allow_generic_fallback or not is_generic_email(best)):
            return EmailScrapeResult(best, None)
    best = pick_best_email(
        all_emails,
        allow_generic_fallback=allow_generic_fallback,
        website_url=website_url,
    )
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
