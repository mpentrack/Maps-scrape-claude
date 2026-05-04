"""
Shared email discovery helpers for website HTML (used by app.py and enrich_emails.py).
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import smtplib
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Comment as _BSComment

log = logging.getLogger(__name__)

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Patterns used to expand human-readable email obfuscations before regex matching.
# e.g. "john [at] hvacpros [dot] com" → "john@hvacpros.com"
_OBFUSCATED_AT = re.compile(r"\s*[\[\(]at[\]\)]\s*|\s+at\s+(?=[a-zA-Z0-9])", re.IGNORECASE)
_OBFUSCATED_DOT = re.compile(r"\s*[\[\(]dot[\]\)]\s*", re.IGNORECASE)


def _normalize_obfuscated(text: str) -> str:
    """Expand [at]/(at)/[dot]/(dot) obfuscations so EMAIL_RE can match them."""
    t = _OBFUSCATED_AT.sub("@", text)
    t = _OBFUSCATED_DOT.sub(".", t)
    return t


# Privacy-shield / registrar-proxy domains that appear in WHOIS but are useless as contacts.
_WHOIS_JUNK_DOMAINS: frozenset[str] = frozenset({
    "privacyguardian.org", "domainsbyproxy.com", "whoisguard.com",
    "contactprivacy.com", "whoisprivacy.com", "privacyprotect.org",
    "networksolutionsprivate.com", "domainprivacygroup.com",
    "withheldforprivacy.com", "anonymize.com", "domains.google.com",
    "namecheaphosting.com", "registrar-servers.com", "hugedomains.com",
    "godaddy.com", "above.com", "perfectprivacy.com",
})

# Substrings that identify ICANN-mandated abuse/complaint contacts or junk registrar
# addresses — never real owner emails.
_WHOIS_JUNK_KEYWORDS: frozenset[str] = frozenset({
    "abuse", "complaint", "icann",
    "domain@", "web.com", "example", "sample", "verisign", "whois", "email.com",
})


def whois_email_for_domain(domain: str) -> str | None:
    """Return the registrant email from WHOIS, skipping privacy shields.

    Returns None on any failure so callers never need to handle exceptions.
    The `python-whois` package is an optional dependency; if missing this
    silently returns None.
    """
    if not domain:
        return None
    domain = domain.lower().strip().lstrip("www.")
    try:
        import whois  # optional: python-whois
        w = whois.whois(domain)
        emails = w.emails
        if isinstance(emails, str):
            emails = [emails]
        if not isinstance(emails, list):
            return None
        for e in emails:
            if not isinstance(e, str) or "@" not in e:
                continue
            e = e.strip().lower()
            if e.split("@")[-1] in _WHOIS_JUNK_DOMAINS:
                continue
            if any(kw in e for kw in _WHOIS_JUNK_KEYWORDS):
                continue
            if is_plausible_email(e):
                return e
    except Exception:
        pass
    return None


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
    "/staff", "/our-team", "/people", "/bios", "/leadership",
    "/owner", "/get-in-touch", "/estimate", "/free-estimate",
    "/schedule", "/contact.html", "/about.html",
]

# Sitemap URL scoring: pages whose paths contain these keywords are most likely
# to have contact info.
_SITEMAP_CONTACT_KWS: frozenset[str] = frozenset({
    "contact", "about", "team", "staff", "owner", "people",
    "leadership", "reach", "email", "location", "directory",
    "bios", "who-we-are", "meet", "get-in-touch", "connect",
})

REQUEST_TIMEOUT = 5
MAX_ATTEMPTS = 2

# Regex to extract owner name from copyright notice: © 2024 John Smith
_FOOTER_COPYRIGHT_RE = re.compile(
    r"©\s*(?:\d{4}[-–]\d{2,4}|\d{4})\s+([A-Z][a-z]{1,20}(?:\s+[A-Z][a-z]{1,20}){1,3})",
    re.UNICODE,
)
# Words that signal the end of a person's name in a copyright line
_COPYRIGHT_STOP_WORDS = re.compile(
    r"\s+(?:All|Rights|Reserved|Inc|LLC|Corp|Ltd|Co|DBA|and|&|The|By)\.?\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class EmailScrapeResult:
    email: str | None
    """Set when a stored address is chosen (respecting generic fallback policy)."""
    stage_reason: str | None
    """When email is None: 'no_email_found' or 'only_generic_email'."""
    city: str | None = None
    """City name inferred from the business website, when not already known."""
    owner_name: str | None = None
    """Owner/principal name extracted from footer copyright, BBB, etc."""


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


# ---------------------------------------------------------------------------
# City extraction helpers
# ---------------------------------------------------------------------------

_US_STATES = frozenset({
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC",
})

# Common non-city words that sometimes appear as addressLocality in structured data.
_CITY_STOP_WORDS: frozenset[str] = frozenset({
    "usa", "united states", "us", "america", "nationwide", "national",
    "online", "virtual", "remote", "service area", "your area",
})

# "City Name, ST" — anchored to a 2-letter state code.
_CITY_STATE_RE = re.compile(
    r"\b([A-Z][a-zA-Z](?:[a-zA-Z .'\-]{0,30}?)[a-zA-Z])\s*,\s*([A-Z]{2})\b"
)


def _city_from_jsonld(obj: Any) -> str | None:
    """Recursively search a parsed JSON-LD object for addressLocality."""
    if isinstance(obj, dict):
        loc = obj.get("addressLocality") or obj.get("address", {})
        if isinstance(loc, str) and loc.strip():
            candidate = loc.strip().title()
            if candidate.lower() not in _CITY_STOP_WORDS and len(candidate) >= 2:
                return candidate
        if isinstance(loc, dict):
            sub = loc.get("addressLocality")
            if isinstance(sub, str) and sub.strip():
                candidate = sub.strip().title()
                if candidate.lower() not in _CITY_STOP_WORDS and len(candidate) >= 2:
                    return candidate
        for v in obj.values():
            result = _city_from_jsonld(v)
            if result:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _city_from_jsonld(item)
            if result:
                return result
    return None


def extract_city_from_html(html: str) -> str | None:
    """Return the most likely city name for the business, or None."""
    soup = BeautifulSoup(html, "html.parser")

    # 1. JSON-LD structured data — most reliable source.
    for script in soup.find_all("script", attrs={"type": lambda t: t and "ld+json" in str(t).lower()}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            result = _city_from_jsonld(json.loads(raw))
            if result:
                return result
        except json.JSONDecodeError:
            continue

    # 2. HTML microdata — itemprop="addressLocality"
    for tag in soup.find_all(attrs={"itemprop": "addressLocality"}):
        text = (tag.get("content") or tag.get_text() or "").strip()
        if text and text.lower() not in _CITY_STOP_WORDS and len(text) >= 2:
            return text.title()

    # 3. Regex scan for "City, ST" pattern in visible text.
    for s in soup(["script", "style", "noscript"]):
        s.decompose()
    page_text = soup.get_text(" ")
    for m in _CITY_STATE_RE.finditer(page_text):
        city_candidate = m.group(1).strip()
        state_candidate = m.group(2).upper()
        if state_candidate in _US_STATES and city_candidate.lower() not in _CITY_STOP_WORDS:
            return city_candidate.title()

    return None


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
    """Collect unique emails from all sources: mailto, JSON-LD, inline scripts,
    HTML comments, visible text, obfuscations, stripped HTML, and data attributes."""
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []

    # 1. Mailto links
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if not isinstance(href, str):
            continue
        if href.lower().startswith("mailto:"):
            addr = unquote(href[7:].split("?")[0].strip())
            if EMAIL_RE.fullmatch(addr):
                candidates.append(addr.lower())

    # 2. JSON-LD structured data
    for script in soup.find_all("script", attrs={"type": lambda t: t and "ld+json" in str(t).lower()}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            _walk_json_for_emails(json.loads(raw), candidates)
        except json.JSONDecodeError:
            continue

    # 3. Non-JSON-LD inline script content (before decomposing)
    for script in soup.find_all("script"):
        stype = (script.get("type") or "").lower()
        if "json" in stype:
            continue  # already handled above
        if script.get("src"):
            continue  # external — handled by extract_emails_from_js_files
        text = script.string or script.get_text() or ""
        for m in EMAIL_RE.finditer(text):
            candidates.append(m.group(0).lower())

    # 4. HTML comments (before decomposing scripts)
    for comment in soup.find_all(string=lambda t: isinstance(t, _BSComment)):
        for m in EMAIL_RE.finditer(str(comment)):
            candidates.append(m.group(0).lower())

    # Remove script/style/noscript nodes before text extraction
    for s in soup(["script", "style", "noscript"]):
        s.decompose()

    # 5. Visible text
    text = soup.get_text(" ")
    for m in EMAIL_RE.finditer(text):
        candidates.append(m.group(0).lower())

    # 6. Second pass on normalized text to catch obfuscated addresses.
    normalized = _normalize_obfuscated(text)
    if normalized != text:
        for m in EMAIL_RE.finditer(normalized):
            candidates.append(m.group(0).lower())

    # 7. Remaining HTML (catches emails in attributes, hidden elements, etc.)
    html_remain = str(soup)
    for m in EMAIL_RE.finditer(html_remain):
        candidates.append(m.group(0).lower())

    # 8. Data attributes — scan all data-* attributes plus common contact attributes
    for tag in soup.find_all(True):
        for attr, val in tag.attrs.items():
            if not isinstance(val, str):
                continue
            attr_low = attr.lower()
            if "@" not in val:
                continue
            if attr_low.startswith("data-") or attr_low in ("content", "value", "placeholder", "title"):
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


# ---------------------------------------------------------------------------
# New email-finding helpers
# ---------------------------------------------------------------------------

def get_sitemap_urls(base_url: str, session: requests.Session) -> list[str]:
    """Parse /sitemap.xml to discover real subpages scored by contact-relevance."""
    base_host = urlparse(base_url).netloc
    locs: list[str] = []

    for path in ("/sitemap.xml", "/sitemap_index.xml", "/sitemap"):
        try:
            html = http_get_text(session, base_url + path)
            if not html:
                continue
            root = ET.fromstring(html)
            for el in root.iter():
                if el.tag.endswith("}loc") or el.tag == "loc":
                    if el.text and el.text.strip():
                        locs.append(el.text.strip())
            if locs:
                break
        except Exception:
            continue

    if not locs:
        return []

    base_host_clean = base_host.lstrip("www.")

    def _score(url: str) -> int:
        low = url.lower()
        return sum(1 for kw in _SITEMAP_CONTACT_KWS if kw in low)

    same_domain = [
        u for u in locs
        if urlparse(u).netloc.lstrip("www.") == base_host_clean
    ]
    scored = sorted(same_domain, key=_score, reverse=True)
    return [u for u in scored if _score(u) > 0][:10]


def extract_emails_from_js_files(
    soup: BeautifulSoup,
    page_url: str,
    session: requests.Session,
) -> list[str]:
    """Fetch up to 5 same-origin linked JS files and scan them for emails."""
    page_host = urlparse(page_url).netloc
    candidates: list[str] = []
    seen: set[str] = set()

    for tag in soup.find_all("script", src=True):
        src = tag.get("src", "")
        if not src or not isinstance(src, str):
            continue
        full_url = urljoin(page_url, src)
        if urlparse(full_url).netloc != page_host:
            continue
        if full_url in seen:
            continue
        seen.add(full_url)
        if len(seen) > 5:
            break
        try:
            js_text = http_get_text(session, full_url)
            if js_text:
                for m in EMAIL_RE.finditer(js_text):
                    candidates.append(m.group(0).lower())
        except Exception:
            pass

    return candidates


def extract_emails_from_pdfs(
    soup: BeautifulSoup,
    base_url: str,
    session: requests.Session,
) -> list[str]:
    """Follow up to 3 same-origin PDF links and extract emails from their text."""
    try:
        import pdfplumber
    except ImportError:
        return []

    base_host = urlparse(base_url).netloc
    candidates: list[str] = []
    seen: set[str] = set()
    pdf_count = 0

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if not isinstance(href, str):
            continue
        full_url = urljoin(base_url, href)
        low = full_url.lower()
        if not (".pdf" in low):
            continue
        if urlparse(full_url).netloc != base_host:
            continue
        if full_url in seen:
            continue
        seen.add(full_url)
        if pdf_count >= 3:
            break
        try:
            resp = session.get(full_url, timeout=REQUEST_TIMEOUT, stream=True)
            if resp.status_code != 200:
                continue
            content_length = int(resp.headers.get("content-length", 0))
            if content_length > 5 * 1024 * 1024:
                continue
            pdf_bytes = resp.content
            if len(pdf_bytes) > 5 * 1024 * 1024:
                continue
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages[:10]:
                    page_text = page.extract_text() or ""
                    for m in EMAIL_RE.finditer(page_text):
                        candidates.append(m.group(0).lower())
            pdf_count += 1
        except Exception:
            pass

    return candidates


def extract_footer_owner_name(soup: BeautifulSoup) -> str | None:
    """Extract owner name from footer copyright (© 2024 John Smith) or <meta name=author>."""
    # Check <meta name="author"> first — explicit and reliable
    meta = soup.find("meta", attrs={"name": re.compile(r"^author$", re.I)})
    if meta:
        content = (meta.get("content") or "").strip()
        if content and len(content.split()) >= 2:
            return content

    # Search footer elements first, then fall back to full page
    search_targets = (
        soup.find_all(["footer"]) or
        soup.find_all(True, class_=re.compile(r"footer", re.I)) or
        [soup]
    )
    for target in search_targets:
        text = target.get_text(" ")
        m = _FOOTER_COPYRIGHT_RE.search(text)
        if m:
            name = m.group(1).strip()
            # Strip trailing copyright boilerplate words (All Rights Reserved, Inc, etc.)
            while True:
                cleaned = _COPYRIGHT_STOP_WORDS.sub("", name).strip()
                if cleaned == name:
                    break
                name = cleaned
            if len(name.split()) >= 2:
                return name

    return None


def wayback_email_for_url(
    url: str,
    session: requests.Session,
    allow_generic_fallback: bool,
) -> str | None:
    """Check the Wayback Machine CDX API for archived versions that may contain an email."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        if not domain:
            return None

        resp = session.get(
            "http://web.archive.org/cdx/search/cdx",
            params={
                "url": domain + "/*",
                "output": "json",
                "limit": "5",
                "fl": "timestamp,original",
                "filter": "statuscode:200",
                "collapse": "digest",
                "matchType": "domain",
            },
            timeout=7,
        )
        if resp.status_code != 200:
            return None

        results = resp.json()
        if len(results) <= 1:  # first row is header ["timestamp","original"]
            return None

        for row in results[1:3]:
            timestamp, orig_url = row[0], row[1]
            archive_url = f"http://web.archive.org/web/{timestamp}id_/{orig_url}"
            html = http_get_text(session, archive_url)
            if not html:
                continue
            emails = extract_email_candidates(html)
            best = pick_best_email(
                emails,
                allow_generic_fallback=allow_generic_fallback,
                website_url=url,
            )
            if best and (allow_generic_fallback or not is_generic_email(best)):
                return best
    except Exception:
        pass
    return None


def bbb_email_lookup(
    business_name: str,
    city: str,
    session: requests.Session,
    allow_generic_fallback: bool,
) -> tuple[str | None, str | None]:
    """Search BBB for the business and return (email, owner_name). Both may be None."""
    if not business_name:
        return None, None
    try:
        resp = session.get(
            "https://www.bbb.org/search",
            params={"find_text": business_name, "find_loc": city or ""},
            timeout=8,
        )
        if resp.status_code != 200:
            return None, None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find first business profile link
        result_link = soup.select_one('a[href*="/us/"]')
        if not result_link or not result_link.get("href"):
            return None, None

        href = result_link["href"]
        profile_url = ("https://www.bbb.org" + href) if href.startswith("/") else href
        profile_html = http_get_text(session, profile_url)
        if not profile_html:
            return None, None

        profile_soup = BeautifulSoup(profile_html, "html.parser")

        # Extract email
        emails = extract_email_candidates(profile_html)
        best_email = pick_best_email(
            emails,
            allow_generic_fallback=allow_generic_fallback,
            website_url=None,
        )

        # Extract principal/owner name
        owner_name: str | None = None
        for tag in profile_soup.find_all(["dt", "th", "strong", "b", "span"]):
            label = tag.get_text().strip().lower()
            if "principal" in label or "owner" in label or "contact" in label:
                next_el = tag.find_next_sibling()
                if not next_el:
                    parent = tag.parent
                    if parent:
                        next_el = tag.next_sibling
                if next_el:
                    name_text = (
                        next_el.get_text().strip()
                        if hasattr(next_el, "get_text")
                        else str(next_el).strip()
                    )
                    if name_text and len(name_text.split()) >= 2 and "@" not in name_text:
                        owner_name = name_text
                        break

        return best_email, owner_name
    except Exception:
        pass
    return None, None


def press_release_email_search(
    business_name: str,
    domain: str,
    session: requests.Session,
    allow_generic_fallback: bool,
) -> str | None:
    """Search DuckDuckGo for prnewswire/businesswire press releases and extract media contact emails."""
    if not business_name:
        return None
    try:
        query = f'site:prnewswire.com "{business_name}"'
        resp = session.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            timeout=8,
            headers={"User-Agent": USER_AGENT},
        )
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # DuckDuckGo HTML search results
        result = soup.select_one(".result__a")
        if not result or not result.get("href"):
            return None

        pr_url = result["href"]
        # DDG sometimes wraps the real URL in a redirect
        from urllib.parse import urlparse as _up, parse_qs as _pqs
        parsed_pr = _up(pr_url)
        if "duckduckgo.com" in (parsed_pr.netloc or ""):
            qs = _pqs(parsed_pr.query)
            pr_url = (qs.get("uddg") or qs.get("u") or [pr_url])[0]

        pr_html = http_get_text(session, pr_url)
        if not pr_html:
            return None

        emails = extract_email_candidates(pr_html)
        return pick_best_email(
            emails,
            allow_generic_fallback=allow_generic_fallback,
            website_url=None,
        )
    except Exception:
        pass
    return None


def infer_and_verify_owner_email(owner_name: str, domain: str) -> str | None:
    """Generate owner email candidates from name + domain, verify via SMTP RCPT TO.

    Uses a silent SMTP handshake (no email is ever sent). Requires dnspython
    for MX resolution. Returns None on any failure.
    """
    if not owner_name or not domain:
        return None
    parts = owner_name.strip().split()
    if len(parts) < 2:
        return None
    first = parts[0].lower().strip(".,")
    last = parts[-1].lower().strip(".,")
    if not first or not last:
        return None

    candidates = [
        f"{first}@{domain}",
        f"{first}.{last}@{domain}",
        f"{first[0]}.{last}@{domain}",
        f"{first[0]}{last}@{domain}",
        f"owner@{domain}",
    ]

    try:
        import dns.resolver
        mx_records = dns.resolver.resolve(domain, "MX")
        mx_host = str(
            sorted(mx_records, key=lambda r: r.preference)[0].exchange
        ).rstrip(".")
    except Exception:
        return None

    for candidate in candidates:
        try:
            with smtplib.SMTP(mx_host, 25, timeout=5) as smtp:
                smtp.ehlo("mailcheck.local")
                smtp.mail("probe@mailcheck.local")
                code, _ = smtp.rcpt(candidate)
                if code == 250:
                    log.debug("SMTP verified owner email: %s", candidate)
                    return candidate
        except Exception:
            pass

    return None


def hunter_io_email(domain: str, api_key: str) -> str | None:
    """Query Hunter.io domain-search API for the best email on the domain."""
    if not api_key or not domain:
        return None
    try:
        resp = requests.get(
            "https://api.hunter.io/v2/domain-search",
            params={"domain": domain, "api_key": api_key},
            timeout=10,
            headers={"User-Agent": USER_AGENT},
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        emails_data = (data.get("data") or {}).get("emails") or []
        if not emails_data:
            return None
        sorted_emails = sorted(
            emails_data, key=lambda e: e.get("confidence", 0), reverse=True
        )
        # Prefer non-generic with highest confidence
        for e in sorted_emails:
            addr = (e.get("value") or "").lower().strip()
            if addr and is_plausible_email(addr) and not is_generic_email(addr):
                return addr
        # Fallback to generic if nothing better
        for e in sorted_emails:
            addr = (e.get("value") or "").lower().strip()
            if addr and is_plausible_email(addr):
                return addr
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape_email_for_website(
    website_url: str,
    session: requests.Session,
    *,
    allow_generic_fallback: bool,
    business_name: str | None = None,
    city: str | None = None,
) -> EmailScrapeResult:
    base = normalize_website_url(website_url)
    if not base:
        return EmailScrapeResult(None, "no_email_found")

    try:
        hostname = urlparse(base).hostname or ""
    except ValueError:
        hostname = ""

    # ── Step 1: Build page list (sitemap-discovered + hardcoded fallbacks) ──
    sitemap_urls = get_sitemap_urls(base, session)
    seen_urls: set[str] = set()
    urls: list[str] = []
    for u in [base] + sitemap_urls + [urljoin(base + "/", p.lstrip("/")) for p in SUBPAGES]:
        if u not in seen_urls:
            seen_urls.add(u)
            urls.append(u)

    all_emails: list[str] = []
    any_page_loaded = False
    city_found: str | None = None
    owner_name: str | None = None
    collected_soups: list[tuple[BeautifulSoup, str]] = []

    # ── Step 2: Visit all pages; extract emails, city, owner name ──
    for url in urls:
        html = http_get_text(session, url)
        if html is None:
            continue
        any_page_loaded = True
        all_emails.extend(extract_email_candidates(html))
        if city_found is None:
            city_found = extract_city_from_html(html)

        soup = BeautifulSoup(html, "html.parser")
        collected_soups.append((soup, url))
        if owner_name is None:
            owner_name = extract_footer_owner_name(soup)

        best = pick_best_email(
            all_emails,
            allow_generic_fallback=allow_generic_fallback,
            website_url=website_url,
        )
        if best and (allow_generic_fallback or not is_generic_email(best)):
            return EmailScrapeResult(best, None, city_found, owner_name)

    # ── Step 3: Mine linked JavaScript files ──
    for soup, page_url in collected_soups:
        all_emails.extend(extract_emails_from_js_files(soup, page_url, session))

    best = pick_best_email(
        all_emails, allow_generic_fallback=allow_generic_fallback, website_url=website_url
    )
    if best and (allow_generic_fallback or not is_generic_email(best)):
        return EmailScrapeResult(best, None, city_found, owner_name)

    # ── Step 4: Mine PDFs linked from the homepage ──
    if collected_soups:
        homepage_soup, _ = collected_soups[0]
        all_emails.extend(extract_emails_from_pdfs(homepage_soup, base, session))

    best = pick_best_email(
        all_emails, allow_generic_fallback=allow_generic_fallback, website_url=website_url
    )
    if best and (allow_generic_fallback or not is_generic_email(best)):
        return EmailScrapeResult(best, None, city_found, owner_name)

    # ── Step 5: Wayback Machine archived versions ──
    if any_page_loaded:
        wb_email = wayback_email_for_url(base, session, allow_generic_fallback)
        if wb_email:
            return EmailScrapeResult(wb_email, None, city_found, owner_name)

    # ── Step 6: BBB profile ──
    if business_name or city or city_found:
        bbb_email, bbb_owner = bbb_email_lookup(
            business_name or "",
            city or city_found or "",
            session,
            allow_generic_fallback,
        )
        if bbb_owner and not owner_name:
            owner_name = bbb_owner
        if bbb_email:
            return EmailScrapeResult(bbb_email, None, city_found, owner_name)

    # ── Step 7: Press release media contact ──
    if business_name:
        pr_email = press_release_email_search(
            business_name, hostname, session, allow_generic_fallback
        )
        if pr_email:
            return EmailScrapeResult(pr_email, None, city_found, owner_name)

    # ── Step 8: Owner email inference + SMTP verification ──
    if owner_name and hostname:
        smtp_email = infer_and_verify_owner_email(owner_name, hostname)
        if smtp_email:
            return EmailScrapeResult(smtp_email, None, city_found, owner_name)

    # ── Step 9: WHOIS registrant email ──
    if hostname:
        w_email = whois_email_for_domain(hostname)
        if w_email and (allow_generic_fallback or not is_generic_email(w_email)):
            return EmailScrapeResult(w_email, None, city_found, owner_name)

    # ── Step 10: Hunter.io (paid — only if HUNTER_API_KEY is set) ──
    hunter_key = os.environ.get("HUNTER_API_KEY", "").strip()
    if hunter_key and hostname:
        h_email = hunter_io_email(hostname, hunter_key)
        if h_email and (allow_generic_fallback or not is_generic_email(h_email)):
            return EmailScrapeResult(h_email, None, city_found, owner_name)

    # ── Final fallback ──
    if not any_page_loaded:
        return EmailScrapeResult(None, "no_email_found", city_found, owner_name)
    plausible = [e for e in all_emails if is_plausible_email(e)]
    if not plausible:
        return EmailScrapeResult(None, "no_email_found", city_found, owner_name)
    if not allow_generic_fallback and all(is_generic_email(e) for e in plausible):
        return EmailScrapeResult(None, "only_generic_email", city_found, owner_name)
    return EmailScrapeResult(None, "no_email_found", city_found, owner_name)
