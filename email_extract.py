"""
Shared email discovery helpers for website HTML (used by app.py and enrich_emails.py).
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


def env_int(name: str, default: int, minimum: int = 1) -> int:
    """Read an int from the environment, falling back to the default.

    Tolerates values wrapped in quotes and never raises: a stray character in a
    deployment variable must not crash the process at import, before the web
    server binds its port.
    """
    raw = (os.environ.get(name) or "").strip().strip('"').strip("'").strip()
    try:
        value = int(raw)
    except ValueError:
        if raw:
            log.warning("Invalid %s=%r — falling back to %d", name, os.environ.get(name), default)
        return default
    if value < minimum:
        log.warning("%s=%d is below the minimum of %d — using %d", name, value, minimum, minimum)
        return minimum
    return value

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Patterns used to expand human-readable email obfuscations before regex matching.
# e.g. "john [at] hvacpros [dot] com" → "john@hvacpros.com"
#
# The bracketed forms are unambiguous. A bare " at " is not: prose like
# "Serving customers at Newington.We cover Hartford" would otherwise become
# "customers@Newington.We". _OBF_AT_BARE therefore only fires when what follows
# genuinely looks like a domain — a labelled name plus a real dotted TLD.
_OBF_AT = re.compile(r"\s*[\[\(\{]\s*at\s*[\]\)\}]\s*", re.I)
_OBF_DOT = re.compile(r"\s*[\[\(\{]\s*dot\s*[\]\)\}]\s*", re.I)
_OBF_AT_BARE = re.compile(
    r"(?<=[\w.\-])\s+at\s+(?=[\w\-]+(?:\s+dot\s+|\.)[\w\-]+\.[a-z]{2,})", re.I
)


def _normalize_obfuscated(text: str) -> str:
    """Expand [at]/(at)/{at}/[dot]/… obfuscations so EMAIL_RE can match them."""
    t = _OBF_AT.sub("@", text)
    t = _OBF_AT_BARE.sub("@", t)
    t = _OBF_DOT.sub(".", t)
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

# Fetched first, in this order — named contacts live on /contact and /team far
# more often than anywhere else.
PRIORITY_SUBPAGES = ["/contact", "/contact-us", "/about", "/team"]

REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 10)
MAX_ATTEMPTS = 2
# Never let one unusually large page consume the Railway container. The body is
# streamed and truncated before decoding/BeautifulSoup parsing, so this is a
# real memory bound rather than a check performed after the download.
MAX_RESPONSE_BYTES = env_int("MAX_RESPONSE_BYTES", 2 * 1024 * 1024)
# How many pages that actually load are read per domain before we stop.
MAX_PAGES_PER_DOMAIN = env_int("MAX_PAGES_PER_DOMAIN", 4)
# Bounds wasted requests on sites where most subpages 404.
_MAX_FETCH_ATTEMPTS_MULTIPLIER = 3


@dataclass(frozen=True)
class EmailScrapeResult:
    email: str | None
    """Set when a stored address is chosen (respecting generic fallback policy)."""
    stage_reason: str | None
    """When email is None: 'no_email_found' or 'only_generic_email'."""
    city: str | None = None
    """City name inferred from the business website, when not already known."""
    all_emails: str | None = None
    """Every plausible candidate found, comma-joined, deduped, best-first."""
    signals: dict | None = None
    """Ad-tech fingerprints from detect_signals(), merged across all pages read."""


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
# Ad-tech / call-tracking fingerprints
# ---------------------------------------------------------------------------

_RE_GOOGLE_ADS = re.compile(
    r"AW-\d{9,}|googleadservices\.com/pagead/conversion|google_conversion_id"
    r"|gtag\(\s*['\"]config['\"]\s*,\s*['\"]AW-",
    re.I,
)
_RE_AW_IDS   = re.compile(r"AW-(\d{9,})", re.I)
_RE_CALLRAIL = re.compile(r"(cdn|js)\.callrail\.com", re.I)
_RE_CTM      = re.compile(r"tctm\.co|calltrackingmetrics\.com", re.I)
_RE_GTM      = re.compile(r"googletagmanager\.com/gtm\.js", re.I)
_RE_GA4      = re.compile(r"gtag/js\?id=G-", re.I)


def detect_signals(html: str) -> dict:
    """Fingerprint paid-search and call-tracking stacks in a raw HTML page.

    Runs on the unparsed source so inline <script> bodies are included.
    """
    if not html:
        return {
            "runs_google_ads": False,
            "aw_ids": "",
            "call_tracking": None,
            "has_gtm": False,
            "has_ga4": False,
        }
    aw_ids = sorted({m.group(1) for m in _RE_AW_IDS.finditer(html)})
    call_tracking = None
    if _RE_CALLRAIL.search(html):
        call_tracking = "callrail"
    elif _RE_CTM.search(html):
        call_tracking = "ctm"
    return {
        "runs_google_ads": bool(_RE_GOOGLE_ADS.search(html)),
        "aw_ids": ",".join(aw_ids),
        "call_tracking": call_tracking,
        "has_gtm": bool(_RE_GTM.search(html)),
        "has_ga4": bool(_RE_GA4.search(html)),
    }


def _merge_signals(acc: dict, page: dict) -> dict:
    """OR the booleans, union the AW ID set, keep the first call-tracking vendor."""
    ids = set(filter(None, (acc.get("aw_ids") or "").split(",")))
    ids |= set(filter(None, (page.get("aw_ids") or "").split(",")))
    return {
        "runs_google_ads": bool(acc.get("runs_google_ads")) or bool(page.get("runs_google_ads")),
        "aw_ids": ",".join(sorted(ids)),
        "call_tracking": acc.get("call_tracking") or page.get("call_tracking"),
        "has_gtm": bool(acc.get("has_gtm")) or bool(page.get("has_gtm")),
        "has_ga4": bool(acc.get("has_ga4")) or bool(page.get("has_ga4")),
    }


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


def rank_emails(emails: list[str], website_url: str | None = None) -> list[str]:
    """Dedupe + drop implausible addresses, best-first by the pick_best_email tiers."""
    site_host = _site_host_for_match(website_url or "")
    seen: set[str] = set()
    ordered: list[str] = []
    for e in emails:
        e = (e or "").strip().lower()
        if not e or e in seen or not is_plausible_email(e):
            continue
        seen.add(e)
        ordered.append(e)
    return sorted(ordered, key=lambda e: _email_priority_tier(e, site_host))


def _extract_cloudflare_emails(soup: BeautifulSoup) -> list[str]:
    """Decode Cloudflare Email Obfuscation payloads (data-cfemail hex blobs)."""
    decoded: list[str] = []
    for tag in soup.select("[data-cfemail]"):
        encoded = tag.get("data-cfemail")
        if not isinstance(encoded, str) or len(encoded) < 4:
            continue
        try:
            b = bytes.fromhex(encoded)
            key = b[0]
            email = "".join(chr(x ^ key) for x in b[1:])
            if "@" in email:
                decoded.append(email.lower())
        except Exception:
            continue
    return decoded


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

    # Must run before decompose() below — that strips the tags carrying the payload.
    candidates.extend(_extract_cloudflare_emails(soup))

    for s in soup(["script", "style", "noscript"]):
        s.decompose()

    text = soup.get_text(" ")
    for m in EMAIL_RE.finditer(text):
        candidates.append(m.group(0).lower())

    # Second pass on normalized text to catch obfuscated addresses.
    normalized = _normalize_obfuscated(text)
    if normalized != text:
        for m in EMAIL_RE.finditer(normalized):
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
        resp = None
        try:
            resp = session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                stream=True,
            )
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 503):
                wait = 2**attempt
                log.debug("HTTP %s on %s, retry in %ss", resp.status_code, url, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            content_type = (resp.headers.get("Content-Type") or "").lower()
            if content_type and not any(
                marker in content_type for marker in ("text/", "html", "xhtml", "xml")
            ):
                log.debug("Skipping non-HTML content %s on %s", content_type, url)
                return None

            body = bytearray()
            truncated = False
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                remaining = MAX_RESPONSE_BYTES - len(body)
                if remaining <= 0:
                    truncated = True
                    break
                body.extend(chunk[:remaining])
                if len(chunk) > remaining or len(body) >= MAX_RESPONSE_BYTES:
                    truncated = True
                    break
            if truncated:
                log.debug("Truncated oversized page at %d bytes — %s", MAX_RESPONSE_BYTES, url)
            encoding = resp.encoding or "utf-8"
            try:
                return bytes(body).decode(encoding, errors="replace")
            except LookupError:
                return bytes(body).decode("utf-8", errors="replace")
        except requests.Timeout:
            log.debug("Timeout (%d/%d) — %s", attempt, MAX_ATTEMPTS, url)
        except requests.RequestException as exc:
            log.debug("Request error (%d/%d) %s — %s", attempt, MAX_ATTEMPTS, url, exc)
        finally:
            if resp is not None:
                resp.close()
    return None


def _page_urls_for(base: str) -> list[str]:
    """Homepage first, then the pages named contacts live on, then the long tail."""
    tail = [p for p in SUBPAGES if p not in PRIORITY_SUBPAGES]
    return [base] + [
        urljoin(base + "/", p.lstrip("/")) for p in (PRIORITY_SUBPAGES + tail)
    ]


def scrape_email_for_website(
    website_url: str,
    session: requests.Session,
    *,
    allow_generic_fallback: bool,
) -> EmailScrapeResult:
    base = normalize_website_url(website_url)
    if not base:
        return EmailScrapeResult(None, "no_email_found")

    candidates: list[str] = []
    signals: dict = detect_signals("")
    any_page_loaded = False
    pages_read = 0
    attempts = 0
    max_attempts = max(
        len(PRIORITY_SUBPAGES) + 1,
        MAX_PAGES_PER_DOMAIN * _MAX_FETCH_ATTEMPTS_MULTIPLIER,
    )
    city_found: str | None = None
    site_host = _site_host_for_match(website_url)

    for url in _page_urls_for(base):
        if pages_read >= MAX_PAGES_PER_DOMAIN or attempts >= max_attempts:
            break
        attempts += 1
        html = http_get_text(session, url)
        if html is None:
            continue
        any_page_loaded = True
        pages_read += 1
        # Signals are collected from every page, whether or not it yields an email.
        signals = _merge_signals(signals, detect_signals(html))
        candidates.extend(extract_email_candidates(html))
        if city_found is None:
            city_found = extract_city_from_html(html)
        # Only a tier-0 hit (named contact on the business's own domain) is worth
        # stopping for — anything else may still be beaten by a later page.
        best_so_far = pick_best_email(
            candidates,
            allow_generic_fallback=allow_generic_fallback,
            website_url=website_url,
        )
        if best_so_far and _email_priority_tier(best_so_far, site_host) == 0:
            return EmailScrapeResult(
                best_so_far, None, city_found,
                ",".join(rank_emails(candidates, website_url)), signals,
            )

    ranked = rank_emails(candidates, website_url)
    all_emails_str = ",".join(ranked) or None
    best = pick_best_email(
        candidates,
        allow_generic_fallback=allow_generic_fallback,
        website_url=website_url,
    )
    if best:
        return EmailScrapeResult(best, None, city_found, all_emails_str, signals)

    # WHOIS fallback — often surfaces the owner's direct registrant email.
    try:
        hostname = urlparse(base).hostname or ""
    except ValueError:
        hostname = ""
    if hostname:
        w_email = whois_email_for_domain(hostname)
        if w_email and (allow_generic_fallback or not is_generic_email(w_email)):
            with_whois = ",".join(rank_emails(ranked + [w_email], website_url)) or None
            return EmailScrapeResult(w_email, None, city_found, with_whois, signals)

    if not any_page_loaded:
        return EmailScrapeResult(None, "no_email_found", city_found, all_emails_str, signals)
    if not ranked:
        return EmailScrapeResult(None, "no_email_found", city_found, all_emails_str, signals)
    if not allow_generic_fallback and all(is_generic_email(e) for e in ranked):
        return EmailScrapeResult(None, "only_generic_email", city_found, all_emails_str, signals)
    return EmailScrapeResult(None, "no_email_found", city_found, all_emails_str, signals)
