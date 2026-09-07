"""Auditable adapters for contact-enrichment providers.

Provider keys are supplied by callers and are never included in results, logs,
exceptions, or persisted request metadata.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any
import json

import requests


EMAIL_RE = re.compile(
    r"(?i)(?<![\w.+-])([a-z0-9][a-z0-9._%+-]*@[a-z0-9.-]+\.[a-z]{2,})(?![\w.-])"
)


def _safe_error(body: Any) -> str:
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict) and error.get("message"):
            return str(error["message"])[:300]
        for key in ("message", "detail", "status"):
            if body.get(key):
                return str(body[key])[:300]
    try:
        return json.dumps(body)[:300]
    except (TypeError, ValueError):
        return str(body)[:300]


@dataclass
class ContactResult:
    email: str
    email_status: str
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    job_title: str | None = None
    linkedin_url: str | None = None
    source_url: str | None = None
    source_context: str | None = None


@dataclass
class ProviderResult:
    provider: str
    outcome: str
    contacts: list[ContactResult] = field(default_factory=list)
    credits_charged: float = 0.0
    http_status: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["contacts"] = [asdict(contact) for contact in self.contacts]
        return data


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    timeout: float = 180,
) -> tuple[int, Any]:
    response = session.request(
        method, url, headers=headers, params=params, json=payload, timeout=timeout
    )
    try:
        body = response.json()
    except ValueError:
        body = {"message": (response.text or "")[:500]}
    return response.status_code, body


def _iter_strings(value: Any):
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from _iter_strings(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _iter_strings(item)


def _unique_emails(value: Any) -> list[str]:
    seen: set[str] = set()
    found: list[str] = []
    for text in _iter_strings(value):
        for match in EMAIL_RE.findall(text):
            email = match.lower().strip(" .,:;<>[]()")
            if email not in seen:
                seen.add(email)
                found.append(email)
    return found


def _first_http_url(value: Any) -> str | None:
    for text in _iter_strings(value):
        if text.startswith(("http://", "https://")):
            return text
    return None


FREE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com",
    "icloud.com", "me.com", "yahoo.com", "ymail.com", "aol.com",
}


def _openweb_email_contacts(body: Any, expected_domain: str | None = None) -> list[ContactResult]:
    contacts: list[ContactResult] = []
    seen: set[str] = set()
    payload = body
    if isinstance(body, dict) and isinstance(body.get("data"), list):
        payload = body["data"][0] if body["data"] else {}
    raw_emails = payload.get("emails", []) if isinstance(payload, dict) else []
    if isinstance(raw_emails, list):
        for item in raw_emails:
            if isinstance(item, dict):
                email = item.get("value")
                sources = item.get("sources") or []
                source_url = next(
                    (url for url in sources if isinstance(url, str) and url.startswith(("http://", "https://"))),
                    None,
                )
            else:
                email, source_url = item, None
            if not isinstance(email, str) or not EMAIL_RE.fullmatch(email.strip()):
                continue
            email = email.strip().lower()
            local, email_domain = email.rsplit("@", 1)
            # Website scrapers occasionally capture adjacent HTML/text as an
            # apparently valid address. Reject these and unrelated domains;
            # retain legitimate free-mail addresses because small local firms
            # often publish an owner's Gmail address.
            if ".com" in email_domain[:-4] or (
                expected_domain
                and email_domain != expected_domain
                and email_domain not in FREE_EMAIL_DOMAINS
            ):
                continue
            if local.isdigit() or email.lower() in seen:
                continue
            if isinstance(email, str) and email.lower() not in seen:
                seen.add(email.lower())
                contacts.append(ContactResult(
                    email=email,
                    email_status="public_unverified",
                    source_url=source_url,
                    source_context="OpenWeb Ninja website contacts result",
                ))
    return contacts


def openwebninja_contacts(
    domain: str,
    api_key: str,
    *,
    session: requests.Session | None = None,
) -> ProviderResult:
    own_session = session is None
    session = session or requests.Session()
    try:
        status, body = _request_json(
            session,
            "GET",
            "https://api.openwebninja.com/website-contacts-scraper/scrape-contacts",
            headers={"x-api-key": api_key},
            params={"query": domain},
            timeout=180,
        )
        if status != 200:
            return ProviderResult(
                "openwebninja", "error", http_status=status, error=_safe_error(body)
            )
        contacts = _openweb_email_contacts(body, expected_domain=domain)
        return ProviderResult(
            "openwebninja",
            "found" if contacts else "not_found",
            contacts=contacts,
            credits_charged=1.0,
            http_status=status,
        )
    except requests.RequestException as exc:
        return ProviderResult("openwebninja", "error", error=type(exc).__name__)
    finally:
        if own_session:
            session.close()


def anymail_decision_maker(
    domain: str,
    api_key: str,
    *,
    categories: list[str] | None = None,
    session: requests.Session | None = None,
) -> ProviderResult:
    own_session = session is None
    session = session or requests.Session()
    categories = categories or ["ceo", "operations", "marketing"]
    try:
        status, body = _request_json(
            session,
            "POST",
            "https://api.anymailfinder.com/v5.1/find-email/decision-maker",
            headers={"Authorization": api_key, "Content-Type": "application/json"},
            payload={"domain": domain, "decision_maker_category": categories},
        )
        if status != 200 or not isinstance(body, dict):
            return ProviderResult(
                "anymailfinder", "error", http_status=status, error=_safe_error(body)
            )
        email = body.get("valid_email")
        contacts = []
        if isinstance(email, str) and email.strip():
            contacts.append(
                ContactResult(
                    email=email.strip().lower(),
                    email_status="valid",
                    full_name=body.get("person_full_name"),
                    first_name=body.get("person_first_name"),
                    last_name=body.get("person_last_name"),
                    job_title=body.get("person_job_title"),
                    linkedin_url=body.get("person_linkedin_url"),
                    source_context=(
                        "Anymail Finder decision maker: "
                        + str(body.get("decision_maker_category") or "unknown")
                    ),
                )
            )
        outcome = "found" if contacts else str(body.get("email_status") or "not_found")
        return ProviderResult(
            "anymailfinder",
            outcome,
            contacts=contacts,
            credits_charged=float(body.get("credits_charged") or 0),
            http_status=status,
        )
    except requests.RequestException as exc:
        return ProviderResult("anymailfinder", "error", error=type(exc).__name__)
    finally:
        if own_session:
            session.close()


def findymail_domain_contact(
    domain: str,
    api_key: str,
    *,
    roles: list[str] | None = None,
    session: requests.Session | None = None,
) -> ProviderResult:
    """Find one role-matched employee, then resolve their verified email."""
    own_session = session is None
    session = session or requests.Session()
    roles = roles or ["Owner", "Founder", "CEO", "President", "Operations"]
    try:
        status, body = _request_json(
            session,
            "POST",
            "https://app.findymail.com/api/search/employees",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            payload={"website": domain, "job_titles": roles, "count": 1},
        )
        if status != 200 or not isinstance(body, dict):
            # Some accounts return the employee list as a top-level array.
            if not (status == 200 and isinstance(body, list)):
                return ProviderResult(
                    "findymail", "error", http_status=status, error=_safe_error(body)
                )
        raw_contacts = body if isinstance(body, list) else (
            body.get("contacts") or body.get("data") or body.get("payload", {}).get("contacts") or []
        )
        if not isinstance(raw_contacts, list) or not raw_contacts:
            return ProviderResult(
                "findymail", "not_found", credits_charged=0.0, http_status=status
            )
        person = next((item for item in raw_contacts if isinstance(item, dict)), None)
        if not person or not person.get("name"):
            return ProviderResult("findymail", "not_found", http_status=status)
        email_status, email_body = _request_json(
            session,
            "POST",
            "https://app.findymail.com/api/search/name",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            payload={"name": person["name"], "domain": domain},
        )
        if email_status != 200 or not isinstance(email_body, dict):
            return ProviderResult(
                "findymail", "error", credits_charged=1.0, http_status=email_status,
                error=_safe_error(email_body),
            )
        contact_data = email_body.get("contact") or email_body
        email = contact_data.get("email") if isinstance(contact_data, dict) else None
        contacts: list[ContactResult] = []
        if isinstance(email, str) and email.strip():
            contacts.append(ContactResult(
                email=email.strip().lower(),
                email_status="valid",
                full_name=contact_data.get("name") or person.get("name"),
                job_title=person.get("job_title") or person.get("jobTitle") or person.get("role"),
                linkedin_url=person.get("linkedin_url") or person.get("linkedinUrl"),
                source_context="Findymail employee role search + name/domain email lookup",
            ))
        return ProviderResult(
            "findymail",
            "found" if contacts else "not_found",
            contacts=contacts,
            credits_charged=1.0 + float(bool(contacts)),
            http_status=email_status,
        )
    except requests.RequestException as exc:
        return ProviderResult("findymail", "error", error=type(exc).__name__)
    finally:
        if own_session:
            session.close()
