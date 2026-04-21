"""
Guard against geographically wrong listings from the Maps vendor.

The API often returns `full_address` *without* the 5-digit ZIP in the string while putting
the ZIP in separate fields (`postal_code`, `zip_code`, etc.). Address-only matching then
rejects 100% of rows — fixed by also reading those fields and optional state tail checks.

STRICT_ZIP_IN_ADDRESS=0 disables all of this (vendor trust mode).
"""

from __future__ import annotations

import os
import re
from typing import Any

_TRAILING_STATE = re.compile(
    r",\s*([A-Za-z]{2})(?:\s+\d{5}(?:-\d{4})?)?\s*(?:,?\s*(USA|United States|US))?\.?\s*$",
    re.IGNORECASE,
)
_ZIP_IN_TEXT = re.compile(r"(?<!\d)(\d{5})(?:-\d{4})?(?!\d)")


def strict_zip_in_address_enabled() -> bool:
    v = os.environ.get("STRICT_ZIP_IN_ADDRESS", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def normalize_zip5(s: str | None) -> str | None:
    if s is None:
        return None
    digits = re.sub(r"\D", "", str(s))
    if len(digits) < 5:
        return None
    return digits[:5].zfill(5)


def _collect_postal_strings(item: dict[str, Any]) -> list[str]:
    keys = (
        "postal_code", "zip", "zip_code", "postcode", "postal", "zipcode",
        "postalCode", "zipCode", "postal_code_short",
    )
    out: list[str] = []
    for k in keys:
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            out.append(str(int(v)))
    addr = item.get("address")
    if isinstance(addr, dict):
        for k in keys:
            v = addr.get(k)
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
    return out


def extract_zip5s_from_item_and_address(item: dict[str, Any], formatted_address: str | None) -> set[str]:
    found: set[str] = set()
    for s in _collect_postal_strings(item):
        z = normalize_zip5(s)
        if z:
            found.add(z)
    if formatted_address:
        for m in _ZIP_IN_TEXT.finditer(str(formatted_address)):
            found.add(m.group(1).zfill(5))
    return found


def formatted_address_contains_search_zip(address: str | None, search_zip: str) -> bool:
    """True if address contains the 5-digit search ZIP as its own token (allows ZIP+4)."""
    target = normalize_zip5(search_zip)
    if not target:
        return True
    if not address or not str(address).strip():
        return False
    return bool(re.search(rf"(?<!\d){re.escape(target)}(?:-\d{{4}})?(?!\d)", str(address)))


def _trailing_state_abbrev(address: str | None) -> str | None:
    if not address:
        return None
    m = _TRAILING_STATE.search(str(address).strip())
    if not m:
        return None
    return m.group(1).upper()


def _expected_state_for_search_zip(search_zip: str) -> str | None:
    """
    Rough US: Connecticut uses 060xx–069xx; used only when listing has no ZIP tokens
    but has a trailing state abbreviation.
    """
    z = normalize_zip5(search_zip)
    if not z:
        return None
    p3 = z[:3]
    if "060" <= p3 <= "069":
        return "CT"
    if "600" <= p3 <= "629":
        return "IL"
    if "100" <= p3 <= "149":
        return "NY"
    if "070" <= p3 <= "089":
        return "NJ"
    if "900" <= p3 <= "961":
        return "CA"
    return None


def listing_matches_search_zip(
    item: Any,
    formatted_address: str | None,
    search_zip: str,
) -> bool:
    """
    Accept listing if we can confirm it matches the searched ZIP, or if we have no ZIP
    signal but the trailing state matches the search ZIP's region (weak guard).
    Reject if we found ZIP(s) in the payload and none match the search ZIP.
    """
    if not strict_zip_in_address_enabled():
        return True
    row: dict[str, Any] = item if isinstance(item, dict) else {}
    target = normalize_zip5(search_zip)
    if not target:
        return True

    found = extract_zip5s_from_item_and_address(row, formatted_address)
    if target in found:
        return True
    if found:
        # Had explicit zips in API / address text, none match → off-target
        return False

    if formatted_address_contains_search_zip(formatted_address, search_zip):
        return True

    addr_state = _trailing_state_abbrev(formatted_address)
    expected = _expected_state_for_search_zip(search_zip)
    if addr_state and expected and addr_state == expected:
        return True
    if addr_state and expected and addr_state != expected:
        return False

    # No ZIP in payload and no state tail we trust — allow (vendor returned for this zip query)
    return True
