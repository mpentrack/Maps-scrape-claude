"""
Optional guard: Maps vendor may return listings whose address is not in the searched ZIP.
When enabled, only accept rows whose formatted address contains that 5-digit ZIP (ZIP+4 ok).
"""

from __future__ import annotations

import os
import re


def strict_zip_in_address_enabled() -> bool:
    v = os.environ.get("STRICT_ZIP_IN_ADDRESS", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def formatted_address_contains_search_zip(address: str | None, search_zip: str) -> bool:
    """True if address contains the 5-digit search ZIP as its own token (allows ZIP+4)."""
    if not search_zip:
        return True
    digits = re.sub(r"\D", "", str(search_zip))
    if len(digits) < 5:
        return True
    z = digits[:5].zfill(5)
    if not address or not str(address).strip():
        return False
    return bool(re.search(rf"(?<!\d){re.escape(z)}(?:-\d{{4}})?(?!\d)", str(address)))


def row_passes_search_zip_filter(address: str | None, search_zip: str) -> bool:
    """If STRICT_ZIP_IN_ADDRESS is on, require the formatted address to include the search ZIP."""
    if not strict_zip_in_address_enabled():
        return True
    return formatted_address_contains_search_zip(address, search_zip)
