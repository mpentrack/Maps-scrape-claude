"""
Extract city from Maps API payloads or US-style full addresses (no AI).

Preference order:
1) Explicit API fields (city, locality, address_components, nested address dict)
2) Parse comma-separated US address: segment before \"ST 12345\" / \"ST 12345-1234\"
"""

from __future__ import annotations

import re
from typing import Any

# Trailing segment: optional country; 2-letter state + ZIP (US)
_STATE_ZIP_TAIL = re.compile(
    r"^\s*([A-Za-z]{2})\s+(\d{5})(?:-\d{4})?\s*"
    r"(?:,?\s*(USA|United States|US))?\.?\s*$",
    re.IGNORECASE,
)

# Fallback: ", City, ST 12345" at end of string
_CITY_BEFORE_STATE_ZIP = re.compile(
    r",\s*([^,]+),\s*[A-Za-z]{2}\s+\d{5}(?:-\d{4})?\s*$",
)


def _str_val(v: Any) -> str | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and not v.is_integer():
            return str(v)
        return str(int(v))
    if isinstance(v, str):
        s = v.strip()
        return s or None
    return None


def formatted_address_from_item(item: dict[str, Any] | None) -> str | None:
    """
    Single-line address for DB/CSV from typical Maps / Places JSON shapes.
    Covers camelCase keys and nested address dicts when full_address is absent.
    """
    if not item or not isinstance(item, dict):
        return None

    for key in (
        "full_address", "formatted_address", "formattedAddress", "fullAddress",
        "address_string", "location_address", "locationAddress",
    ):
        s = _str_val(item.get(key))
        if s:
            return s

    raw = item.get("address")
    if isinstance(raw, str):
        s = _str_val(raw)
        if s:
            return s

    if isinstance(raw, dict):
        for key in ("formatted_address", "formatted", "formattedAddress"):
            s = _str_val(raw.get(key))
            if s:
                return s

        sa = _str_val(raw.get("street_address"))
        sn = _str_val(raw.get("street_number"))
        rt = _str_val(raw.get("route") or raw.get("street"))
        if sa:
            street_line = sa
        else:
            parts = [p for p in (sn, rt) if p]
            street_line = " ".join(parts) if parts else None

        city = _str_val(raw.get("city") or raw.get("locality") or raw.get("town"))
        st_raw = raw.get("state") or raw.get("administrative_area_level_1") or raw.get("administrative_area")
        if isinstance(st_raw, dict):
            st = _str_val(st_raw.get("short_name") or st_raw.get("long_name"))
        else:
            st = _str_val(st_raw)
        postal = _str_val(
            raw.get("postal_code") or raw.get("zip_code") or raw.get("zip") or raw.get("postcode")
        )

        locality_bits: list[str] = []
        if city:
            locality_bits.append(city)
        if st and postal:
            locality_bits.append(f"{st} {postal}")
        elif st:
            locality_bits.append(st)
        elif postal:
            locality_bits.append(postal)

        if street_line and locality_bits:
            return f"{street_line}, {', '.join(locality_bits)}"
        if locality_bits:
            return ", ".join(locality_bits)
        if street_line:
            return street_line

    s = _str_val(item.get("vicinity"))
    if s:
        return s

    return None


def _clean_city(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    return s or None


def city_from_maps_item(item: dict[str, Any] | None) -> str | None:
    """Use vendor/Google-style fields when present."""
    if not item or not isinstance(item, dict):
        return None

    for key in ("city", "locality", "city_name", "town"):
        v = item.get(key)
        if isinstance(v, str):
            c = _clean_city(v)
            if c:
                return c
        if isinstance(v, dict):
            c = _clean_city(v.get("name") or v.get("long_name"))
            if c:
                return c

    addr = item.get("address")
    if isinstance(addr, dict):
        for key in ("city", "locality", "town"):
            v = addr.get(key)
            if isinstance(v, str):
                c = _clean_city(v)
                if c:
                    return c

    for comp in item.get("address_components") or []:
        if not isinstance(comp, dict):
            continue
        types = comp.get("types") or []
        if not isinstance(types, list):
            continue
        if "locality" in types or "postal_town" in types or "sublocality" in types:
            c = _clean_city(comp.get("long_name") or comp.get("short_name"))
            if c:
                return c

    return None


def city_from_us_address(address: str | None) -> str | None:
    """
    Parse typical US full_address: \"..., City, ST 12345\" or \"..., City, ST 12345, USA\".
    """
    if not address or not isinstance(address, str):
        return None
    normalized = address.replace("\n", ",")
    parts = [p.strip() for p in normalized.split(",") if p.strip()]
    if len(parts) < 2:
        return None

    for i in range(len(parts) - 1, 0, -1):
        if _STATE_ZIP_TAIL.match(parts[i]):
            candidate = parts[i - 1]
            return _clean_city(candidate)

    m = _CITY_BEFORE_STATE_ZIP.search(address.strip())
    if m:
        return _clean_city(m.group(1))

    return None


def resolve_city(item: dict[str, Any] | None, address: str | None, _zip_code: str | None = None) -> str | None:
    """
    _zip_code reserved for future heuristics; US parsing uses address tail.
    """
    c = city_from_maps_item(item)
    if c:
        return c
    return city_from_us_address(address)
