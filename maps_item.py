"""
Normalize Maps Data API (RapidAPI) search payloads and contact fields.
"""

from __future__ import annotations

import re
from typing import Any


def iter_search_results(data: Any, _depth: int = 0) -> list[dict]:
    """
    Return a flat list of business dicts from various wrapper shapes.
    Avoid treating a non-list `data` dict as the result set (which would iterate keys).
    """
    if data is None or _depth > 8:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []

    def _dicts_from_list(v: Any) -> list[dict] | None:
        if not isinstance(v, list) or not v:
            return None
        dicts = [x for x in v if isinstance(x, dict)]
        return dicts if dicts else None

    priority = (
        "data", "results", "result", "businesses", "places", "items",
        "records", "list", "rows", "companies", "search_results",
        "locations", "payload",
    )
    for key in priority:
        v = data.get(key)
        if isinstance(v, list):
            got = _dicts_from_list(v)
            if got:
                return got
        if isinstance(v, dict):
            inner = iter_search_results(v, _depth + 1)
            if inner:
                return inner

    for _k, v in data.items():
        if isinstance(v, list):
            got = _dicts_from_list(v)
            if got:
                return got
        if isinstance(v, dict) and _depth < 4:
            inner = iter_search_results(v, _depth + 1)
            if inner:
                return inner
    return []


def _first_str(*vals: Any) -> str | None:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            s = str(int(v)) if v == int(v) else str(v)
            if s.strip():
                return s.strip()
    return None


def contact_fields_from_maps_item(item: dict[str, Any]) -> tuple[str | None, str | None]:
    """Best-effort phone and website from one search/detail object."""
    phone = _first_str(
        item.get("phone_number"),
        item.get("phone"),
        item.get("formatted_phone_number"),
        item.get("international_phone_number"),
        item.get("phoneNumber"),
        item.get("tel"),
        item.get("mobile"),
        item.get("main_phone"),
    )
    if phone and not re.search(r"\d", phone):
        phone = None

    url = _first_str(
        item.get("website"),
        item.get("website_url"),
        item.get("homepage"),
        item.get("site"),
    )
    if url and not url.lower().startswith(("http://", "https://")):
        url = "https://" + url.lstrip("/")

    if not url:
        urls = item.get("urls")
        if isinstance(urls, dict):
            url = _first_str(urls.get("website"), urls.get("url"), urls.get("primary"))
            if url and not url.lower().startswith(("http://", "https://")):
                url = "https://" + url.lstrip("/")

    if not url:
        for k in ("link", "google_maps_url", "maps_url", "google_url", "share_link", "url"):
            v = item.get(k)
            if isinstance(v, str) and "maps" in v.lower() and v.strip().startswith("http"):
                url = v.strip()
                break

    if not url and not phone:
        token = _first_str(
            item.get("place_id"),
            item.get("google_id"),
            item.get("placeId"),
            item.get("googleId"),
        )
        if token:
            url = f"https://www.google.com/maps/search/?api=1&query_place_id={token}"

    return phone, url
