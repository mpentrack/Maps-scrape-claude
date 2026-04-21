"""
Approximate lat/lng for a US ZIP (5-digit). Used to scope Maps Data API search.

The vendor Search endpoint defaults lat/lng to Europe when omitted, which yields
empty or irrelevant US results when only `query` + `zipcode` are sent.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

import requests

from geo_zip import normalize_zip5

log = logging.getLogger(__name__)

_cache: dict[str, tuple[float, float] | None] = {}
_lock = threading.Lock()

_ZIP_USER_AGENT = "Maps-scrape-claude/1.0 (https://github.com/mpentrack/Maps-scrape-claude)"


def us_zip_latlng(zip_code: str | None) -> tuple[float, float] | None:
    """Return (lat, lng) for a US ZIP, or None if lookup fails."""
    z = normalize_zip5(zip_code or "")
    if not z:
        return None
    with _lock:
        if z in _cache:
            return _cache[z]
    latlng: tuple[float, float] | None = None
    try:
        resp = requests.get(
            f"https://api.zippopotam.us/us/{z}",
            timeout=12,
            headers={"User-Agent": _ZIP_USER_AGENT},
        )
        if resp.status_code != 200:
            log.debug("zippopotam HTTP %s for zip %s", resp.status_code, z)
        else:
            data: Any = resp.json()
            places = data.get("places") if isinstance(data, dict) else None
            if isinstance(places, list) and places and isinstance(places[0], dict):
                p0 = places[0]
                lat_s, lng_s = p0.get("latitude"), p0.get("longitude")
                if lat_s is not None and lng_s is not None:
                    latlng = (float(lat_s), float(lng_s))
    except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
        log.debug("zip geocode failed for %s: %s", z, exc)
    with _lock:
        _cache[z] = latlng
    return latlng
