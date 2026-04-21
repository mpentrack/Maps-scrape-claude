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

_cache: dict[str, "ZipCoords | None"] = {}
_lock = threading.Lock()

_ZIP_USER_AGENT = "Maps-scrape-claude/1.0 (https://github.com/mpentrack/Maps-scrape-claude)"

# Approximate center coords for each US ZIP first-digit region.
# Used as fallback when zippopotam.us is unreachable so the Maps API
# always gets a US-based coordinate instead of defaulting to Europe.
_ZIP_REGION_FALLBACK: dict[str, tuple[float, float]] = {
    "0": (41.8, -72.7),   # New England (CT, MA, ME, NH, RI, VT)
    "1": (41.2, -74.5),   # NY, NJ, PA
    "2": (38.0, -77.5),   # DC, DE, MD, NC, SC, VA, WV
    "3": (32.8, -86.8),   # AL, FL, GA, MS, TN
    "4": (41.0, -83.5),   # IN, KY, MI, OH
    "5": (44.2, -93.1),   # IA, MN, MT, ND, SD, WI
    "6": (39.8, -91.5),   # IL, KS, MO, NE
    "7": (31.9, -97.1),   # AR, LA, OK, TX
    "8": (38.0, -108.5),  # AZ, CO, ID, NM, NV, UT, WY
    "9": (37.8, -120.5),  # AK, CA, HI, OR, WA
}


class ZipCoords(tuple):
    """(lat, lng) tuple that also carries an `exact` flag.

    exact=True  → from zippopotam.us centroid; safe to use with zoom=13.
    exact=False → regional fallback; tells the API we're in the US but
                  should NOT be combined with a tight zoom level.
    """
    exact: bool

    def __new__(cls, lat: float, lng: float, *, exact: bool) -> "ZipCoords":
        obj = super().__new__(cls, (lat, lng))
        obj.exact = exact
        return obj

    def __repr__(self) -> str:
        return f"ZipCoords({self[0]}, {self[1]}, exact={self.exact})"


def us_zip_latlng(zip_code: str | None) -> "ZipCoords | None":
    """Return (lat, lng) for a US ZIP, or None for an invalid ZIP.

    Tries zippopotam.us for an exact centroid (exact=True), then falls
    back to a rough regional coordinate (exact=False).  Always returns
    *something* for a valid US ZIP so the Maps API search stays in the
    correct US region rather than defaulting to Europe.
    """
    z = normalize_zip5(zip_code or "")
    if not z:
        return None
    with _lock:
        if z in _cache:
            return _cache[z]
    result: ZipCoords | None = None
    try:
        resp = requests.get(
            f"https://api.zippopotam.us/us/{z}",
            timeout=12,
            headers={"User-Agent": _ZIP_USER_AGENT},
        )
        if resp.status_code == 200:
            data: Any = resp.json()
            places = data.get("places") if isinstance(data, dict) else None
            if isinstance(places, list) and places and isinstance(places[0], dict):
                p0 = places[0]
                lat_s, lng_s = p0.get("latitude"), p0.get("longitude")
                if lat_s is not None and lng_s is not None:
                    result = ZipCoords(float(lat_s), float(lng_s), exact=True)
        else:
            log.debug("zippopotam HTTP %s for zip %s", resp.status_code, z)
    except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
        log.debug("zip geocode failed for %s: %s", z, exc)
    # Fall back to regional approximate center so the Maps API stays in the US.
    if result is None:
        fallback = _ZIP_REGION_FALLBACK.get(z[0])
        if fallback:
            result = ZipCoords(fallback[0], fallback[1], exact=False)
            log.debug("Using regional fallback coords for zip %s: %s", z, result)
    with _lock:
        _cache[z] = result
    return result
