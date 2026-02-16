from __future__ import annotations

import math
import re
from datetime import date
from typing import Iterable, Optional

from dateutil import parser as date_parser


_MONEY_RE = re.compile(r"-?\d{1,3}(?:,\d{3})*(?:\.\d{2})?|-?\d+(?:\.\d{2})?")


def parse_money(value: str | None) -> Optional[float]:
    if not value:
        return None

    cleaned = value.strip()
    cleaned = cleaned.replace("£", "").replace("$", "").replace("€", "")
    cleaned = cleaned.replace(" ", "")

    match = _MONEY_RE.search(cleaned)
    if not match:
        return None

    number = match.group(0).replace(",", "")
    try:
        return float(number)
    except ValueError:
        return None


def parse_date(value: str | None, dayfirst: bool = True) -> Optional[date]:
    if not value:
        return None

    cleaned = value.strip()
    if not cleaned:
        return None

    try:
        parsed = date_parser.parse(cleaned, dayfirst=dayfirst)
    except (ValueError, TypeError, OverflowError):
        return None

    return parsed.date()


def approx_equal(left: float | None, right: float | None, tolerance: float = 0.02) -> bool:
    if left is None or right is None:
        return False

    if math.isclose(left, right, abs_tol=tolerance, rel_tol=0.0):
        return True

    return False


def first_match(patterns: Iterable[str], text: str, flags: int = 0) -> Optional[re.Match[str]]:
    for pattern in patterns:
        match = re.search(pattern, text, flags)
        if match:
            return match
    return None


# ---------------------------------------------------------------------------
# Shared postcode / ledger-account helpers
# ---------------------------------------------------------------------------

POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})\b",
    re.IGNORECASE,
)

LEDGER_MAP: dict[str, int] = {
    "CF10 1AE": 5001,
    "CF24 3LP": 5002,
    "CF11 9DX": 5004,
}


def normalize_postcode(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip().upper().replace(" ", "")
    if len(raw) < 5:
        return None
    return f"{raw[:-3]} {raw[-3:]}"


def extract_delivery_postcode(text: str) -> Optional[str]:
    """Find a UK postcode in *text*, preferring known store postcodes."""
    matches = POSTCODE_RE.findall(text or "")
    normalized = [normalize_postcode(f"{m[0]}{m[1]}") for m in matches]
    normalized = [pc for pc in normalized if pc is not None]
    # Prefer a postcode that maps to a known store / ledger account
    for pc in normalized:
        if pc in LEDGER_MAP:
            return pc
    # Fall back to first postcode found
    return normalized[0] if normalized else None
