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
