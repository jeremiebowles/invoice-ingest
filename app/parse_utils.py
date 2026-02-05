# app/parse_utils.py
from __future__ import annotations

import re
from datetime import date
from dateutil import parser as dtparser


_money_re = re.compile(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d{2})|[-+]?\d+(?:\.\d{2})")


def parse_money(s: str) -> float:
    """
    Accepts '1,234.56' or '1234.56' etc.
    Returns float (good enough for invoices; you can swap to Decimal later).
    """
    s = s.strip()
    m = _money_re.search(s.replace("£", "").replace("GBP", "").strip())
    if not m:
        raise ValueError(f"Could not parse money from: {s!r}")
    return float(m.group(0).replace(",", ""))


def parse_date(s: str) -> date:
    """
    Tolerant date parse e.g. '05/02/2026', '5 Feb 2026', etc.
    """
    return dtparser.parse(s, dayfirst=True).date()


def approx_equal(a: float, b: float, tol: float = 0.02) -> bool:
    return abs(a - b) <= tol
