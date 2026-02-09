from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal

_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*([0-9][A-Z]{2})\b",
    re.IGNORECASE,
)

_LEDGER_MAP = {
    "CF10 1AE": 5001,
    "CF24 3LP": 5002,
    "CF11 9DX": 5004,
}


def _normalize_postcode(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip().upper().replace(" ", "")
    if len(raw) < 5:
        return None
    return f"{raw[:-3]} {raw[-3:]}"


def _extract_postcode(text: str) -> Optional[str]:
    match = _POSTCODE_RE.search(text or "")
    if not match:
        return None
    return _normalize_postcode(match.group(0))


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+no\s*([A-Z0-9-]+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(r"Invoice date\s*([0-9]{1,2}/[A-Za-z]{3}/[0-9]{4})", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_due_date(text: str) -> Optional[str]:
    match = re.search(r"Due date\s*([0-9]{1,2}/[A-Za-z]{3}/[0-9]{4})", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_total(text: str) -> Optional[float]:
    match = re.search(r"Total\s*£?\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    return parse_money(match.group(1)) if match else None


def parse_tonyrefail(text: str) -> InvoiceData:
    warnings: list[str] = []

    postcode = _extract_postcode(text or "")
    ledger_account = _LEDGER_MAP.get(postcode) if postcode else None
    if not postcode:
        warnings.append("Deliver To postcode not found")
    elif ledger_account is None:
        warnings.append(f"Unknown Deliver To postcode: {postcode}")

    invoice_number = _extract_invoice_number(text or "") or "UNKNOWN"
    invoice_date_str = _extract_invoice_date(text or "")
    invoice_date = parse_date(invoice_date_str)
    if not invoice_date:
        warnings.append("Invoice date not found")
        invoice_date = parse_date("01/01/1970")

    due_date_str = _extract_due_date(text or "")
    due_date = parse_date(due_date_str) if due_date_str else None
    if due_date is None and invoice_date:
        due_date = invoice_date + timedelta(days=30)

    total = _extract_total(text or "")
    if total is None:
        warnings.append("Total amount not found")
        total = 0.0

    vat_net = 0.0
    vat_amount = 0.0
    nonvat_net = round(total, 2)

    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Tonyrefail Apiary",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="92cbebef85424457befc01b894ea8cf0",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
