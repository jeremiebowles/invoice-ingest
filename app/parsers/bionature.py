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
    match = re.search(r"Invoice\s+No\s*([0-9]+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+Date\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    net_match = re.search(r"Total Net Amount\s*£?\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    vat_match = re.search(r"Total Tax Amount\s*£?\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    total_match = re.search(r"Invoice Total\s*£?\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    net = parse_money(net_match.group(1)) if net_match else None
    vat = parse_money(vat_match.group(1)) if vat_match else None
    total = parse_money(total_match.group(1)) if total_match else None
    return net, vat, total


def parse_bionature(text: str) -> InvoiceData:
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

    due_date = invoice_date + timedelta(days=30) if invoice_date else None

    vat_net, vat_amount, total = _extract_totals(text or "")
    if vat_net is None:
        warnings.append("VAT net amount not found")
        vat_net = 0.0
    if vat_amount is None:
        warnings.append("VAT amount not found")
        vat_amount = 0.0
    if total is None:
        total = round(vat_net + vat_amount, 2)
        warnings.append("Total amount not found")

    nonvat_net = round(max(total - vat_net - vat_amount, 0.0), 2)
    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Bio-Nature",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="a86c0c45c364492394f743bc09db5c90",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
