from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, extract_delivery_postcode, LEDGER_MAP

_YU_ENERGY_CONTACT_ID = "6be9c69483054490a0b5ed9bee2d2fc9"
_YU_ENERGY_LEDGER = 5004  # default; overridden by supply postcode if known


def _extract_invoice_number(text: str) -> Optional[str]:
    m = re.search(r"Invoice\s+Number\s*:\s*(\S+)", text, re.IGNORECASE)
    return m.group(1).strip() if m else None


def _extract_invoice_date(text: str) -> Optional[str]:
    m = re.search(r"Date\s+of\s+Invoice\s*:\s*(\d{2}/\d{2}/\d{4})", text, re.IGNORECASE)
    return m.group(1).strip() if m else None


def _extract_due_date(text: str) -> Optional[str]:
    # "£NNN will be collected on DD Mon YYYY by Direct Debit"
    m = re.search(r"collected\s+on\s+(\d{1,2}\s+\w+\s+\d{4})\s+by\s+Direct\s+Debit", text, re.IGNORECASE)
    return m.group(1).strip() if m else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    net = None
    total = None

    m = re.search(r"Electricity\s+Charges\s+For\s+This\s+Bill\s+[£$]?([\d,]+\.\d{2})", text, re.IGNORECASE)
    if m:
        net = parse_money(m.group(1))

    m = re.search(r"Total\s+Charges\s+For\s+This\s+Bill\s+[£$]?([\d,]+\.\d{2})", text, re.IGNORECASE)
    if m:
        total = parse_money(m.group(1))

    # Derive VAT from total - net rather than parsing the ambiguous VAT line
    vat = None
    if net is not None and total is not None:
        vat = round(total - net, 2)

    return net, vat, total


def parse_yu_energy(text: str) -> InvoiceData:
    warnings: list[str] = []

    invoice_number = _extract_invoice_number(text or "") or "UNKNOWN"

    invoice_date_str = _extract_invoice_date(text or "")
    invoice_date = parse_date(invoice_date_str)
    if not invoice_date:
        warnings.append("Invoice date not found")
        invoice_date = parse_date("01/01/1970")

    due_date_str = _extract_due_date(text or "")
    due_date = parse_date(due_date_str) if due_date_str else (invoice_date + timedelta(days=30) if invoice_date else None)

    postcode = extract_delivery_postcode(text or "")
    ledger_account = LEDGER_MAP.get(postcode or "", _YU_ENERGY_LEDGER)

    net, vat_amount, total = _extract_totals(text or "")

    if net is None:
        net = 0.0
        warnings.append("Net amount not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = round(net + vat_amount, 2)
        warnings.append("Total not found")

    if not approx_equal(net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Yu Energy",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id=_YU_ENERGY_CONTACT_ID,
        vat_net=round(net, 2),
        nonvat_net=0.0,
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
