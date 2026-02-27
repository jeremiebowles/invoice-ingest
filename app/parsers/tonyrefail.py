from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    # OCR splits label from value across lines; match the IN-prefixed number directly.
    match = re.search(r"\b(IN\d{4,10})\b", text or "")
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    # OCR column-reads the table so label and value are not adjacent.
    # The DD/Mon/YYYY format is distinctive enough to match anywhere.
    match = re.search(r"\b(\d{1,2}/[A-Za-z]{3}/\d{4})\b", text or "")
    return match.group(1).strip() if match else None


def _extract_due_date(text: str) -> Optional[str]:
    match = re.search(r"Due date\s*([0-9]{1,2}/[A-Za-z]{3}/[0-9]{4})", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_total(text: str) -> Optional[float]:
    # "Balance due £468.00" is the primary pattern; fall back to generic "Total £..."
    match = re.search(r"Balance due\s*£?\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"(?<!Sub )Total\s*£?\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    return parse_money(match.group(1)) if match else None


def parse_tonyrefail(text: str) -> InvoiceData:
    warnings: list[str] = []

    postcode = extract_delivery_postcode(text or "")
    ledger_account = LEDGER_MAP.get(postcode) if postcode else None
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
