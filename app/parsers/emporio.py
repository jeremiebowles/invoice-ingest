from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+No\s+(\d+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+Date\s+(\d{1,2}/\d{1,2}/\d{4})", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    net = None
    vat = None
    total = None
    m = re.search(r"Total\s+Net\s+Amount\s+[£$]?\s*([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        net = parse_money(m.group(1))
    m = re.search(r"Total\s+VAT\s+Amount\s+[£$]?\s*([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        vat = parse_money(m.group(1))
    m = re.search(r"Invoice\s+Total\s+[£$]?\s*([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        total = parse_money(m.group(1))
    return net, vat, total


def parse_emporio(text: str) -> InvoiceData:
    warnings: list[str] = []

    postcode = extract_delivery_postcode(text or "")
    ledger_account = LEDGER_MAP.get(postcode) if postcode else None
    if not postcode:
        warnings.append("Postcode not found")
    elif ledger_account is None:
        warnings.append(f"Unknown postcode: {postcode}")

    invoice_number = _extract_invoice_number(text or "") or "UNKNOWN"
    invoice_date_str = _extract_invoice_date(text or "")
    invoice_date = parse_date(invoice_date_str)
    if not invoice_date:
        warnings.append("Invoice date not found")
        invoice_date = parse_date("01/01/1970")

    due_date = invoice_date + timedelta(days=30) if invoice_date else None

    net, vat_amount, total = _extract_totals(text or "")

    if net is None:
        net = 0.0
        warnings.append("Net amount not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = round(net + vat_amount, 2)
        warnings.append("Invoice total not found")

    if not approx_equal(net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Emporio",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="ba1633b010f64a1cbfb264179760fa12",
        vat_net=round(net, 2),
        nonvat_net=0.0,
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
