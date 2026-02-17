from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+No:\s*([0-9-]+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(r"Date:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_zero_and_standard_nets(text: str) -> tuple[Optional[float], Optional[float]]:
    zero_net = None
    standard_net = None
    # "Zero Rated 6.55" or "Zero Rated\n6.55"
    m = re.search(r"Zero\s+Rated\s+([\d.,]+)", text, re.IGNORECASE)
    if m:
        zero_net = parse_money(m.group(1))
    # "20% 161.74" in the VAT NET summary (not the line-item VAT Rate column)
    # Look for "20%" followed by a number in the VAT NET block
    for m in re.finditer(r"^20%\s+([\d.,]+)", text, re.MULTILINE):
        val = parse_money(m.group(1))
        if val is not None:
            standard_net = val
    return zero_net, standard_net


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    sub_total = None
    vat_amount = None
    total = None
    m = re.search(r"Sub\s*Total:\s*([\d.,]+)", text, re.IGNORECASE)
    if m:
        sub_total = parse_money(m.group(1))
    m = re.search(r"(?<!\w)VAT:\s*([\d.,]+)", text, re.IGNORECASE)
    if m:
        vat_amount = parse_money(m.group(1))
    m = re.search(r"(?<!Sub )Total:\s*([\d.,]+)", text, re.IGNORECASE)
    if m:
        total = parse_money(m.group(1))
    return sub_total, vat_amount, total


def parse_avogel(text: str) -> InvoiceData:
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

    due_date = invoice_date + timedelta(days=30) if invoice_date else None

    zero_net, standard_net = _extract_zero_and_standard_nets(text or "")
    sub_total, vat_amount, total = _extract_totals(text or "")

    if zero_net is None:
        zero_net = 0.0
    if standard_net is None:
        standard_net = 0.0
        warnings.append("VAT net amount not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None and sub_total is not None:
        total = round(sub_total + vat_amount, 2)
    if total is None:
        total = round(standard_net + zero_net + vat_amount, 2)
        warnings.append("Total amount not found")

    vat_net = round(standard_net, 2)
    nonvat_net = round(zero_net, 2)

    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="A.Vogel",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="1cc12fd2293c4eb48365ed85ccb5f2f6",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
