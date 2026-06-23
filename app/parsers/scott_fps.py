from __future__ import annotations

import re
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"\b(INV-\d+)\b", text)
    return match.group(1) if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    # Header section: "InvoiceDate ScottFPSLimited\n9Apr2026"
    match = re.search(r"InvoiceDate\s+\S+\s+(\d{1,2}[A-Za-z]{3}\d{4})", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1)
    # Fallback: payment advice section "DueDate 9May2026" — not the invoice date, ignore
    return None


def _extract_due_date(text: str) -> Optional[str]:
    match = re.search(r"Due Date:\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    net_match = re.search(r"Subtotal\s+([\d.,]+)", text, re.IGNORECASE)
    vat_match = re.search(r"TOTAL VAT\s+\d+%\s+([\d.,]+)", text, re.IGNORECASE)
    total_match = re.search(r"TOTALGBP\s+([\d.,]+)", text, re.IGNORECASE)
    net = parse_money(net_match.group(1)) if net_match else None
    vat = parse_money(vat_match.group(1)) if vat_match else None
    total = parse_money(total_match.group(1)) if total_match else None
    return net, vat, total


def parse_scott_fps(text: str) -> InvoiceData:
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
        supplier="Scott FPS",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="122090b8486145cfb305c53fde2340f8",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
