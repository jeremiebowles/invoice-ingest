from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, LEDGER_MAP, POSTCODE_RE, normalize_postcode


def _extract_delivery_postcode(text: str) -> Optional[str]:
    """Extract postcode from the Delivery Address section specifically."""
    # Find text after "Delivery Address" label and search there first
    m = re.search(r"Delivery Address\b(.+)", text or "", flags=re.IGNORECASE | re.DOTALL)
    if m:
        delivery_section = m.group(1)
        pc_match = POSTCODE_RE.search(delivery_section)
        if pc_match:
            pc = normalize_postcode(f"{pc_match.group(1)}{pc_match.group(2)}")
            if pc:
                return pc
    # Fall back to first known postcode in full text
    for m2 in POSTCODE_RE.finditer(text or ""):
        pc = normalize_postcode(f"{m2.group(1)}{m2.group(2)}")
        if pc and pc in LEDGER_MAP:
            return pc
    return None


def _extract_invoice_number(text: str) -> Optional[str]:
    # "Invoice No.BC01459342" (no space between label and value in extracted text)
    match = re.search(r"Invoice No\.?\s*(BC\d{6,12})", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    # "Invoice Date 16. January 2026"
    match = re.search(
        r"Invoice Date\s+(\d{1,2}\.\s+\w+\s+\d{4})",
        text or "",
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    vat_net = None
    nonvat_net = None
    vat_amount = None
    total = None

    # "GOODS SUBTOTAL 132.96" — net after discount, VAT-applicable goods
    m = re.search(r"GOODS SUBTOTAL\s+([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        vat_net = parse_money(m.group(1))

    # "TOTAL GOODS (0% VAT) 0.00"
    m = re.search(r"TOTAL GOODS\s*\(0%\s*VAT\)\s+([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        nonvat_net = parse_money(m.group(1))

    # "TOTAL VAT 26.59"
    m = re.search(r"TOTAL VAT\s+([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        vat_amount = parse_money(m.group(1))

    # "INVOICE TOTAL 159.55"
    m = re.search(r"INVOICE TOTAL\s+([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        total = parse_money(m.group(1))

    return vat_net, nonvat_net, vat_amount, total


def parse_biocare(text: str) -> InvoiceData:
    warnings: list[str] = []

    postcode = _extract_delivery_postcode(text or "")
    ledger_account = LEDGER_MAP.get(postcode) if postcode else None
    if not postcode:
        warnings.append("Delivery postcode not found")
    elif ledger_account is None:
        warnings.append(f"Unknown delivery postcode: {postcode}")

    invoice_number = _extract_invoice_number(text or "") or "UNKNOWN"
    invoice_date_str = _extract_invoice_date(text or "")
    invoice_date = parse_date(invoice_date_str)
    if not invoice_date:
        warnings.append("Invoice date not found")
        invoice_date = parse_date("01/01/1970")

    due_date = invoice_date + timedelta(days=30) if invoice_date else None

    vat_net, nonvat_net, vat_amount, total = _extract_totals(text or "")

    if vat_net is None:
        vat_net = 0.0
        warnings.append("VAT net (GOODS SUBTOTAL) not found")
    if nonvat_net is None:
        nonvat_net = 0.0
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = round(vat_net + nonvat_net + vat_amount, 2)
        warnings.append("Invoice total not found")

    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="BioCare",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="b2be8e123f9446dab93d719c6a69ec05",
        vat_net=round(vat_net, 2),
        nonvat_net=round(nonvat_net, 2),
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
