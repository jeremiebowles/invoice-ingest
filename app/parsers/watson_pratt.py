from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    # Watson & Pratt invoice numbers are IN-NNNNNN format.
    # Generic [A-Z0-9-]+ after "Invoice Number" grabs the postcode (CF11)
    # from the merged two-column layout, so match the specific format.
    match = re.search(r"(IN-\d+)", text)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(r"Invoice Date\s*([0-9]{1,2}\s+[A-Za-z]{3}\s+[0-9]{4})", text)
    return match.group(1).strip() if match else None


def _extract_due_date(text: str) -> Optional[str]:
    match = re.search(r"Due Date:\s*([0-9]{1,2}\s+[A-Za-z]{3}\s+[0-9]{4})", text)
    return match.group(1).strip() if match else None


def _extract_subtotal(text: str) -> Optional[float]:
    match = re.search(r"Subtotal\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    return parse_money(match.group(1)) if match else None


def _extract_vat_amount(text: str) -> Optional[float]:
    match = re.search(r"Total VAT\s*20%?\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    return parse_money(match.group(1)) if match else None


def _extract_total(text: str) -> Optional[float]:
    match = re.search(r"Invoice Total GBP\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    return parse_money(match.group(1)) if match else None


def _extract_vat_net(text: str) -> float:
    """Sum all 20%-rated line item amounts (typically just delivery charge).

    Line format: '*Delivery Charge 1.00 2.50 20% 2.50'
    We match '20% <amount>' in the line-items section (before Subtotal).
    """
    parts = re.split(r"Subtotal", text, flags=re.IGNORECASE)
    items_text = parts[0] if parts else text
    total = 0.0
    for m in re.finditer(r"20%\s+([\d.,]+)", items_text):
        val = parse_money(m.group(1))
        if val is not None:
            total += val
    return round(total, 2)


def parse_watson_pratt(text: str) -> InvoiceData:
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

    subtotal = _extract_subtotal(text or "")
    vat_amount = _extract_vat_amount(text or "")
    total = _extract_total(text or "")

    if subtotal is None:
        warnings.append("Subtotal not found")
        subtotal = 0.0
    if vat_amount is None:
        warnings.append("VAT amount not found")
        vat_amount = 0.0
    if total is None:
        total = round(subtotal + vat_amount, 2)
        warnings.append("Total amount not found")

    vat_net = _extract_vat_net(text or "")
    nonvat_net = round(max(subtotal - vat_net, 0.0), 2)

    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Watson & Pratt",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="de28527c07f842b3a12fd9d4298f7055",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
