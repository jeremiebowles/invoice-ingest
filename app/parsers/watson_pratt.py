from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"Invoice Number\s*([A-Z0-9-]+)", text, flags=re.IGNORECASE)
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


def _detect_delivery_vat_net(text: str) -> Optional[float]:
    if re.search(r"\\*Delivery Charge", text, flags=re.IGNORECASE):
        return 2.00
    return None


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

    delivery_vat_net = _detect_delivery_vat_net(text or "")
    if delivery_vat_net is None:
        delivery_vat_net = round(vat_amount / 0.20, 2) if vat_amount else 0.0

    vat_net = round(delivery_vat_net, 2)
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
