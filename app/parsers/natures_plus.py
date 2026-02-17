from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_ship_to_postcode(text: str) -> Optional[str]:
    """Extract postcode from Ship To block, not Bill To.

    The invoice has both Bill To (CF10 1AE) and Ship To (CF24 3LP) addresses.
    extract_delivery_postcode picks the first LEDGER_MAP match (Bill To),
    so we extract the Ship To section explicitly.
    """
    match = re.search(r"Ship To(.+?)(?:Invoice Details|Item\s+Material)", text, re.IGNORECASE | re.DOTALL)
    if match:
        ship_block = match.group(1)
        pc = extract_delivery_postcode(ship_block)
        if pc:
            return pc
    return extract_delivery_postcode(text or "")


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"Invoice Number\s*([0-9-]+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(
        r"Invoice Date\s*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})",
        text,
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    net_match = re.search(r"Total Net Ext\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    vat_match = re.search(r"Total Tax/VAT\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    total_match = re.search(r"Grand Total\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    net = parse_money(net_match.group(1)) if net_match else None
    vat = parse_money(vat_match.group(1)) if vat_match else None
    total = parse_money(total_match.group(1)) if total_match else None
    return net, vat, total


def parse_natures_plus(text: str) -> InvoiceData:
    warnings: list[str] = []

    postcode = _extract_ship_to_postcode(text or "")
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
        supplier="Natures Plus",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="011268104d684327bb5707c8886e7339",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
