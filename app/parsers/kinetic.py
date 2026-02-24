from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    # Header and value are on separate lines; match the SIN prefix directly.
    match = re.search(r"\b(SIN\d{5,10})\b", text or "")
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    # "Invoice Date ... 24/02/2026" — date is on the same line as other headers,
    # so grab the first DD/MM/YYYY pattern after the label
    match = re.search(r"Invoice Date\b.{0,80}?(\d{2}/\d{2}/\d{4})", text or "", flags=re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    vat_net = None
    vat_amount = None
    total = None

    # "Net Total GBP 162.38"
    m = re.search(r"Net Total GBP\s+([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        vat_net = parse_money(m.group(1))

    # "VAT GBP 32.47"
    m = re.search(r"VAT GBP\s+([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        vat_amount = parse_money(m.group(1))

    # "Total GBP 194.85" — avoid matching "Net Total GBP"
    m = re.search(r"(?<!Net )Total GBP\s+([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        total = parse_money(m.group(1))

    return vat_net, vat_amount, total


def parse_kinetic(text: str) -> InvoiceData:
    warnings: list[str] = []

    postcode = extract_delivery_postcode(text or "")
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

    vat_net, vat_amount, total = _extract_totals(text or "")

    if vat_net is None:
        vat_net = 0.0
        warnings.append("Net total not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = round(vat_net + vat_amount, 2)
        warnings.append("Invoice total not found")

    if not approx_equal(vat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Kinetic Enterprises Ltd",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="3861e92a06c54fa489b540c6f9673aab",
        vat_net=round(vat_net, 2),
        nonvat_net=0.0,
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
