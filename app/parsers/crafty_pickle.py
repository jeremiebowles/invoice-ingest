from __future__ import annotations

import re
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    # "INVOICE CPC139"
    match = re.search(r"INVOICE\s+(CPC\d+)", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    # "28 May 2026"
    match = re.search(r"\b(\d{1,2}\s+\w+\s+\d{4})\b", text or "")
    return match.group(1).strip() if match else None


def _extract_due_date(text: str) -> Optional[str]:
    # "Payment due by 27 June 2026"
    match = re.search(r"Payment due by\s+(\d{1,2}\s+\w+\s+\d{4})", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    # "Net Total 126.99"
    m = re.search(r"Net Total\s+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
    nonvat_net = parse_money(m.group(1)) if m else None

    # "VAT 0.00" — use word boundary and no colon to avoid matching "VAT: 354887354"
    m = re.search(r"\bVAT\s+([\d,]+\.\d{2})", text or "")
    vat_amount = parse_money(m.group(1)) if m else None

    # "GBP Total £126.99"
    m = re.search(r"GBP Total\s+£?([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
    total = parse_money(m.group(1)) if m else None

    return nonvat_net, vat_amount, total


def parse_crafty_pickle(text: str) -> InvoiceData:
    warnings: list[str] = []

    postcode = extract_delivery_postcode(text or "")
    ledger_account = LEDGER_MAP.get(postcode) if postcode else None
    if not postcode:
        warnings.append("Delivery postcode not found")
    elif ledger_account is None:
        warnings.append(f"Unknown delivery postcode: {postcode}")

    invoice_number = _extract_invoice_number(text or "") or "UNKNOWN"
    if invoice_number == "UNKNOWN":
        warnings.append("Invoice number not found")

    invoice_date_str = _extract_invoice_date(text or "")
    invoice_date = parse_date(invoice_date_str)
    if not invoice_date:
        warnings.append("Invoice date not found")
        invoice_date = parse_date("01/01/1970")

    due_date_str = _extract_due_date(text or "")
    due_date = parse_date(due_date_str) if due_date_str else None

    nonvat_net, vat_amount, total = _extract_totals(text or "")

    if nonvat_net is None:
        nonvat_net = 0.0
        warnings.append("Net total not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = round(nonvat_net + vat_amount, 2)
        warnings.append("Invoice total not found; calculated from net + VAT")

    if not approx_equal(nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="The Crafty Pickle Co.",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="df65f459f4804f5bb60f4622ad9447be",
        vat_net=0.0,
        nonvat_net=round(nonvat_net, 2),
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
