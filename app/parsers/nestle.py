from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    """Extract 10-digit Nestle invoice number.

    The header line reads:
        TO AND INVOICE No. ON ALL 6873868 1337640820 379432906 ...
    Numbers are: SHIP-TO (7 digits), INVOICE NO (10 digits), SALES ORDER (9 digits).
    """
    match = re.search(r"ON ALL\s+\d+\s+(\d{10})\b", text, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    """Extract invoice / taxpoint date.

    pdfplumber puts the date on its own line below the header numbers:
        ... HSBC Bank PLC.
        20/11/2025
    """
    match = re.search(r"^\s*(\d{2}/\d{2}/\d{4})\s*$", text, re.MULTILINE)
    return match.group(1).strip() if match else None


def _extract_due_date(text: str) -> Optional[str]:
    match = re.search(
        r"PAYMENT DUE DATE:\s*(\d{1,2}/\d{1,2}/\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """Extract from the TOTALS summary line.

    pdfplumber renders the summary as:
        TOTALS 237.39 47.48 284.87
    which is: Value Excl VAT, VAT, Invoice Total.
    """
    match = re.search(r"TOTALS\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)", text, re.IGNORECASE)
    if match:
        net = parse_money(match.group(1))
        vat = parse_money(match.group(2))
        total = parse_money(match.group(3))
        return net, vat, total
    return None, None, None


def parse_nestle(text: str) -> InvoiceData:
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
        supplier="Nestle",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="5e9d739c44d64621898bd8b526a2d472",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
