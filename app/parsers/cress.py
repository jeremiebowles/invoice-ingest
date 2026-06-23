from __future__ import annotations

import re
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    # "Invoice Number SI-00003058"
    match = re.search(r"Invoice\s+Number\s+(SI-\d+)", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    # "Invoice Date: 09/06/2026"
    match = re.search(r"Invoice\s+Date[:\s]+(\d{1,2}/\d{1,2}/\d{4})", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_due_date(text: str) -> Optional[str]:
    # "Due Date: 09/06/2026"
    match = re.search(r"Due\s+Date[:\s]+(\d{1,2}/\d{1,2}/\d{4})", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    # "Sub Total 521.26"
    m = re.search(r"Sub\s+Total\s+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
    vat_net = parse_money(m.group(1)) if m else None

    # "Tax Total 104.27"
    m = re.search(r"Tax\s+Total\s+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
    vat_amount = parse_money(m.group(1)) if m else None

    # "Total 625.53" — take the last occurrence to avoid matching sub-totals
    matches = list(re.finditer(r"(?<!\w)Total\s+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE))
    # Filter out Sub Total / Tax Total lines
    grand_matches = [m for m in matches if not re.search(r"(Sub|Tax)\s+Total", text[max(0, m.start()-10):m.start()], re.IGNORECASE)]
    total = parse_money(grand_matches[-1].group(1)) if grand_matches else None

    return vat_net, vat_amount, total


def parse_cress(text: str) -> InvoiceData:
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

    vat_net, vat_amount, total = _extract_totals(text or "")

    if vat_net is None:
        vat_net = 0.0
        warnings.append("Sub total (net) not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("Tax total (VAT) not found")
    if total is None:
        total = round(vat_net + vat_amount, 2)
        warnings.append("Invoice total not found; calculated from net + VAT")

    if not approx_equal(vat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Cress Ltd",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="a7d4240155bd4af682752c9707e702e4",
        vat_net=round(vat_net, 2),
        nonvat_net=0.0,
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
