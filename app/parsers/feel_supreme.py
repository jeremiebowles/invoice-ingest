from __future__ import annotations

import re
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    # "Vat Invoice PO49799"
    match = re.search(r"Vat Invoice\s+((?:PO|INV|CR)\d+)", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    # Columns are interleaved in extracted text:
    # "DATE PLEASE PAY DUE DATE\n8 Royal Arcade ...\n20/05/2026 £358.20 19/06/2026"
    # Invoice date is the first DD/MM/YYYY immediately before the £ amount.
    match = re.search(r"(\d{2}/\d{2}/\d{4})\s+£[\d,]+\.\d{2}", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_due_date(text: str) -> Optional[str]:
    # Due date is the first DD/MM/YYYY immediately after the £ amount.
    match = re.search(r"£[\d,]+\.\d{2}\s+(\d{2}/\d{2}/\d{4})", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    vat_net = None
    vat_amount = None
    total = None

    # VAT SUMMARY table: "VAT @ 20% 59.70 298.50" (VAT then NET)
    m = re.search(r"VAT @ 20%\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
    if m:
        vat_amount = parse_money(m.group(1))
        vat_net = parse_money(m.group(2))

    # Fallback: "SUBTOTAL 298.50"
    if vat_net is None:
        m = re.search(r"^SUBTOTAL\s+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE | re.MULTILINE)
        if m:
            vat_net = parse_money(m.group(1))

    # Fallback: "VAT TOTAL 59.70"
    if vat_amount is None:
        m = re.search(r"^VAT TOTAL\s+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE | re.MULTILINE)
        if m:
            vat_amount = parse_money(m.group(1))

    # "TOTAL 358.20" anchored to line start to avoid SUBTOTAL / VAT TOTAL
    m = re.search(r"^TOTAL\s+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE | re.MULTILINE)
    if m:
        total = parse_money(m.group(1))
    else:
        # Fallback: "TOTAL DUE £358.20"
        m = re.search(r"TOTAL DUE\s+£?([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
        if m:
            total = parse_money(m.group(1))

    return vat_net, vat_amount, total


def parse_feel_supreme(text: str) -> InvoiceData:
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
        warnings.append("Net total not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = round(vat_net + vat_amount, 2)
        warnings.append("Invoice total not found; calculated from net + VAT")

    if not approx_equal(vat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Feel Supreme",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="e0b1d25cd0f14a6d90f74b913d5f61ba",
        vat_net=round(vat_net, 2),
        nonvat_net=0.0,
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
