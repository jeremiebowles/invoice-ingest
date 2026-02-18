from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, extract_delivery_postcode, LEDGER_MAP

# Maps keywords in Customer Ref to known store postcodes
_CUSTOMER_REF_POSTCODE: list[tuple[str, str]] = [
    ("royal arcade", "CF10 1AE"),
    ("canton", "CF11 9DX"),
    ("roath", "CF24 3LP"),
    ("albany", "CF24 3LP"),
]


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+Number\s*[:\-]?\s*(\d+)", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+Date\s*[:\-]?\s*(.+)", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_customer_ref(text: str) -> Optional[str]:
    match = re.search(r"Customer\s+Ref\s*[:\-]?\s*(.+)", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _postcode_from_customer_ref(customer_ref: str) -> Optional[str]:
    normalized = (customer_ref or "").lower()
    for keyword, postcode in _CUSTOMER_REF_POSTCODE:
        if keyword in normalized:
            return postcode
    return None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    net = None
    vat = None
    total = None
    m = re.search(r"NETT\s+[£$]?([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        net = parse_money(m.group(1))
    m = re.search(r"VAT\s*\([^)]*\)\s+[£$]?([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        vat = parse_money(m.group(1))
    m = re.search(r"TOTAL\s+[£$]?([\d,]+\.\d{2})", text, flags=re.IGNORECASE)
    if m:
        total = parse_money(m.group(1))
    return net, vat, total


def parse_pestokill(text: str) -> InvoiceData:
    warnings: list[str] = []

    customer_ref = _extract_customer_ref(text or "")
    postcode = _postcode_from_customer_ref(customer_ref or "")

    # Fallback: scan text for known store postcodes
    if not postcode:
        postcode = extract_delivery_postcode(text or "")

    ledger_account = LEDGER_MAP.get(postcode) if postcode else None
    if not postcode:
        warnings.append("Store not identified from Customer Ref or postcode")
    elif ledger_account is None:
        warnings.append(f"Unknown postcode: {postcode}")

    invoice_number = _extract_invoice_number(text or "") or "UNKNOWN"
    invoice_date_str = _extract_invoice_date(text or "")
    invoice_date = parse_date(invoice_date_str)
    if not invoice_date:
        warnings.append("Invoice date not found")
        invoice_date = parse_date("01/01/1970")

    due_date = invoice_date + timedelta(days=30) if invoice_date else None

    net, vat_amount, total = _extract_totals(text or "")

    if net is None:
        net = 0.0
        warnings.append("Net amount not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = round(net + vat_amount, 2)
        warnings.append("Total not found")

    if not approx_equal(net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Pestokill",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="38ca157c1564493a98726455872be080",
        vat_net=round(net, 2),
        nonvat_net=0.0,
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
