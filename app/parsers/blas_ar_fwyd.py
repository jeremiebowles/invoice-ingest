from __future__ import annotations

import calendar
import re
from datetime import date, timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, parse_date, parse_money, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"Invoice No[:\s]+(\d+)", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(r"Invoice Date[:\s]+(\d{2}/\d{2}/\d{4})", text or "", flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_delivery_postcode(text: str) -> Optional[str]:
    """
    The three-column PDF layout is read left-to-right, so the billing postcode
    (CF11 9DX) lands before the delivery postcode in extracted text.  The
    delivery postcode always appears immediately before "Payment Terms" in the
    interleaved output, so target that anchor.
    """
    from app.parse_utils import normalize_postcode, POSTCODE_RE
    m = re.search(
        r"([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\s*(?:\n\s*)?(?:United Kingdom\s*(?:\n\s*)?)?Payment Terms",
        text or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        pc = normalize_postcode(m.group(1))
        if pc:
            return pc
    # Fallback to generic extraction
    return extract_delivery_postcode(text or "")


def _calculate_due_date(invoice_date: date) -> date:
    """Payment terms: 15 days from end of invoice month."""
    last_day = calendar.monthrange(invoice_date.year, invoice_date.month)[1]
    return date(invoice_date.year, invoice_date.month, last_day) + timedelta(days=15)


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    # "Archeb/Order Net: 191.22" — total net (0% + 20% items combined)
    m = re.search(r"Archeb/Order Net[:\s]+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
    order_net = parse_money(m.group(1)) if m else None

    # "TAW / VAT: 8.28"
    m = re.search(r"TAW\s*/\s*VAT[:\s]+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
    vat_amount = parse_money(m.group(1)) if m else None

    # "Cyfanswm/Total: 199.50"
    m = re.search(r"Cyfanswm/Total[:\s]+([\d,]+\.\d{2})", text or "", flags=re.IGNORECASE)
    total = parse_money(m.group(1)) if m else None

    # Derive vat_net (20% items only) and nonvat_net from order_net
    if order_net is not None and vat_amount is not None:
        vat_net = round(vat_amount / 0.20, 2)
        nonvat_net = round(order_net - vat_net, 2)
    else:
        vat_net = None
        nonvat_net = None

    return order_net, vat_net, nonvat_net, vat_amount, total


def parse_blas_ar_fwyd(text: str) -> InvoiceData:
    warnings: list[str] = []

    postcode = _extract_delivery_postcode(text or "")
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

    due_date = _calculate_due_date(invoice_date) if invoice_date else None

    order_net, vat_net, nonvat_net, vat_amount, total = _extract_totals(text or "")

    if vat_net is None:
        vat_net = 0.0
        warnings.append("VAT net could not be derived")
    if nonvat_net is None:
        nonvat_net = 0.0
        warnings.append("Non-VAT net could not be derived")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = round(vat_net + nonvat_net + vat_amount, 2)
        warnings.append("Invoice total not found; calculated from net + VAT")

    if nonvat_net < 0:
        warnings.append(f"Derived non-VAT net is negative ({nonvat_net}); check VAT rate assumption")
        nonvat_net = 0.0

    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (vat_net + nonvat_net + vat != total)")

    return InvoiceData(
        supplier="Blas ar Fwyd",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="5315cf7f3e18468aa580df1cfa50ed76",
        vat_net=round(vat_net, 2),
        nonvat_net=round(nonvat_net, 2),
        vat_amount=round(vat_amount, 2),
        total=round(total, 2),
        warnings=warnings,
        is_credit=False,
    )
