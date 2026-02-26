from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_invoice_number(text: str) -> Optional[str]:
    """Match the S-INV-NN-NNNNNN format directly."""
    match = re.search(r"(S-INV-\d{2}-\d{6})", text)
    return match.group(1) if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    """Invoice date appears immediately before the S-INV number on the data line."""
    match = re.search(r"(\d{2}/\d{2}/\d{4})\s+S-INV", text)
    return match.group(1) if match else None


def _extract_due_date(text: str) -> Optional[str]:
    """Payment due date appears immediately before the S-SHPT number on the data line."""
    match = re.search(r"(\d{2}/\d{2}/\d{4})\s+S-SHPT", text)
    return match.group(1) if match else None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    net_match = re.search(r"Net Total\s*£?([\d.,]+)", text, re.IGNORECASE)
    vat_match = re.search(r"^VAT\s*£?([\d.,]+)", text, re.IGNORECASE | re.MULTILINE)
    # Exclude "Sub Total" and "Net Total" - match standalone "Total"
    total_match = re.search(r"(?<!Sub )(?<!Net )Total\s*£?([\d.,]+)", text, re.IGNORECASE)
    net = parse_money(net_match.group(1)) if net_match else None
    vat = parse_money(vat_match.group(1)) if vat_match else None
    total = parse_money(total_match.group(1)) if total_match else None
    return net, vat, total


def _extract_vat_split(text: str) -> tuple[Optional[float], Optional[float]]:
    """Split net amount into VAT-rated and zero-rated from line items.

    Each line item ends with: ... %Disc VAT% NetAmount
    e.g. '... 27.5 20 3.62' or '... 27.5 0 4.59'
    """
    vat_net = 0.0
    nonvat_net = 0.0
    found = False
    for m in re.finditer(r"\b(0|20)\s+([\d.]+)\s*$", text, re.MULTILINE):
        vat_pct = int(m.group(1))
        amount = parse_money(m.group(2))
        if amount is None:
            continue
        found = True
        if vat_pct == 0:
            nonvat_net += amount
        else:
            vat_net += amount
    if not found:
        return None, None
    return round(vat_net, 2), round(nonvat_net, 2)


def parse_natures_aid(text: str) -> InvoiceData:
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
    split_vat_net, split_nonvat_net = _extract_vat_split(text or "")
    if vat_net is None:
        warnings.append("VAT net amount not found")
        vat_net = 0.0
    if vat_amount is None:
        warnings.append("VAT amount not found")
        vat_amount = 0.0
    if total is None:
        total = round(vat_net + vat_amount, 2)
        warnings.append("Total amount not found")

    if split_vat_net is not None and split_nonvat_net is not None:
        split_total = round(split_vat_net + split_nonvat_net, 2)
        if vat_net is not None and not approx_equal(split_total, vat_net):
            # Line item values are pre-discount; use Net Total as authoritative
            # and apply split ratio to determine VAT vs zero-rated breakdown.
            net_total = vat_net
            if split_total > 0:
                vat_ratio = split_vat_net / split_total
                vat_net = round(net_total * vat_ratio, 2)
                nonvat_net = round(net_total - vat_net, 2)
            else:
                vat_net = net_total
                nonvat_net = 0.0
        else:
            vat_net = split_vat_net
            nonvat_net = split_nonvat_net
    else:
        nonvat_net = round(max(total - vat_net - vat_amount, 0.0), 2)

    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Natures Aid",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="4aef1b19bf73426fb4e52a5a803277e9",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
