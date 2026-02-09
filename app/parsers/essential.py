from __future__ import annotations

import re
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, first_match, parse_date, parse_money


_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})\b", re.IGNORECASE)
_MONEY_RE = re.compile(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d{2})|[-+]?\d+(?:\.\d{2})")

_LEDGER_MAP = {
    "CF10 1AE": 5001,
    "CF24 3LP": 5002,
    "CF11 9DX": 5004,
}


def _normalize_postcode(raw: str) -> str:
    raw = raw.strip().upper().replace(" ", "")
    if len(raw) <= 3:
        return raw
    return f"{raw[:-3]} {raw[-3:]}"


def _extract_invoice_number(text: str) -> str:
    match = first_match([r"Invoice\s*No\.?\s*[:]?\s*([A-Z0-9\-/]+)"], text, flags=re.IGNORECASE)
    if match:
        return match.group(match.lastindex)
    return "UNKNOWN"


def _extract_invoice_date(text: str) -> Optional[str]:
    match = first_match([r"Invoice\s*Date\s*[:]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})"], text, flags=re.IGNORECASE)
    if match:
        return match.group(match.lastindex)
    return None


def _extract_postcode(text: str) -> Optional[str]:
    match = _POSTCODE_RE.search(text or "")
    if match:
        return _normalize_postcode(match.group(0))
    return None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    net = None
    vat = None
    total = None

    match = first_match([r"Order\s*Net\s*[:£]?\s*([0-9,]+\.\d{2})"], text, flags=re.IGNORECASE)
    if match:
        net = parse_money(match.group(1))
    match = first_match([r"VAT\s*[:£]?\s*([0-9,]+\.\d{2})"], text, flags=re.IGNORECASE)
    if match:
        vat = parse_money(match.group(1))
    match = first_match([r"Total\s*[:£]?\s*([0-9,]+\.\d{2})"], text, flags=re.IGNORECASE)
    if match:
        total = parse_money(match.group(1))
    return net, vat, total


def _extract_vat_analysis(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    lower = text or ""
    start = lower.find("Net (£)")
    if start == -1:
        return None, None, None
    end = lower.find("VAT (£)", start)
    if end == -1:
        return None, None, None
    net_block = lower[start:end]
    vat_block = lower[end:]
    vat_end = vat_block.find("VAT Analysis")
    if vat_end != -1:
        vat_block = vat_block[:vat_end]

    net_values = [parse_money(v) for v in _MONEY_RE.findall(net_block)]
    net_values = [v for v in net_values if v is not None]
    vat_values = [parse_money(v) for v in _MONEY_RE.findall(vat_block)]
    vat_values = [v for v in vat_values if v is not None]

    nonvat_net = net_values[0] if len(net_values) >= 1 else None
    vat_net = net_values[1] if len(net_values) >= 2 else None
    vat_amount = vat_values[1] if len(vat_values) >= 2 else None
    return vat_net, nonvat_net, vat_amount


def parse_essential(text: str) -> InvoiceData:
    invoice_number = _extract_invoice_number(text)
    date_text = _extract_invoice_date(text)
    invoice_date = parse_date(date_text, dayfirst=True)
    if not invoice_date:
        raise ValueError("Essential invoice date not found")

    postcode = _extract_postcode(text)
    ledger_account = _LEDGER_MAP.get(postcode) if postcode else None

    vat_net, nonvat_net, vat_amount = _extract_vat_analysis(text)
    order_net, order_vat, order_total = _extract_totals(text)

    warnings = []
    if vat_net is None or nonvat_net is None or vat_amount is None:
        warnings.append("VAT analysis missing; using order totals")
        vat_net = vat_net or 0.0
        nonvat_net = nonvat_net or 0.0
        vat_amount = vat_amount or order_vat or 0.0

    net_total = round((vat_net or 0.0) + (nonvat_net or 0.0), 2)
    total = order_total or round(net_total + (vat_amount or 0.0), 2)

    if order_net is not None and not approx_equal(order_net, net_total, tolerance=0.05):
        warnings.append("Order net does not match VAT analysis")
    if order_total is not None and not approx_equal(order_total, total, tolerance=0.05):
        warnings.append("Order total does not reconcile")

    if not postcode:
        warnings.append("Deliver to postcode not found")
    if not ledger_account:
        warnings.append("Ledger account not mapped for postcode")

    return InvoiceData(
        supplier="Essential Trading",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=None,
        description="Purchases",
        is_credit=False,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        vat_net=round(vat_net or 0.0, 2),
        nonvat_net=round(nonvat_net or 0.0, 2),
        vat_amount=round(vat_amount or 0.0, 2),
        total=round(total or 0.0, 2),
        warnings=warnings,
    )
