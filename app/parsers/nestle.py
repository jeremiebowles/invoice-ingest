from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal

_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*([0-9][A-Z]{2})\b",
    re.IGNORECASE,
)

_LEDGER_MAP = {
    "CF10 1AE": 5001,
    "CF24 3LP": 5002,
    "CF11 9DX": 5004,
}


def _normalize_postcode(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip().upper().replace(" ", "")
    if len(raw) < 5:
        return None
    return f"{raw[:-3]} {raw[-3:]}"


def _extract_postcode(text: str) -> Optional[str]:
    match = _POSTCODE_RE.search(text or "")
    if not match:
        return None
    return _normalize_postcode(match.group(0))


def _strip_terms(text: str) -> str:
    split = re.split(r"Terms\s*&?\s*Conditions", text, flags=re.IGNORECASE)
    return split[0] if split else text


def _extract_invoice_number(text: str) -> Optional[str]:
    # Invoice number typically in a line below "INVOICE NO :"
    match = re.search(r"INVOICE NO\s*:?\s*([0-9-]+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(
        r"INVOICE DATE:\s*\(TAXPOINT\)\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()
    return None


def _extract_due_date(text: str) -> Optional[str]:
    match = re.search(
        r"PAYMENT DUE DATE:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()
    return None


def _extract_totals(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    value_ex = []
    vat_vals = []
    total_vals = []
    for i, line in enumerate(lines):
        if line.lower() == "value excl vat":
            for nxt in lines[i + 1 : i + 6]:
                val = parse_money(nxt)
                if val is not None:
                    value_ex.append(val)
        if line.lower() == "vat":
            for nxt in lines[i + 1 : i + 6]:
                val = parse_money(nxt)
                if val is not None:
                    vat_vals.append(val)
        if line.lower() == "invoice total":
            for nxt in lines[i + 1 : i + 6]:
                val = parse_money(nxt)
                if val is not None:
                    total_vals.append(val)
    net = value_ex[-1] if value_ex else None
    vat = vat_vals[-1] if vat_vals else None
    total = total_vals[-1] if total_vals else None
    return net, vat, total


def parse_nestle(text: str) -> InvoiceData:
    warnings: list[str] = []
    text = _strip_terms(text or "")

    postcode = _extract_postcode(text)
    ledger_account = _LEDGER_MAP.get(postcode) if postcode else None
    if not postcode:
        warnings.append("Deliver To postcode not found")
    elif ledger_account is None:
        warnings.append(f"Unknown Deliver To postcode: {postcode}")

    invoice_number = _extract_invoice_number(text) or "UNKNOWN"
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

    # Rare zero VAT line: we assume zero-rated net = total - vat_net - vat_amount
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
