from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, first_match, parse_date, parse_money


_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})\b", re.IGNORECASE)
_DATE_RE = re.compile(r"\b([0-9]{1,2}[\-/][0-9]{1,2}[\-/][0-9]{2,4})\b")
_MONEY_CAPTURE_RE = re.compile(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d{2})|[-+]?\d+(?:\.\d{2})")

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


def _extract_deliver_to_block(text: str) -> Optional[str]:
    match = re.search(
        r"Deliver\s*To\s*:?.*?\n(.+?)(?:\n\s*\n|Bill\s*To|Invoice|Purchase|Order|VAT|Total|Amount|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        return match.group(1).strip()
    return None


def _extract_amount(text: str, labels: list[str]) -> Optional[float]:
    patterns = [rf"{label}\s*[:£$]?\s*([\d,]+\.\d{{2}})" for label in labels]
    match = first_match(patterns, text, flags=re.IGNORECASE)
    if match:
        return parse_money(match.group(1))
    return None


def _extract_invoice_number(text: str) -> str:
    patterns = [
        r"Invoice\s*(Number|No\.?|#)\s*[:]?\s*([A-Z0-9\-/]+)",
        r"Inv\s*No\.?\s*[:]?\s*([A-Z0-9\-/]+)",
    ]
    match = first_match(patterns, text, flags=re.IGNORECASE)
    if match:
        return match.group(match.lastindex)
    return "UNKNOWN"


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(
        r"Posting\s*Date\s*:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        text or "",
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1)

    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    for line in lines:
        if re.search(r"Posting\s*Date", line, flags=re.IGNORECASE):
            date_match = _DATE_RE.search(line)
            if date_match:
                return date_match.group(1)

    patterns = [
        r"Invoice\s*Date\s*[:]?\s*([A-Z0-9\-/ ]+)",
        r"Posting\s*Date\s*[:]?\s*([A-Z0-9\-/ ]+)",
        r"Date\s*[:]?\s*([0-9]{1,2}[\-/][0-9]{1,2}[\-/][0-9]{2,4})",
    ]
    match = first_match(patterns, text, flags=re.IGNORECASE)
    if match:
        value = match.group(match.lastindex)
        date_match = _DATE_RE.search(value)
        return date_match.group(1) if date_match else value
    return None


def _extract_due_date(text: str) -> Optional[str]:
    patterns = [
        r"Due\s*Date\s*[:]?\s*([A-Z0-9\-/ ]+)",
        r"Payment\s*Due\s*[:]?\s*([A-Z0-9\-/ ]+)",
    ]
    match = first_match(patterns, text, flags=re.IGNORECASE)
    if match:
        return match.group(match.lastindex)
    return None


def _extract_terms_days(text: str) -> Optional[int]:
    match = re.search(r"Net\s*(\d{1,3})", text, flags=re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _extract_postcode_from_lines(text: str) -> Optional[str]:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    for idx, line in enumerate(lines):
        if re.search(r"Deliver\\s*To", line, flags=re.IGNORECASE):
            for offset in range(0, 8):
                if idx + offset >= len(lines):
                    break
                match = _POSTCODE_RE.search(lines[idx + offset])
                if match:
                    return _normalize_postcode(match.group(0))
    return None


def _find_known_postcode(text: str) -> Optional[str]:
    normalized = re.sub(r"\s+", "", (text or "").upper())
    for known in _LEDGER_MAP.keys():
        if known.replace(" ", "") in normalized:
            return known
    return None


def _extract_money_values(line: str) -> list[float]:
    values: list[float] = []
    for match in _MONEY_CAPTURE_RE.findall(line.replace("£", "").replace("GBP", "")):
        value = parse_money(match)
        if value is not None:
            values.append(value)
    return values


def _extract_vat_breakdown(
    text: str,
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    vat_net = None
    nonvat_net = None
    vat_amount = None
    total = None

    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    start_idx = 0
    for idx, line in enumerate(lines):
        if re.search(r"VAT\\s*Identifier", line, flags=re.IGNORECASE):
            start_idx = idx + 1
            break

    for line in lines[start_idx:]:
        if re.search(r"Total\\s*GBP\\s*Incl\\.?\\s*VAT", line, flags=re.IGNORECASE):
            values = _extract_money_values(line)
            if values:
                total = values[0]
            break

        if re.match(r"^Total\\b", line, flags=re.IGNORECASE):
            values = _extract_money_values(line)
            if values:
                total = values[0]
            break

        if re.match(r"^S\\b", line):
            values = _extract_money_values(line)
            if len(values) >= 2:
                vat_net = values[1]
                vat_amount = values[-1] if len(values) >= 3 else vat_amount
        if re.match(r"^Z\\b", line):
            values = _extract_money_values(line)
            if len(values) >= 2:
                nonvat_net = values[1]

    return vat_net, nonvat_net, vat_amount, total


def parse_clf(text: str) -> InvoiceData:
    warnings: list[str] = []

    deliver_block = _extract_deliver_to_block(text or "")
    postcode = None
    ledger_account = None

    if deliver_block:
        postcode_match = _POSTCODE_RE.search(deliver_block)
        if postcode_match:
            postcode = _normalize_postcode(postcode_match.group(0))
    if not postcode:
        postcode = _find_known_postcode(text or "")
    if not postcode:
        postcode = _extract_postcode_from_lines(text or "")

    if postcode:
        ledger_account = _LEDGER_MAP.get(postcode)
        if ledger_account is None:
            warnings.append(f"Unknown Deliver To postcode: {postcode}")
    else:
        warnings.append("Deliver To postcode not found")

    invoice_number = _extract_invoice_number(text or "")
    invoice_date_str = _extract_invoice_date(text or "")
    invoice_date = parse_date(invoice_date_str)
    if not invoice_date:
        warnings.append("Invoice date not found")
        invoice_date = parse_date("01/01/1970")

    due_date_str = _extract_due_date(text or "")
    due_date = parse_date(due_date_str) if due_date_str else None
    if due_date is None:
        terms_days = _extract_terms_days(text or "")
        if terms_days and invoice_date:
            due_date = invoice_date + timedelta(days=terms_days)

    vat_net = _extract_amount(text, ["VAT Net", "VATable", "Net Amount", "Net"])
    nonvat_net = _extract_amount(text, ["Non-VAT", "Non VAT", "Zero Rated", "Non-Vatable"])
    vat_amount = _extract_amount(text, ["VAT Amount", "VAT"])
    total = _extract_amount(
        text,
        ["Total GBP Incl. VAT", "Total Amount", "Total", "Amount Due", "Balance Due", "Invoice Total"],
    )

    if vat_net is None or nonvat_net is None or vat_amount is None or total is None:
        (
            vat_net_from_breakdown,
            nonvat_net_from_breakdown,
            vat_amount_from_breakdown,
            total_from_breakdown,
        ) = _extract_vat_breakdown(text)
        if vat_net is None:
            vat_net = vat_net_from_breakdown
        if nonvat_net is None:
            nonvat_net = nonvat_net_from_breakdown
        if vat_amount is None:
            vat_amount = vat_amount_from_breakdown
        if total is None:
            total = total_from_breakdown

    if vat_net is None:
        vat_net = 0.0
        warnings.append("VAT net amount not found")
    if nonvat_net is None:
        nonvat_net = 0.0
        warnings.append("Non-VAT net amount not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    if total is None:
        total = vat_net + nonvat_net + vat_amount
        warnings.append("Total amount not found")

    subtotal = vat_net + nonvat_net + vat_amount
    if not approx_equal(subtotal, total):
        warnings.append("Totals do not reconcile")

    return InvoiceData(
        supplier="CLF",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        vat_net=max(vat_net, 0.0),
        nonvat_net=max(nonvat_net, 0.0),
        vat_amount=max(vat_amount, 0.0),
        total=max(total, 0.0),
        warnings=warnings,
    )
