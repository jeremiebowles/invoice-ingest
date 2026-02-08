from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import approx_equal, first_match, parse_date, parse_money


_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})\b", re.IGNORECASE)
_DATE_RE = re.compile(r"\b([0-9]{1,2}[\-/.][0-9]{1,2}[\-/.][0-9]{2,4})\b")
_MONEY_CAPTURE_RE = re.compile(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d{2})|[-+]?\d+(?:\.\d{2})")
_MONEY_ONLY_RE = re.compile(r"^[£$]?\s*[-+]?\d{1,3}(?:,\d{3})*(?:\.\d{2})\s*$")

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

def _extract_credit_number(text: str) -> Optional[str]:
    patterns = [
        r"Credit\s*Memo\s*(Number|No\.?|#)\s*[:]?\s*([A-Z0-9\-/]+)",
        r"Credit\s*Note\s*(Number|No\.?|#)\s*[:]?\s*([A-Z0-9\-/]+)",
    ]
    match = first_match(patterns, text, flags=re.IGNORECASE)
    if match:
        return match.group(match.lastindex)
    return None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(
        r"(Posting\s*Date|Posting/Tax\s*Point\s*Date|Posting\s*Tax\s*Point\s*Dat)\s*:?\s*([0-9]{1,2}[\-/.][0-9]{1,2}[\-/.][0-9]{2,4})",
        text or "",
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(match.lastindex)

    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    for line in lines:
        if re.search(r"Posting\s*Date|Posting/Tax\s*Point\s*Date|Posting\s*Tax\s*Point\s*Dat", line, flags=re.IGNORECASE):
            date_match = _DATE_RE.search(line)
            if date_match:
                return date_match.group(1)

    patterns = [
        r"Invoice\s*Date\s*[:]?\s*([A-Z0-9\-/ ]+)",
        r"Posting\s*Date\s*[:]?\s*([A-Z0-9\-/ ]+)",
        r"Posting/Tax\s*Point\s*Date\s*[:]?\s*([A-Z0-9\-/ ]+)",
        r"Posting\s*Tax\s*Point\s*Dat\s*[:]?\s*([A-Z0-9\-/ ]+)",
        r"Date\s*[:]?\s*([0-9]{1,2}[\-/][0-9]{1,2}[\-/][0-9]{2,4})",
    ]
    match = first_match(patterns, text, flags=re.IGNORECASE)
    if match:
        value = match.group(match.lastindex)
        date_match = _DATE_RE.search(value)
        return date_match.group(1) if date_match else value
    match = re.search(r"\b\d{1,2}\.\s*[A-Za-z]+\s+\d{4}\b", text or "")
    if match:
        return match.group(0)
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


def _strip_vat_rate(values: list[float]) -> list[float]:
    if len(values) >= 3 and values[0] <= 100 and values[0].is_integer():
        return values[1:]
    return values


def _extract_vat_breakdown(
    text: str,
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    vat_net = None
    nonvat_net = None
    vat_amount = None

    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    start_idx = 0
    for idx, line in enumerate(lines):
        if re.search(r"VAT\s*Identifier", line, flags=re.IGNORECASE):
            start_idx = idx + 1
            break

    for line in lines[start_idx:]:
        if re.match(r"^[SZ]\b", line):
            values = _strip_vat_rate(_extract_money_values(line))
            if len(values) >= 2:
                if line.startswith("S"):
                    vat_net = values[0]
                    vat_amount = values[-1] if len(values) >= 2 else vat_amount
                elif line.startswith("Z"):
                    nonvat_net = values[0]

    if vat_net is None and nonvat_net is None:
        vat_total = 0.0
        nonvat_total = 0.0
        vat_found = False
        nonvat_found = False
        for line in lines:
            code_match = re.search(r"\b([SZ])\b", line)
            if not code_match:
                continue
            values = _extract_money_values(line)
            if not values:
                continue
            amount = values[-1]
            if code_match.group(1) == "S":
                vat_total += amount
                vat_found = True
            else:
                nonvat_total += amount
                nonvat_found = True
        if vat_found:
            vat_net = vat_total
        if nonvat_found:
            nonvat_net = nonvat_total

    return vat_net, nonvat_net, vat_amount


def _extract_total_gbp_incl_vat(text: str) -> Optional[float]:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    in_vat_section = False
    for line in lines:
        if re.search(r"VAT\s*Identifier", line, flags=re.IGNORECASE):
            in_vat_section = True
        if re.search(r"Total\s*GBP\s*Incl\.?\s*VAT", line, flags=re.IGNORECASE):
            values = _extract_money_values(line)
            if values:
                return values[0]
        if in_vat_section and re.match(r"^Total\b", line, flags=re.IGNORECASE):
            values = _extract_money_values(line)
            if values:
                return values[0]
    return None


def _extract_totals_block(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    total_excl = None
    vat_amount = None
    total_incl = None

    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]

    def _next_numeric_value(idx: int) -> Optional[float]:
        for offset in range(1, 6):
            if idx + offset >= len(lines):
                break
            candidate = lines[idx + offset]
            if _MONEY_ONLY_RE.match(candidate):
                values = _extract_money_values(candidate)
                return values[0] if values else None
        return None

    excl_idx = None
    vat_idx = None
    incl_idx = None

    for idx, line in enumerate(lines):
        if total_incl is None and re.search(r"Total\s*GBP\s*Incl\.?\s*VAT", line, flags=re.IGNORECASE):
            total_incl = _next_numeric_value(idx)
            incl_idx = idx
            continue
        if vat_amount is None and re.search(r"(VAT\s*Amount|\d{1,2}%\s*VAT)", line, flags=re.IGNORECASE):
            vat_amount = _next_numeric_value(idx)
            vat_idx = idx
            continue
        if total_excl is None and re.search(r"Total\s*GBP\s*Excl\.?\s*VAT", line, flags=re.IGNORECASE):
            total_excl = _next_numeric_value(idx)
            excl_idx = idx
            continue

    # If totals are listed as a block of labels followed by numeric lines, map in order.
    label_indices = [i for i in (excl_idx, vat_idx, incl_idx) if i is not None]
    if label_indices:
        start = max(label_indices) + 1
        numeric_lines: list[float] = []
        for line in lines[start : start + 8]:
            if _MONEY_ONLY_RE.match(line):
                values = _extract_money_values(line)
                if values:
                    numeric_lines.append(values[0])
        if len(numeric_lines) >= 3:
            total_excl = numeric_lines[0]
            vat_amount = numeric_lines[1]
            total_incl = numeric_lines[2]
        elif len(numeric_lines) >= 2:
            total_excl = numeric_lines[0]
            vat_amount = numeric_lines[1]
        elif len(numeric_lines) >= 1:
            total_excl = numeric_lines[0]

    if vat_amount is None and total_excl is not None and total_incl is not None:
        vat_amount = round(total_incl - total_excl, 2)

    return total_excl, vat_amount, total_incl


def _extract_vat_section_total(text: str) -> Optional[float]:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    in_vat_section = False
    for line in lines:
        if re.search(r"VAT\s*Identifier", line, flags=re.IGNORECASE):
            in_vat_section = True
        if in_vat_section and re.match(r"^Total\b", line, flags=re.IGNORECASE):
            values = _extract_money_values(line)
            if values:
                return values[0]
    return None


def _extract_total_gbp(text: str) -> Optional[float]:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    for line in lines:
        if re.search(r"Total\s*GBP\b", line, flags=re.IGNORECASE):
            values = _extract_money_values(line)
            if values:
                return values[0]
    return None


def parse_clf(text: str) -> InvoiceData:
    warnings: list[str] = []
    is_credit = bool(re.search(r"Credit\s*(Memo|Note)", text or "", flags=re.IGNORECASE))

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

    credit_number = _extract_credit_number(text or "")
    invoice_number = credit_number or _extract_invoice_number(text or "")
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
    total_excl_vat, totals_vat_amount, total_incl_vat = _extract_totals_block(text)
    total_incl_vat = total_incl_vat or _extract_total_gbp_incl_vat(text)
    vat_section_total = _extract_vat_section_total(text)
    if total_incl_vat is None and vat_section_total is not None:
        total_incl_vat = vat_section_total
    total_gbp = _extract_total_gbp(text)
    if vat_amount is None and totals_vat_amount is not None:
        vat_amount = totals_vat_amount

    if (
        vat_net is None
        and nonvat_net is None
        and total_excl_vat is not None
    ):
        if vat_amount is None and total_incl_vat is not None:
            vat_amount = round(total_incl_vat - total_excl_vat, 2)
        if vat_amount is not None and vat_amount > 0:
            vat_net = total_excl_vat
            nonvat_net = 0.0
        else:
            vat_net = 0.0
            nonvat_net = total_excl_vat

    if (
        total_excl_vat is not None
        and total_incl_vat is not None
        and vat_amount is not None
        and not approx_equal(vat_amount, total_incl_vat - total_excl_vat)
    ):
        vat_amount = round(total_incl_vat - total_excl_vat, 2)
        warnings.append("VAT amount overridden from totals")

    if vat_net is None or nonvat_net is None or vat_amount is None:
        (
            vat_net_from_breakdown,
            nonvat_net_from_breakdown,
            vat_amount_from_breakdown,
        ) = _extract_vat_breakdown(text)
        if vat_net is None:
            vat_net = vat_net_from_breakdown
        if nonvat_net is None:
            nonvat_net = nonvat_net_from_breakdown
        if vat_amount is None:
            vat_amount = vat_amount_from_breakdown

    # Zero-VAT fallback: no S/Z lines detected, but Total GBP exists
    zero_vat_fallback = (
        vat_net is None
        and nonvat_net is None
        and vat_amount is None
        and total_gbp is not None
    )
    if zero_vat_fallback:
        vat_net = 0.0
        nonvat_net = total_gbp
        vat_amount = 0.0
        total_incl_vat = total_gbp

    if vat_net is None:
        vat_net = 0.0
        warnings.append("VAT net amount not found")
    if nonvat_net is None:
        nonvat_net = 0.0
        warnings.append("Non-VAT net amount not found")
    if vat_amount is None:
        vat_amount = 0.0
        warnings.append("VAT amount not found")
    subtotal = vat_net + nonvat_net + vat_amount
    total = total_incl_vat if total_incl_vat is not None else subtotal
    if total_incl_vat is None and total_gbp is not None:
        total = total_gbp

    if total_incl_vat is None and total_gbp is None:
        warnings.append("Total GBP Incl. VAT not found")
    elif total_incl_vat is not None and not approx_equal(subtotal, total_incl_vat):
        warnings.append("Total GBP Incl. VAT does not reconcile")

    if total_excl_vat is not None and not approx_equal(vat_net + nonvat_net, total_excl_vat):
        warnings.append("Total GBP Excl. VAT does not reconcile")

    if totals_vat_amount is not None and not approx_equal(vat_amount, totals_vat_amount):
        warnings.append("VAT Amount does not reconcile")

    if is_credit:
        vat_net = abs(vat_net)
        nonvat_net = abs(nonvat_net)
        vat_amount = abs(vat_amount)
        total = abs(total)
        if vat_net == 0 and vat_amount == 0 and nonvat_net > 0:
            warnings = [
                w
                for w in warnings
                if w not in {"VAT net amount not found", "VAT amount not found"}
            ]

    return InvoiceData(
        supplier="CLF",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        is_credit=is_credit,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        vat_net=max(vat_net, 0.0),
        nonvat_net=max(nonvat_net, 0.0),
        vat_amount=max(vat_amount, 0.0),
        total=max(total, 0.0),
        warnings=warnings,
    )
