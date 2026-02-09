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


def _extract_deliver_block(text: str) -> Optional[str]:
    match = re.search(
        r"Deliver To\s*(.+?)\s*INVOICE",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        return match.group(1).strip()
    return None


def _extract_postcode(text: str) -> Optional[str]:
    match = _POSTCODE_RE.search(text or "")
    if not match:
        return None
    return _normalize_postcode(match.group(0))


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"INVOICE\s*([0-9-]+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_tax_point_date(text: str) -> Optional[str]:
    match = re.search(
        r"Tax Point Date\s*([0-9]{1,2}\s*/\s*[0-9]{1,2}\s*/\s*[0-9]{2,4})",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).replace(" ", "")
    return None


def _value_after_label(lines: list[str], label: str) -> Optional[float]:
    for i, line in enumerate(lines):
        if line.strip().lower() == label.lower():
            for nxt in lines[i + 1 : i + 6]:
                val = parse_money(nxt.strip())
                if val is not None:
                    return val
    return None


def _extract_vat_analysis(text: str) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    start = None
    for m in re.finditer(r"VAT Analysis", text, flags=re.IGNORECASE):
        start = m.start()
    if start is None:
        return None, None, None, None

    tail = text[start:]
    end_match = re.search(r"BACS Payment Details|All monetary values", tail, flags=re.IGNORECASE)
    section = tail[: end_match.start()] if end_match else tail

    lines = [line.strip() for line in section.splitlines() if line.strip()]
    rates: list[str] = []
    values: list[float] = []
    vats: list[float] = []
    state = None
    for line in lines:
        if line.lower() == "vat rate":
            state = "rates"
            continue
        if line.lower() == "value":
            state = "values"
            continue
        if line.lower() == "vat":
            state = "vat"
            continue
        if re.search(r"^total$|^ex vat$|^amount$", line, flags=re.IGNORECASE):
            state = None
            continue
        if state == "rates" and ( "zero" in line.lower() or "%" in line):
            rates.append(line)
        elif state == "values":
            val = parse_money(line)
            if val is not None:
                values.append(val)
        elif state == "vat":
            val = parse_money(line)
            if val is not None:
                vats.append(val)

    vat_net = 0.0
    nonvat_net = 0.0
    for idx, val in enumerate(values):
        rate = rates[idx] if idx < len(rates) else ""
        if "zero" in rate.lower() or "0.00" in rate:
            nonvat_net += val
        else:
            vat_net += val

    vat_amount = sum(vats) if vats else None

    total = _value_after_label(lines, "Amount")
    if total is None:
        total = _value_after_label(lines, "Total Amount")
    if total is None:
        total = _value_after_label(lines, "Total")

    return round(vat_net, 2), round(nonvat_net, 2), (round(vat_amount, 2) if vat_amount is not None else None), total


def _parse_section(text: str) -> InvoiceData:
    warnings: list[str] = []
    is_credit = bool(re.search(r"Credit\s*(Memo|Note)", text or "", flags=re.IGNORECASE))

    deliver_block = _extract_deliver_block(text or "")
    postcode = _extract_postcode(deliver_block or "") or _extract_postcode(text or "")
    ledger_account = _LEDGER_MAP.get(postcode) if postcode else None
    if not postcode:
        warnings.append("Deliver To postcode not found")
    elif ledger_account is None:
        warnings.append(f"Unknown Deliver To postcode: {postcode}")

    invoice_number = _extract_invoice_number(text or "") or "UNKNOWN"
    invoice_date_str = _extract_tax_point_date(text or "")
    invoice_date = parse_date(invoice_date_str)
    if not invoice_date:
        warnings.append("Invoice date not found")
        invoice_date = parse_date("01/01/1970")
    due_date = invoice_date + timedelta(days=30) if invoice_date else None

    vat_net, nonvat_net, vat_amount, total = _extract_vat_analysis(text or "")
    if vat_net is None:
        warnings.append("VAT net amount not found")
        vat_net = 0.0
    if nonvat_net is None:
        nonvat_net = 0.0
    if vat_amount is None:
        warnings.append("VAT amount not found")
        vat_amount = 0.0
    if total is None:
        total = round(vat_net + nonvat_net + vat_amount, 2)
        warnings.append("Total amount not found")

    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Hunts",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="76ef2eec964848e2bae7e9d8fe15a633",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=is_credit,
    )


def parse_hunts(text: str) -> list[InvoiceData]:
    if not text:
        return []
    normalized = text.replace("Hunt’s", "Hunt's")
    starts = [m.start() for m in re.finditer(r"Hunt's Food Group Ltd", normalized)]
    if not starts:
        return [_parse_section(normalized)]
    sections: list[str] = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(normalized)
        section = normalized[start:end].strip()
        if section:
            sections.append(section)
    return [_parse_section(section) for section in sections]
