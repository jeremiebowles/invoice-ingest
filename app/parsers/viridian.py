from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP


def _extract_delivery_block(text: str) -> Optional[str]:
    match = re.search(
        r"Delivery Address\s*(.+?)\s*Qty\s+Code",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        return match.group(1).strip()
    return None


def _extract_invoice_number(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+No:\s*([A-Z0-9-]+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_invoice_date(text: str) -> Optional[str]:
    match = re.search(r"Invoice\s+Date:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_terms_days(text: str) -> Optional[int]:
    match = re.search(r"Terms:\s*([0-9]+)\s*days", text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _extract_money_list(text: str, label: str) -> list[float]:
    pattern = rf"{re.escape(label)}\s*([0-9.,]+)"
    return [parse_money(value) for value in re.findall(pattern, text, flags=re.IGNORECASE)]


def _extract_total(text: str) -> Optional[float]:
    match = re.search(r"Total:\s*([0-9.,]+)", text, flags=re.IGNORECASE)
    if match:
        return parse_money(match.group(1))
    return None


def _extract_vat_analysis(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    lines = [line.strip() for line in (text or "").splitlines()]
    try:
        start = next(i for i, line in enumerate(lines) if "vat analysis" in line.lower())
    except StopIteration:
        return None, None, None

    section: list[str] = []
    for line in lines[start + 1 :]:
        if re.search(r"^Terms:|^Goods Net:|^Delivery:|^Order Net:|^Total:", line, flags=re.IGNORECASE):
            break
        if line:
            section.append(line)

    def _collect_after(label: str) -> list[str]:
        try:
            idx = next(i for i, line in enumerate(section) if line.lower() == label.lower())
        except StopIteration:
            return []
        values: list[str] = []
        for line in section[idx + 1 :]:
            if re.search(r"^Tax Code$|^VAT %$|^Net \\(£\\)$|^VAT \\(£\\)$", line):
                break
            values.append(line)
        return values

    tax_codes = _collect_after("Tax Code")
    vat_rates = _collect_after("VAT %")
    net_values = _collect_after("Net (£)")
    vat_values = _collect_after("VAT (£)")

    vat_net = 0.0
    nonvat_net = 0.0
    vat_amount = 0.0

    for idx, net in enumerate(net_values):
        net_val = parse_money(net)
        if net_val is None:
            continue
        rate = None
        if idx < len(vat_rates):
            rate = parse_money(vat_rates[idx])
        code = tax_codes[idx] if idx < len(tax_codes) else ""
        if (rate is not None and rate > 0) or code.upper().startswith("T1"):
            vat_net += net_val
        else:
            nonvat_net += net_val

    for val in vat_values:
        vat_val = parse_money(val)
        if vat_val is not None:
            vat_amount += vat_val

    return round(vat_net, 2), round(nonvat_net, 2), round(vat_amount, 2)


def parse_viridian(text: str) -> InvoiceData:
    warnings: list[str] = []

    deliver_block = _extract_delivery_block(text or "")
    postcode = extract_delivery_postcode(deliver_block or "") or extract_delivery_postcode(text or "")
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

    terms_days = _extract_terms_days(text or "")
    due_date = invoice_date + timedelta(days=terms_days or 30) if invoice_date else None

    vat_net, nonvat_net, vat_amount = _extract_vat_analysis(text or "")
    if vat_net is None or nonvat_net is None or vat_amount is None:
        net_values = _extract_money_list(text or "", "Net (£)")
        vat_values = _extract_money_list(text or "", "VAT (£)")
        vat_net = round(sum(v for v in net_values if v is not None), 2) if net_values else None
        vat_amount = round(sum(v for v in vat_values if v is not None), 2) if vat_values else None

    total = _extract_total(text or "")
    if total is None:
        warnings.append("Total amount not found")
        total = 0.0

    if vat_net is None:
        warnings.append("VAT net amount not found")
        vat_net = 0.0
    if vat_amount is None:
        warnings.append("VAT amount not found")
        vat_amount = 0.0

    if nonvat_net is None:
        nonvat_net = round(max(total - vat_net - vat_amount, 0.0), 2)

    if not approx_equal(vat_net + nonvat_net + vat_amount, total):
        warnings.append("Totals do not reconcile (net + vat != total)")

    return InvoiceData(
        supplier="Viridian",
        supplier_reference=invoice_number,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=postcode,
        ledger_account=ledger_account,
        contact_id="36ee5838c7a54c799c2cf60c667b41b0",
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
        is_credit=False,
    )
