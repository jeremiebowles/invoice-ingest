from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional

from app.models import InvoiceData
from app.parse_utils import parse_date, parse_money, approx_equal, extract_delivery_postcode, LEDGER_MAP, POSTCODE_RE, normalize_postcode


def _extract_delivery_postcode_hunts(text: str) -> Optional[str]:
    """Extract delivery postcode from Hunts two-column header layout.

    pdfplumber merges left (Invoice To) and right (Deliver To) columns onto
    the same line, so the delivery postcode (right column) appears AFTER the
    billing postcode on the same line.  We grab the last known-store postcode
    in the header section.
    """
    header_match = re.search(r"Deliver To(.+?)Code\s+Description", text, re.IGNORECASE | re.DOTALL)
    header = header_match.group(1) if header_match else text

    matches = POSTCODE_RE.findall(header)
    normalized = [normalize_postcode(f"{m[0]}{m[1]}") for m in matches]
    normalized = [pc for pc in normalized if pc is not None]

    # Prefer the last LEDGER_MAP postcode (rightmost column = delivery)
    known = [pc for pc in normalized if pc in LEDGER_MAP]
    if known:
        return known[-1]
    return normalized[-1] if normalized else None


def _extract_invoice_number(text: str) -> Optional[str]:
    """Extract Hunts invoice number in format 510-NNNNNN."""
    match = re.search(r"\b(\d{3}-\d{6})\b", text)
    return match.group(1) if match else None


def _extract_tax_point_date(text: str) -> Optional[str]:
    """Extract Tax Point Date from Hunts header.

    pdfplumber often renders the label with internal spaces
    (e.g. "Ta x Po in t D at e"), so we fall back to finding the first
    date pattern in the header area (before line items).
    """
    # Try the clean label first
    match = re.search(
        r"Tax Point Date\s*(\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{2,4})",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).replace(" ", "")

    # Fallback: first date in the header section (before line items)
    header_match = re.search(r"(.+?)Code\s+Description", text, re.DOTALL | re.IGNORECASE)
    header = header_match.group(1) if header_match else text[:500]

    dates = re.findall(r"(\d{1,2}/\s*\d{1,2}/\s*\d{2,4})", header)
    if dates:
        return dates[0].replace(" ", "")
    return None


def _extract_vat_analysis(text: str) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Parse Hunts VAT Analysis table.

    pdfplumber renders numbered rate lines like:
        1 ZERO 148.22 0.00
        2 20.00% 115.20 23.04
    And summary values like:
        263.42 Ex VAT
        23.04  (VAT total)
        286.46 (Total amount)
    """
    # Find the LAST "VAT Analysis" section (multi-page invoices repeat a
    # "Continued" placeholder on earlier pages; the real data is on the last page)
    start = None
    for m in re.finditer(r"VAT Analysis", text, flags=re.IGNORECASE):
        start = m.start()
    if start is None:
        return None, None, None, None

    tail = text[start:]
    end_match = re.search(r"BACS Payment Details|All monetary values", tail, flags=re.IGNORECASE)
    section = tail[: end_match.start()] if end_match else tail

    lines = [line.strip() for line in section.splitlines() if line.strip()]

    vat_net = 0.0
    nonvat_net = 0.0
    vat_amount = 0.0
    found_rates = False

    # Parse numbered rate lines: "1 ZERO 148.22 0.00" or "2 20.00% 115.20 23.04"
    for line in lines:
        m = re.match(r"^\d+\s+(ZERO|[\d.]+%)\s+([\d.,]+)\s+([\d.,]+)", line, re.IGNORECASE)
        if m:
            rate_str = m.group(1)
            net_val = parse_money(m.group(2))
            vat_val = parse_money(m.group(3))
            if net_val is None:
                continue
            found_rates = True
            if rate_str.upper() == "ZERO" or rate_str == "0.00%":
                nonvat_net += net_val
            else:
                vat_net += net_val
            if vat_val is not None:
                vat_amount += vat_val

    if not found_rates:
        return None, None, None, None

    # Extract total from the summary area
    # Look for a number on a line by itself or before "Amount", after the rate lines
    total = None
    # Pattern: number followed by "Amount" or at end of section near account number
    for line in lines:
        # Match total line: "02920 494902 286.46" or just "286.46" near end
        # The total appears on the line with the account number
        m = re.match(r"^\d{5}\s+\d{6}\s+([\d.,]+)$", line)
        if m:
            val = parse_money(m.group(1))
            if val is not None:
                total = val

    # Also try "Ex VAT" line for the ex-vat total as cross-check
    # e.g. "263.42 Ex VAT" or "V AT R at e Va l ue V AT 263.42 Ex VAT"
    ex_vat_match = re.search(r"([\d.,]+)\s+Ex\s*VAT", section, re.IGNORECASE)
    if total is None and ex_vat_match:
        ex_vat = parse_money(ex_vat_match.group(1))
        if ex_vat is not None:
            total = round(ex_vat + vat_amount, 2)

    return round(vat_net, 2), round(nonvat_net, 2), round(vat_amount, 2), total


def _parse_section(text: str) -> InvoiceData:
    warnings: list[str] = []
    is_credit = bool(re.search(r"Credit\s*(Memo|Note)", text or "", flags=re.IGNORECASE))

    postcode = _extract_delivery_postcode_hunts(text or "")
    ledger_account = LEDGER_MAP.get(postcode) if postcode else None
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
    normalized = text.replace("\u2019", "'")
    # Split on "Hunt's Food Group Ltd" header (page boundaries)
    starts = [m.start() for m in re.finditer(r"Hunt's Food Group Ltd", normalized)]
    if not starts:
        return [_parse_section(normalized)]

    # Build per-page sections
    pages: list[str] = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(normalized)
        page = normalized[start:end].strip()
        if page:
            pages.append(page)

    # Merge multi-page invoices: if "N of M" where N > 1, it's a continuation
    sections: list[str] = []
    for page in pages:
        page_match = re.search(r"\b(\d+)\s+of\s+(\d+)\b", page)
        if page_match and int(page_match.group(1)) > 1 and sections:
            sections[-1] = sections[-1] + "\n" + page
        else:
            sections.append(page)

    return [_parse_section(section) for section in sections]
