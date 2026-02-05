# app/parsers/clf.py
from __future__ import annotations

import re
from datetime import timedelta
from app.models import InvoiceData
from app.parse_utils import parse_money, parse_date, approx_equal


# UK postcode (good-enough pattern) e.g. CF10 1AE / CF243LP etc.
_UK_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})\b", re.IGNORECASE
)

# Deliver-to postcode → Sage ledger account code
_POSTCODE_TO_LEDGER = {
    "CF10 1AE": 5001,  # Shop 1
    "CF24 3LP": 5002,  # Shop 2
    "CF11 9DX": 5004,  # Shop 3
}


def _find(label_patterns: list[str], text: str) -> str | None:
    for pat in label_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if m:
            return m.group(1).strip()
    return None


def _extract_deliver_to_block(text: str) -> str:
    """
    Tries to capture the "Deliver To" section up to a likely next heading.
    """
    block = _find(
        [
            # Capture after "Deliver To" until a likely next section header
            r"Deliver\s*To\s*[:#]?\s*(.+?)(?:\n\s*\n|VAT\s*Summary|VAT\s*Analysis|Summary|Totals?|Sub\s*Total|Total\s*Due|Invoice\s*Total)",
        ],
        text,
    )
    return block or ""


def _normalise_uk_postcode(raw: str) -> str:
    """
    Normalise to 'CF10 1AE' style.
    """
    m = _UK_POSTCODE_RE.search(raw.upper().replace("\u00a0", " "))
    if not m:
        raise ValueError("No UK postcode found")
    outward = m.group(1).upper()
    inward = m.group(2).upper()
    return f"{outward} {inward}"


def parse_clf(text: str) -> InvoiceData:
    supplier = "CLF Distribution"

    inv_no = _find(
        [
            r"Invoice\s*(?:No|Number)\s*[:#]?\s*(.+)$",
            r"Supplier\s*Ref(?:erence)?\s*[:#]?\s*(.+)$",
        ],
        text,
    )
    if not inv_no:
        raise ValueError("Could not find invoice number")

    inv_date_raw = _find(
        [
            r"Invoice\s*Date\s*[:#]?\s*(.+)$",
            r"Date\s*[:#]?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        ],
        text,
    )
    if not inv_date_raw:
        raise ValueError("Could not find invoice date")
    invoice_date = parse_date(inv_date_raw)

    due_date_raw = _find([r"Due\s*Date\s*[:#]?\s*(.+)$"], text)
    terms_raw = _find([r"Terms\s*[:#]?\s*(.+)$", r"Payment\s*Terms\s*[:#]?\s*(.+)$"], text)

    due_date = None
    warnings: list[str] = []

    if due_date_raw:
        try:
            due_date = parse_date(due_date_raw)
        except Exception:
            warnings.append(f"Found Due Date but couldn't parse: {due_date_raw!r}")
    elif terms_raw:
        m = re.search(r"(\d+)\s*day", terms_raw, flags=re.IGNORECASE)
        if m:
            due_date = invoice_date + timedelta(days=int(m.group(1)))

    # --- Deliver To → postcode → ledger mapping ---
    deliver_to = _extract_deliver_to_block(text)
    if not deliver_to:
        warnings.append("Deliver To block not found")

    deliver_to_postcode = None
    ledger_account = None

    if deliver_to:
        try:
            deliver_to_postcode = _normalise_uk_postcode(deliver_to)
            ledger_account = _POSTCODE_TO_LEDGER.get(deliver_to_postcode)
            if ledger_account is None:
                warnings.append(f"Deliver To postcode {deliver_to_postcode} not recognised for ledger mapping")
        except Exception:
            warnings.append("Could not find/parse a UK postcode in Deliver To block")

    # VAT summary parsing (keep your existing patterns / refine as needed)
    vat_amount_raw = _find([r"VAT\s*(?:Amount)?\s*[:#]?\s*£?\s*([0-9,]+\.\d{2})"], text)
    total_raw = _find(
        [
            r"Total\s*(?:Due)?\s*[:#]?\s*£?\s*([0-9,]+\.\d{2})",
            r"Invoice\s*Total\s*[:#]?\s*£?\s*([0-9,]+\.\d{2})",
        ],
        text,
    )

    vat_net_raw = _find(
        [
            r"(?:Standard\s*Rated|VAT\s*able|Taxable)\s*(?:Net)?\s*[:#]?\s*£?\s*([0-9,]+\.\d{2})",
        ],
        text,
    )
    nonvat_net_raw = _find(
        [
            r"(?:Zero\s*Rated|Non[-\s]*VAT|Exempt)\s*(?:Net)?\s*[:#]?\s*£?\s*([0-9,]+\.\d{2})",
        ],
        text,
    )

    vat_net = parse_money(vat_net_raw) if vat_net_raw else 0.0
    nonvat_net = parse_money(nonvat_net_raw) if nonvat_net_raw else 0.0
    vat_amount = parse_money(vat_amount_raw) if vat_amount_raw else 0.0
    total = parse_money(total_raw) if total_raw else 0.0

    if total <= 0:
        warnings.append("Total missing or zero")
    if vat_amount_raw is None:
        warnings.append("VAT amount not found (parsed as 0.00)")
    if vat_net_raw is None:
        warnings.append("VAT-able net not found (parsed as 0.00)")
    if nonvat_net_raw is None:
        warnings.append("Non-VAT net not found (parsed as 0.00)")

    if total > 0:
        calc_total = vat_net + nonvat_net + vat_amount
        if not approx_equal(calc_total, total):
            warnings.append(
                f"Reconciliation: vat_net+nonvat_net+vat_amount={calc_total:.2f} != total={total:.2f}"
            )

    # Put ledger_account into the model in whatever way you prefer:
    # Option A: store it in warnings? (no)
    # Option B: add a field to InvoiceData (recommended)
    # For now, I’ll tuck it into warnings only if missing, and return a dict-ready value via model extension later.

    invoice = InvoiceData(
        supplier=supplier,
        supplier_reference=inv_no,
        invoice_date=invoice_date,
        due_date=due_date,
        deliver_to_postcode=deliver_to_postcode,
        ledger_account=ledger_account,
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=warnings,
    )


    # If you haven't added a field yet, you can at least log these now:
    # logger.info("DeliverTo postcode=%s ledger_account=%s", deliver_to_postcode, ledger_account)

    return invoice
