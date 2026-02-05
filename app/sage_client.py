from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Dict, Optional

import requests

from app.models import InvoiceData

SAGE_TOKEN_URL = "https://oauth.accounting.sage.com/token"
SAGE_API_BASE = "https://api.accounting.sage.com/v3.1"


def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value.strip() if value else None


def _refresh_access_token() -> str:
    client_id = _get_env("SAGE_CLIENT_ID")
    client_secret = _get_env("SAGE_CLIENT_SECRET")
    refresh_token = _get_env("SAGE_REFRESH_TOKEN")

    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Missing Sage OAuth env vars")

    resp = requests.post(
        SAGE_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _get_ledger_account_id(invoice: InvoiceData) -> Optional[str]:
    if invoice.ledger_account == 5001:
        return _get_env("SAGE_LEDGER_5001_ID")
    if invoice.ledger_account == 5002:
        return _get_env("SAGE_LEDGER_5002_ID")
    if invoice.ledger_account == 5004:
        return _get_env("SAGE_LEDGER_5004_ID")
    return None


def _get_tax_rate_ids() -> tuple[str, str]:
    standard = _get_env("SAGE_TAX_STANDARD_ID") or "GB_STANDARD"
    zero = _get_env("SAGE_TAX_ZERO_ID") or "GB_ZERO"
    return standard, zero


def _compute_due_date(invoice_date: date) -> date:
    return invoice_date + timedelta(days=30)


def post_purchase_invoice(invoice: InvoiceData) -> Dict[str, Any]:
    business_id = _get_env("SAGE_BUSINESS_ID")
    contact_id = _get_env("SAGE_CONTACT_ID")
    if not business_id or not contact_id:
        raise RuntimeError("Missing Sage business/contact configuration")

    ledger_account_id = _get_ledger_account_id(invoice)
    if not ledger_account_id:
        raise RuntimeError("Missing Sage ledger account mapping")

    access_token = _refresh_access_token()
    tax_standard_id, tax_zero_id = _get_tax_rate_ids()

    vat_net = round(invoice.vat_net, 2)
    nonvat_net = round(invoice.nonvat_net, 2)
    vat_amount = round(invoice.vat_amount, 2)
    net_amount = round(vat_net + nonvat_net, 2)
    total_amount = round(net_amount + vat_amount, 2)
    due_date = _compute_due_date(invoice.invoice_date)

    invoice_lines = []
    if vat_net > 0:
        invoice_lines.append(
            {
                "description": invoice.description or "Purchases",
                "ledger_account_id": ledger_account_id,
                "quantity": 1,
                "unit_price": vat_net,
                "net_amount": vat_net,
                "tax_rate_id": tax_standard_id,
                "tax_amount": vat_amount,
                "total_amount": round(vat_net + vat_amount, 2),
            }
        )

    if nonvat_net > 0:
        invoice_lines.append(
            {
                "description": invoice.description or "Purchases",
                "ledger_account_id": ledger_account_id,
                "quantity": 1,
                "unit_price": nonvat_net,
                "net_amount": nonvat_net,
                "tax_rate_id": tax_zero_id,
                "tax_amount": 0,
                "total_amount": nonvat_net,
            }
        )

    if not invoice_lines:
        raise RuntimeError("Invoice has no line amounts to post")

    payload = {
        "purchase_invoice": {
            "contact_id": contact_id,
            "date": invoice.invoice_date.isoformat(),
            "due_date": due_date.isoformat(),
            "reference": invoice.supplier_reference,
            "invoice_number": invoice.supplier_reference,
            "net_amount": net_amount,
            "tax_amount": vat_amount,
            "total_amount": total_amount,
            "invoice_lines": invoice_lines,
        }
    }

    resp = requests.post(
        f"{SAGE_API_BASE}/purchase_invoices",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Session-Company-Id": business_id,
        },
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()
