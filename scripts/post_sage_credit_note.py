#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict

import requests


SAGE_TOKEN_URL = "https://oauth.accounting.sage.com/token"
SAGE_API_BASE = "https://api.accounting.sage.com/v3.1"


def _env(name: str) -> str | None:
    value = os.getenv(name)
    return value.strip() if value else None


def _parse_date(value: str) -> date:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value}")


def _exchange_code(client_id: str, client_secret: str, code: str) -> Dict[str, Any]:
    resp = requests.post(
        SAGE_TOKEN_URL,
        headers={"Accept": "application/json"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": "https://oauth.pstmn.io/v1/browser-callback",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _post_credit_note(
    access_token: str,
    company_id: str,
    contact_id: str,
    ledger_id: str,
    number: str,
    credit_date: date,
    amount: float,
    tax_rate_id: str,
) -> Dict[str, Any]:
    payload = {
        "purchase_credit_note": {
            "contact_id": contact_id,
            "credit_note_number": number,
            "date": credit_date.isoformat(),
            "due_date": (credit_date + timedelta(days=30)).isoformat(),
            "reference": number,
            "net_amount": round(amount, 2),
            "tax_amount": 0.0 if tax_rate_id == "GB_ZERO" else None,
            "total_amount": round(amount, 2),
            "credit_note_lines": [
                {
                    "ledger_account_id": ledger_id,
                    "description": "Purchases",
                    "quantity": 1,
                    "unit_price": round(amount, 2),
                    "net_amount": round(amount, 2),
                    "tax_rate_id": tax_rate_id,
                    "tax_amount": 0.0 if tax_rate_id == "GB_ZERO" else None,
                    "total_amount": round(amount, 2),
                }
            ],
        }
    }
    resp = requests.post(
        f"{SAGE_API_BASE}/purchase_credit_notes",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Session-Company-Id": company_id,
        },
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Post a Sage purchase credit note from an auth code.")
    parser.add_argument("--code", required=True, help="OAuth auth code (GB/...)")
    parser.add_argument("--client-id", default=_env("SAGE_CLIENT_ID"), required=False)
    parser.add_argument("--client-secret", default=_env("SAGE_CLIENT_SECRET"), required=False)
    parser.add_argument("--company-id", default=_env("SAGE_BUSINESS_ID"), required=False)
    parser.add_argument("--contact-id", default=_env("SAGE_CONTACT_ID"), required=False)
    parser.add_argument("--ledger-id", default=_env("SAGE_LEDGER_5001_ID"), required=False)
    parser.add_argument("--number", required=True, help="Credit note number, e.g. PSCN-143858")
    parser.add_argument("--date", required=True, help="Credit date, e.g. 2026-02-05 or 05/02/26")
    parser.add_argument("--amount", required=True, type=float, help="Net/total amount for zero VAT credit")
    parser.add_argument("--tax-rate", default="GB_ZERO", help="Tax rate id, default GB_ZERO")
    args = parser.parse_args()

    missing = [
        name
        for name, value in {
            "client_id": args.client_id,
            "client_secret": args.client_secret,
            "company_id": args.company_id,
            "contact_id": args.contact_id,
            "ledger_id": args.ledger_id,
        }.items()
        if not value
    ]
    if missing:
        raise SystemExit(f"Missing required values: {', '.join(missing)}")

    credit_date = _parse_date(args.date)
    tokens = _exchange_code(args.client_id, args.client_secret, args.code)
    access_token = tokens["access_token"]
    refresh_token = tokens.get("refresh_token")

    result = _post_credit_note(
        access_token=access_token,
        company_id=args.company_id,
        contact_id=args.contact_id,
        ledger_id=args.ledger_id,
        number=args.number,
        credit_date=credit_date,
        amount=args.amount,
        tax_rate_id=args.tax_rate,
    )

    output = {
        "credit_note_id": result.get("id"),
        "displayed_as": result.get("displayed_as"),
        "refresh_token": refresh_token,
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
