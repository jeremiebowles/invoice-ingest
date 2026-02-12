from __future__ import annotations

import os
import hashlib
import logging
import base64
from datetime import date, timedelta
from typing import Any, Dict, Optional

import requests
from google.cloud import secretmanager

from app.models import InvoiceData

SAGE_TOKEN_URL = "https://oauth.accounting.sage.com/token"
SAGE_API_BASE = "https://api.accounting.sage.com/v3.1"
logger = logging.getLogger(__name__)

_ATTACHMENT_CONTEXT_TYPES = {
    "purchase_invoice": "PURCHASE_INVOICE",
    "purchase_credit_note": "PURCHASE_CREDIT_NOTE",
}
_ATTACHMENT_CONTEXT_TYPE_IDS: dict[str, str] = {}


def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value.strip() if value else None


def _sha256(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sage_env_hashes() -> Dict[str, Optional[str]]:
    return {
        "SAGE_CLIENT_ID": _sha256(_get_env("SAGE_CLIENT_ID")),
        "SAGE_CLIENT_SECRET": _sha256(_get_env("SAGE_CLIENT_SECRET")),
        "SAGE_REFRESH_TOKEN": _sha256(_get_env("SAGE_REFRESH_TOKEN")),
        "SAGE_REFRESH_SECRET_NAME": _sha256(_get_env("SAGE_REFRESH_SECRET_NAME")),
    }


def _get_refresh_token() -> Optional[str]:
    secret_name = _get_env("SAGE_REFRESH_SECRET_NAME")
    if secret_name:
        client = secretmanager.SecretManagerServiceClient()
        version = client.access_secret_version(name=f"{secret_name}/versions/latest")
        return version.payload.data.decode("utf-8").strip()
    return _get_env("SAGE_REFRESH_TOKEN")


def _store_refresh_token(refresh_token: str) -> None:
    secret_name = _get_env("SAGE_REFRESH_SECRET_NAME")
    if not secret_name:
        return
    client = secretmanager.SecretManagerServiceClient()
    client.add_secret_version(
        parent=secret_name,
        payload={"data": refresh_token.encode("utf-8")},
    )


def _refresh_access_token() -> str:
    client_id = _get_env("SAGE_CLIENT_ID")
    client_secret = _get_env("SAGE_CLIENT_SECRET")
    refresh_token = _get_refresh_token()

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
    if resp.status_code >= 400:
        # The default requests exception message drops the response body, which is where
        # Sage tells us whether this is invalid_grant, invalid_client, etc.
        snippet = (resp.text or "")[:2000]
        body: Dict[str, Any]
        try:
            body = resp.json()
        except Exception:
            body = {"raw": snippet}

        # Do not log tokens even if Sage ever returned them unexpectedly.
        redacted = dict(body) if isinstance(body, dict) else {"raw": snippet}
        for k in ("access_token", "refresh_token", "id_token"):
            if k in redacted:
                redacted[k] = "<redacted>"

        logger.error(
            "Sage token refresh failed: HTTP %s body=%s env_hashes=%s",
            resp.status_code,
            redacted,
            sage_env_hashes(),
        )
        raise RuntimeError(f"Sage token refresh failed: HTTP {resp.status_code} {redacted}")

    data = resp.json()
    new_refresh = data.get("refresh_token")
    if new_refresh:
        try:
            _store_refresh_token(new_refresh)
        except Exception:
            logger.critical(
                "CRITICAL: Failed to store rotated refresh token in Secret Manager. "
                "The old token is now invalid. "
                "New token hash: %s",
                _sha256(new_refresh),
                exc_info=True,
            )
            raise
    return data["access_token"]


def check_sage_auth() -> Dict[str, Any]:
    try:
        access_token = _refresh_access_token()
    except Exception as exc:
        return {"status": "error", "message": str(exc)}
    return {"status": "ok", "access_token": access_token[:10] + "..."}


def debug_refresh() -> Dict[str, Any]:
    client_id = _get_env("SAGE_CLIENT_ID")
    client_secret = _get_env("SAGE_CLIENT_SECRET")
    refresh_token = _get_refresh_token()

    if not client_id or not client_secret or not refresh_token:
        return {"status": "error", "message": "Missing Sage OAuth env vars"}

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
    if resp.status_code >= 400:
        try:
            body = resp.json()
        except ValueError:
            body = {"raw": resp.text[:2000]}
        redacted = dict(body) if isinstance(body, dict) else {"raw": resp.text[:2000]}
        for k in ("access_token", "refresh_token", "id_token"):
            if k in redacted:
                redacted[k] = "<redacted>"
        logger.error(
            "Sage debug refresh failed: HTTP %s body=%s env_hashes=%s",
            resp.status_code,
            redacted,
            sage_env_hashes(),
        )
        return {"status": "error", "http_status": resp.status_code, "body": body}
    return {"status": "ok"}


def debug_refresh_token(refresh_token: str) -> Dict[str, Any]:
    client_id = _get_env("SAGE_CLIENT_ID")
    client_secret = _get_env("SAGE_CLIENT_SECRET")

    if not client_id or not client_secret:
        return {"status": "error", "message": "Missing Sage OAuth env vars"}

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
    if resp.status_code >= 400:
        try:
            body = resp.json()
        except ValueError:
            body = {"raw": resp.text[:2000]}
        redacted = dict(body) if isinstance(body, dict) else {"raw": resp.text[:2000]}
        for k in ("access_token", "refresh_token", "id_token"):
            if k in redacted:
                redacted[k] = "<redacted>"
        logger.error(
            "Sage debug refresh token failed: HTTP %s body=%s env_hashes=%s",
            resp.status_code,
            redacted,
            sage_env_hashes(),
        )
        return {"status": "error", "http_status": resp.status_code, "body": body}
    return {"status": "ok"}

def exchange_auth_code(code: str, redirect_uri: str = "https://oauth.pstmn.io/v1/browser-callback") -> Dict[str, Any]:
    client_id = _get_env("SAGE_CLIENT_ID")
    client_secret = _get_env("SAGE_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("Missing Sage OAuth env vars")

    resp = requests.post(
        SAGE_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    new_refresh = data.get("refresh_token")
    if new_refresh:
        try:
            _store_refresh_token(new_refresh)
            logger.info("Stored refresh token from code exchange in Secret Manager")
        except Exception:
            logger.critical(
                "CRITICAL: Failed to store refresh token from code exchange. "
                "Token hash: %s -- manually store this token or re-auth.",
                _sha256(new_refresh),
                exc_info=True,
            )
            # Don't re-raise here: return the tokens so the caller can recover manually.

    return data


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


def _build_invoice_lines(
    invoice: InvoiceData, ledger_account_id: str
) -> tuple[list[Dict[str, Any]], float, float, float]:
    tax_standard_id, tax_zero_id = _get_tax_rate_ids()

    vat_net = round(invoice.vat_net, 2)
    nonvat_net = round(invoice.nonvat_net, 2)
    vat_amount = round(invoice.vat_amount, 2)
    net_amount = round(vat_net + nonvat_net, 2)
    total_amount = round(net_amount + vat_amount, 2)

    invoice_lines: list[Dict[str, Any]] = []
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

    return invoice_lines, net_amount, vat_amount, total_amount


def _sage_headers(access_token: str, business_id: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Session-Company-Id": business_id,
    }


def _raise_for_status_with_body(resp: requests.Response, context: str) -> None:
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        body_text = resp.text or ""
        snippet = body_text[:1000]
        logger.error("%s failed: HTTP %s", context, resp.status_code)
        if snippet:
            logger.error("%s response body: %s", context, snippet)
        try:
            data = resp.json()
            logger.error("%s response json: %s", context, data)
        except Exception:
            pass
        raise RuntimeError(f"{context} failed: HTTP {resp.status_code} {snippet}") from exc


def _already_exists(
    access_token: str,
    business_id: str,
    endpoint: str,
    number: str,
    number_field: str,
    contact_id: Optional[str] = None,
) -> bool:
    if not number or number == "UNKNOWN":
        return False
    target = str(number).strip().upper()

    def _norm(value: Any) -> str:
        return str(value).strip().upper()

    def _matches(item: Dict[str, Any]) -> bool:
        for key in (number_field, "reference", "vendor_reference", "displayed_as"):
            value = item.get(key)
            if value and _norm(value) == target:
                return True
        return False

    contact_filter = f" and contact_id eq '{contact_id}'" if contact_id else ""
    candidates = [
        {"search": number, "items_per_page": 50},
        {number_field: number},
        {"reference": number},
        {"vendor_reference": number},
        {"filter": f"{number_field} eq '{number}'"},
        {"filter": f"reference eq '{number}'"},
        {"filter": f"vendor_reference eq '{number}'"},
        {"filter": f"vendor_reference eq '{number}'{contact_filter}"},
        {"filter": f"reference eq '{number}'{contact_filter}"},
        {"filter": f"{number_field} eq '{number}'{contact_filter}"},
    ]
    if contact_id:
        candidates.insert(
            0, {"contact_id": contact_id, "vendor_reference": number, "items_per_page": 50}
        )
        candidates.insert(1, {"contact_id": contact_id, "reference": number, "items_per_page": 50})
    for params in candidates:
        try:
            resp = requests.get(
                f"{SAGE_API_BASE}/{endpoint}",
                headers=_sage_headers(access_token, business_id),
                params=params,
                timeout=30,
            )
            if resp.status_code >= 400:
                continue
            data = resp.json()
            items = data.get("$items") or []
            for item in items:
                if not isinstance(item, dict):
                    continue
                if _matches(item):
                    logger.info(
                        "Sage duplicate match on %s for %s",
                        endpoint,
                        number,
                        extra={"params": params, "id": item.get("id")},
                    )
                    return True
                if contact_id and item.get("contact", {}).get("id") == contact_id:
                    if _matches(item):
                        logger.info(
                            "Sage duplicate match on %s for %s (contact)",
                            endpoint,
                            number,
                            extra={"params": params, "id": item.get("id")},
                        )
                        return True
            if items:
                logger.info(
                    "Sage duplicate candidate(s) returned for %s on %s",
                    number,
                    endpoint,
                    extra={"params": params, "count": len(items)},
                )
        except Exception as exc:
            logger.info("Sage duplicate check failed for %s: %s", params, exc)
            continue

    try:
        resp = requests.get(
            f"{SAGE_API_BASE}/transactions",
            headers=_sage_headers(access_token, business_id),
            params={"search": number, "items_per_page": 50},
            timeout=30,
        )
        if resp.status_code < 400:
            data = resp.json()
            items = data.get("$items") or []
            for item in items:
                if not isinstance(item, dict):
                    continue
                if _matches(item):
                    logger.info(
                        "Sage duplicate match on transactions for %s",
                        number,
                        extra={"id": item.get("id")},
                    )
                    return True
    except Exception as exc:
        logger.info("Sage duplicate check failed for transactions: %s", exc)
    return False


def post_purchase_invoice(invoice: InvoiceData) -> Dict[str, Any]:
    business_id = _get_env("SAGE_BUSINESS_ID")
    contact_id = invoice.contact_id or _get_env("SAGE_CONTACT_ID")
    if not business_id or not contact_id:
        raise RuntimeError("Missing Sage business/contact configuration")

    ledger_account_id = _get_ledger_account_id(invoice)
    if not ledger_account_id:
        raise RuntimeError("Missing Sage ledger account mapping")

    access_token = _refresh_access_token()
    invoice_lines, net_amount, vat_amount, total_amount = _build_invoice_lines(
        invoice, ledger_account_id
    )
    due_date = _compute_due_date(invoice.invoice_date)

    if _already_exists(
        access_token,
        business_id,
        "purchase_invoices",
        invoice.supplier_reference,
        "invoice_number",
        contact_id,
    ):
        return {"status": "skipped", "reason": "already_exists", "number": invoice.supplier_reference}

    payload = {
        "purchase_invoice": {
            "contact_id": contact_id,
            "date": invoice.invoice_date.isoformat(),
            "due_date": due_date.isoformat(),
            "reference": invoice.supplier_reference,
            "invoice_number": invoice.supplier_reference,
            "vendor_reference": invoice.supplier_reference,
            "net_amount": net_amount,
            "tax_amount": vat_amount,
            "total_amount": total_amount,
            "invoice_lines": invoice_lines,
        }
    }

    resp = requests.post(
        f"{SAGE_API_BASE}/purchase_invoices",
        headers=_sage_headers(access_token, business_id),
        json=payload,
        timeout=30,
    )
    _raise_for_status_with_body(resp, "Sage purchase invoice")
    return resp.json()


def post_purchase_credit_note(invoice: InvoiceData) -> Dict[str, Any]:
    business_id = _get_env("SAGE_BUSINESS_ID")
    contact_id = invoice.contact_id or _get_env("SAGE_CONTACT_ID")
    if not business_id or not contact_id:
        raise RuntimeError("Missing Sage business/contact configuration")

    ledger_account_id = _get_ledger_account_id(invoice)
    if not ledger_account_id:
        raise RuntimeError("Missing Sage ledger account mapping")

    access_token = _refresh_access_token()
    invoice_lines, net_amount, vat_amount, total_amount = _build_invoice_lines(
        invoice, ledger_account_id
    )
    due_date = _compute_due_date(invoice.invoice_date)

    if _already_exists(
        access_token,
        business_id,
        "purchase_credit_notes",
        invoice.supplier_reference,
        "credit_note_number",
        contact_id,
    ):
        return {"status": "skipped", "reason": "already_exists", "number": invoice.supplier_reference}

    payload = {
        "purchase_credit_note": {
            "contact_id": contact_id,
            "date": invoice.invoice_date.isoformat(),
            "due_date": due_date.isoformat(),
            "reference": invoice.supplier_reference,
            "credit_note_number": invoice.supplier_reference,
            "vendor_reference": invoice.supplier_reference,
            "net_amount": net_amount,
            "tax_amount": vat_amount,
            "total_amount": total_amount,
            "credit_note_lines": invoice_lines,
        }
    }

    resp = requests.post(
        f"{SAGE_API_BASE}/purchase_credit_notes",
        headers=_sage_headers(access_token, business_id),
        json=payload,
        timeout=30,
    )
    _raise_for_status_with_body(resp, "Sage purchase credit note")
    return resp.json()


def sage_invoice_exists(invoice: InvoiceData) -> bool:
    business_id = _get_env("SAGE_BUSINESS_ID")
    contact_id = invoice.contact_id or _get_env("SAGE_CONTACT_ID")
    if not business_id or not contact_id:
        raise RuntimeError("Missing Sage business/contact configuration")

    access_token = _refresh_access_token()
    if invoice.is_credit:
        return _already_exists(
            access_token,
            business_id,
            "purchase_credit_notes",
            invoice.supplier_reference,
            "credit_note_number",
            contact_id,
        )
    return _already_exists(
        access_token,
        business_id,
        "purchase_invoices",
        invoice.supplier_reference,
        "invoice_number",
        contact_id,
    )


def attach_pdf_to_sage(
    context_type: str,
    context_id: str,
    filename: str,
    pdf_bytes: bytes,
) -> Dict[str, Any]:
    if not pdf_bytes:
        raise RuntimeError("Missing PDF bytes for attachment")
    business_id = _get_env("SAGE_BUSINESS_ID")
    if not business_id:
        raise RuntimeError("Missing Sage business configuration")

    context_type_name = _ATTACHMENT_CONTEXT_TYPES.get(context_type)
    if not context_type_name:
        raise RuntimeError(f"Unknown attachment context type: {context_type}")

    access_token = _refresh_access_token()
    context_type_id = _get_attachment_context_type_id(access_token, business_id, context_type_name)
    encoded = base64.b64encode(pdf_bytes).decode("utf-8")
    file_name = filename or "invoice.pdf"
    if not file_name.lower().endswith(".pdf"):
        file_name = f"{file_name}.pdf"

    payload = {
        "attachment": {
            "file": encoded,
            "file_name": file_name,
            "mime_type": "application/pdf",
            "description": "Uploaded via API",
            "file_extension": ".pdf",
            "attachment_context_id": context_id,
            "attachment_context_type_id": context_type_id,
        }
    }

    resp = requests.post(
        f"{SAGE_API_BASE}/attachments",
        headers=_sage_headers(access_token, business_id),
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def list_attachments(context_type: str, context_id: str, limit: int = 20) -> Dict[str, Any]:
    business_id = _get_env("SAGE_BUSINESS_ID")
    if not business_id:
        raise RuntimeError("Missing Sage business configuration")
    context_type_name = _ATTACHMENT_CONTEXT_TYPES.get(context_type)
    if not context_type_name:
        raise RuntimeError(f"Unknown attachment context type: {context_type}")

    access_token = _refresh_access_token()
    context_type_id = _get_attachment_context_type_id(access_token, business_id, context_type_name)
    resp = requests.get(
        f"{SAGE_API_BASE}/attachments",
        headers=_sage_headers(access_token, business_id),
        params={
            "attachment_context_id": context_id,
            "attachment_context_type_id": context_type_id,
            "items_per_page": limit,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _get_attachment_context_type_id(
    access_token: str, business_id: str, context_type_name: str
) -> str:
    cached = _ATTACHMENT_CONTEXT_TYPE_IDS.get(context_type_name)
    if cached:
        return cached
    resp = requests.get(
        f"{SAGE_API_BASE}/attachment_context_types",
        headers=_sage_headers(access_token, business_id),
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data.get("$items") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("id") == context_type_name or item.get("displayed_as") == context_type_name:
            ctx_id = item.get("id")
            if ctx_id:
                _ATTACHMENT_CONTEXT_TYPE_IDS[context_type_name] = ctx_id
                return ctx_id
    raise RuntimeError(f"Attachment context type not found: {context_type_name}")


def search_contacts(query: str, limit: int = 20) -> Dict[str, Any]:
    business_id = _get_env("SAGE_BUSINESS_ID")
    if not business_id:
        raise RuntimeError("Missing Sage business configuration")

    access_token = _refresh_access_token()
    resp = requests.get(
        f"{SAGE_API_BASE}/contacts",
        headers=_sage_headers(access_token, business_id),
        params={"search": query, "items_per_page": limit},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data.get("$items") or []
    simplified = []
    for item in items:
        if not isinstance(item, dict):
            continue
        simplified.append(
            {
                "id": item.get("id"),
                "displayed_as": item.get("displayed_as"),
                "name": item.get("name"),
                "reference": item.get("reference"),
                "email": item.get("email"),
            }
        )
    return {"count": len(simplified), "items": simplified}


def search_purchase_credit_notes(query: str, limit: int = 20) -> Dict[str, Any]:
    business_id = _get_env("SAGE_BUSINESS_ID")
    if not business_id:
        raise RuntimeError("Missing Sage business configuration")

    access_token = _refresh_access_token()
    resp = requests.get(
        f"{SAGE_API_BASE}/purchase_credit_notes",
        headers=_sage_headers(access_token, business_id),
        params={"search": query, "items_per_page": limit},
        timeout=30,
    )
    _raise_for_status_with_body(resp, "Sage credit note search")
    data = resp.json() if resp.content else {}
    items = data.get("$items") or []
    simplified = []
    for item in items:
        if not isinstance(item, dict):
            continue
        simplified.append(
            {
                "id": item.get("id"),
                "displayed_as": item.get("displayed_as"),
                "reference": item.get("reference"),
                "vendor_reference": item.get("vendor_reference"),
                "date": item.get("date"),
                "total_amount": item.get("total_amount"),
            }
        )
    return {"count": len(simplified), "items": simplified}


def _count_endpoint(
    access_token: str,
    business_id: str,
    endpoint: str,
    date_from: str,
    date_to: str,
) -> int:
    params = {"from_date": date_from, "to_date": date_to, "items_per_page": 1}
    resp = requests.get(
        f"{SAGE_API_BASE}/{endpoint}",
        headers=_sage_headers(access_token, business_id),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json() if resp.content else {}
    total = data.get("$total")
    if isinstance(total, int):
        return total
    if isinstance(total, str) and total.isdigit():
        return int(total)
    items = data.get("$items") or []
    return len(items)


def count_purchase_invoices(
    date_from: str, date_to: str, include_credits: bool = False
) -> Dict[str, Any]:
    business_id = _get_env("SAGE_BUSINESS_ID")
    if not business_id:
        raise RuntimeError("Missing Sage business configuration")

    access_token = _refresh_access_token()
    invoices = _count_endpoint(access_token, business_id, "purchase_invoices", date_from, date_to)
    credits = 0
    if include_credits:
        credits = _count_endpoint(
            access_token, business_id, "purchase_credit_notes", date_from, date_to
        )
    return {"invoices": invoices, "credits": credits, "total": invoices + credits}
