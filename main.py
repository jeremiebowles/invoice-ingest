from __future__ import annotations

import base64
import logging
import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request, status

from app.firestore_queue import enqueue_record, update_record
from app.models import InvoiceData
from app.parsers.clf import parse_clf
from app.parse_utils import parse_date
from app.pdf_text import extract_text_from_pdf
from app.sage_client import (
    check_sage_auth,
    debug_refresh,
    debug_refresh_token,
    exchange_auth_code,
    post_purchase_credit_note,
    post_purchase_invoice,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("invoice-ingest")

app = FastAPI()


BASIC_USER = os.getenv("BASIC_USER")
BASIC_PASS = os.getenv("BASIC_PASS")
MAX_REQUEST_BYTES = os.getenv("MAX_REQUEST_BYTES")
LOG_PDF_TEXT = os.getenv("LOG_PDF_TEXT", "").lower() in {"1", "true", "yes", "on"}
SAGE_ENABLED = os.getenv("SAGE_ENABLED", "").lower() in {"1", "true", "yes", "on"}
FIRESTORE_ENABLED = os.getenv("FIRESTORE_ENABLED", "").lower() in {"1", "true", "yes", "on"}


def _max_request_bytes() -> Optional[int]:
    if not MAX_REQUEST_BYTES:
        return None
    try:
        return int(MAX_REQUEST_BYTES)
    except ValueError:
        logger.warning("Invalid MAX_REQUEST_BYTES value: %s", MAX_REQUEST_BYTES)
        return None


def _check_basic_auth(request: Request) -> None:
    if BASIC_USER is None or BASIC_PASS is None:
        logger.error("BASIC_USER/BASIC_PASS not configured")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Auth not configured")

    auth_header = request.headers.get("authorization")
    if not auth_header or not auth_header.lower().startswith("basic "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

    token = auth_header.split(" ", 1)[1].strip()
    try:
        decoded = base64.b64decode(token).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

    if ":" not in decoded:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

    username, password = decoded.split(":", 1)
    if username != BASIC_USER or password != BASIC_PASS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )


def _enforce_request_size(request: Request) -> None:
    limit = _max_request_bytes()
    if limit is None:
        return

    content_length = request.headers.get("content-length")
    if content_length is None:
        return

    try:
        length = int(content_length)
    except ValueError:
        return

    if length > limit:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Request too large: {length} bytes (max {limit})",
        )


def _invoice_to_dict(invoice: Any) -> Dict[str, Any]:
    if hasattr(invoice, "model_dump"):
        return invoice.model_dump()  # Pydantic v2
    if hasattr(invoice, "dict"):
        return invoice.dict()  # Pydantic v1
    return dict(invoice)


def _log_pdf_text(text: str) -> None:
    if not LOG_PDF_TEXT:
        return
    logger.info("Extracted text head: %s", text[:800])
    logger.info("Extracted text tail: %s", text[-1200:])


def _find_first_pdf_attachment(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    attachments = payload.get("Attachments") or []
    if not isinstance(attachments, list):
        return None

    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        content_type = (attachment.get("ContentType") or "").lower()
        name = (attachment.get("Name") or "").lower()
        if "pdf" in content_type or name.endswith(".pdf"):
            if attachment.get("Content"):
                return attachment

    return None


def _extract_sender_email(payload: Dict[str, Any]) -> Optional[str]:
    from_full = payload.get("FromFull") or {}
    if isinstance(from_full, dict):
        email = from_full.get("Email")
        if isinstance(email, str) and email.strip():
            return email.strip().lower()
    from_header = payload.get("From")
    if isinstance(from_header, str) and from_header.strip():
        return from_header.strip().lower()
    return None


def _text_looks_like_clf(text: str) -> bool:
    normalized = (text or "").lower()
    return "clf distribution" in normalized or "clf distribution ltd" in normalized


def _payload_meta(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "message_id": payload.get("MessageID") or payload.get("MessageId"),
        "subject": payload.get("Subject"),
        "from": payload.get("From"),
        "from_email": (payload.get("FromFull") or {}).get("Email"),
        "to": payload.get("To"),
    }


def _attachment_meta(
    attachment: Dict[str, Any], pdf_size: Optional[int] = None
) -> Dict[str, Any]:
    return {
        "name": attachment.get("Name"),
        "content_type": attachment.get("ContentType"),
        "size_bytes": pdf_size,
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/sage/health")
async def sage_health() -> Dict[str, Any]:
    if not SAGE_ENABLED:
        return {"status": "disabled"}
    return check_sage_auth()


@app.get("/sage/test-refresh")
async def sage_test_refresh(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not SAGE_ENABLED:
        return {"status": "disabled"}
    return debug_refresh()


@app.post("/sage/test-refresh-token")
async def sage_test_refresh_token(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not SAGE_ENABLED:
        return {"status": "disabled"}
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON payload must be an object")

    refresh_token = payload.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing refresh_token")

    return debug_refresh_token(str(refresh_token).strip())


@app.post("/sage/post")
async def sage_post(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON payload must be an object")

    supplier_reference = payload.get("supplier_reference")
    invoice_date_raw = payload.get("invoice_date")
    if not supplier_reference or not invoice_date_raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing supplier_reference or invoice_date",
        )

    invoice_date = parse_date(str(invoice_date_raw))
    if not invoice_date:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid invoice_date")

    vat_net = float(payload.get("vat_net", 0) or 0)
    nonvat_net = float(payload.get("nonvat_net", 0) or 0)
    vat_amount = float(payload.get("vat_amount", 0) or 0)
    total = float(payload.get("total", vat_net + nonvat_net + vat_amount) or 0)

    invoice = InvoiceData(
        supplier=payload.get("supplier") or "CLF",
        supplier_reference=str(supplier_reference),
        invoice_date=invoice_date,
        due_date=None,
        description=payload.get("description") or "Purchases",
        is_credit=bool(payload.get("is_credit", False)),
        deliver_to_postcode=payload.get("deliver_to_postcode"),
        ledger_account=payload.get("ledger_account"),
        vat_net=vat_net,
        nonvat_net=nonvat_net,
        vat_amount=vat_amount,
        total=total,
        warnings=[],
    )

    record_id = None
    if FIRESTORE_ENABLED:
        try:
            record_id = enqueue_record(
                {
                    "status": "manual_post",
                    "source": "manual",
                    "payload_meta": {"source": "sage_post"},
                    "parsed": _invoice_to_dict(invoice),
                }
            )
        except Exception:
            logger.exception("Failed to write Firestore record for manual post")

    if not SAGE_ENABLED:
        logger.info("Sage disabled; skipping /sage/post")
        if record_id:
            update_record(record_id, {"status": "queued", "reason": "sage_disabled"})
        return {"status": "disabled", "record_id": record_id}

    try:
        if invoice.is_credit:
            sage_result = post_purchase_credit_note(invoice)
        else:
            sage_result = post_purchase_invoice(invoice)
    except Exception as exc:
        logger.exception("Sage post failed: %s", exc)
        if record_id:
            update_record(record_id, {"status": "error", "error": str(exc)})
        return {"status": "error", "message": str(exc), "record_id": record_id}

    if isinstance(sage_result, dict) and sage_result.get("id"):
        logger.info("Sage created id: %s", sage_result.get("id"))

    if record_id:
        if isinstance(sage_result, dict) and sage_result.get("id"):
            update_record(record_id, {"status": "posted", "sage": sage_result})
        elif isinstance(sage_result, dict) and sage_result.get("status") == "skipped":
            update_record(record_id, {"status": "skipped", "sage": sage_result})
        else:
            update_record(record_id, {"status": "unknown", "sage": sage_result})

    return {"status": "ok", "sage": sage_result, "record_id": record_id}


@app.get("/sage/auth-url")
async def sage_auth_url(request: Request) -> Dict[str, str]:
    _check_basic_auth(request)
    client_id = os.getenv("SAGE_CLIENT_ID")
    if not client_id:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing SAGE_CLIENT_ID")
    url = (
        "https://www.sageone.com/oauth2/auth/central"
        "?filter=apiv3.1"
        "&response_type=code"
        f"&client_id={client_id.replace('/', '%2F')}"
        "&redirect_uri=https%3A%2F%2Foauth.pstmn.io%2Fv1%2Fbrowser-callback"
        "&scope=full_access"
        "&state=123"
    )
    return {"url": url}


@app.post("/sage/exchange")
async def sage_exchange(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON payload must be an object")

    code = payload.get("code")
    if not code:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing code")

    try:
        tokens = exchange_auth_code(str(code).strip())
    except Exception as exc:
        logger.exception("Sage exchange failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Sage exchange failed: {exc}",
        ) from exc

    return {
        "status": "ok",
        "refresh_token": tokens.get("refresh_token"),
        "expires_in": tokens.get("expires_in"),
    }


@app.post("/postmark/inbound")
async def postmark_inbound(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)
    _enforce_request_size(request)

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON payload must be an object")

    pdf_attachment = _find_first_pdf_attachment(payload)
    if not pdf_attachment:
        if FIRESTORE_ENABLED:
            try:
                enqueue_record(
                    {
                        "status": "no_pdf",
                        "source": "postmark",
                        "payload_meta": _payload_meta(payload),
                    }
                )
            except Exception:
                logger.exception("Failed to write Firestore record for no-pdf payload")
        return {
            "status": "ok",
            "max_request_bytes": _max_request_bytes(),
            "message": "No PDF attachment found",
        }

    content = pdf_attachment.get("Content")
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PDF attachment missing content")

    try:
        pdf_bytes = base64.b64decode(content)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid base64 content") from exc

    text = extract_text_from_pdf(pdf_bytes)
    _log_pdf_text(text)

    sender_email = _extract_sender_email(payload) or ""
    is_clf_sender = sender_email.endswith("@clfdistribution.com")
    if not is_clf_sender and _text_looks_like_clf(text):
        is_clf_sender = True
        logger.info("Sender not CLF domain but PDF looks like CLF; using CLF parser: %s", sender_email)
    if not is_clf_sender:
        logger.info("Sender email not CLF; defaulting to CLF parser: %s", sender_email)

    invoice = parse_clf(text)
    logger.info("Parsed invoice data: %s", _invoice_to_dict(invoice))

    record_id = None
    if FIRESTORE_ENABLED:
        try:
            record_id = enqueue_record(
                {
                    "status": "parsed",
                    "source": "postmark",
                    "payload_meta": _payload_meta(payload),
                    "attachment": _attachment_meta(pdf_attachment, len(pdf_bytes)),
                    "parsed": _invoice_to_dict(invoice),
                }
            )
        except Exception:
            logger.exception("Failed to write Firestore record")

    sage_result = None
    if SAGE_ENABLED:
        try:
            if invoice.is_credit:
                sage_result = post_purchase_credit_note(invoice)
            else:
                sage_result = post_purchase_invoice(invoice)
            if isinstance(sage_result, dict):
                if sage_result.get("id"):
                    logger.info("Sage created id: %s", sage_result.get("id"))
                elif sage_result.get("status") == "skipped":
                    logger.info("Sage post skipped: %s", sage_result)
            if record_id:
                if isinstance(sage_result, dict) and sage_result.get("id"):
                    update_record(record_id, {"status": "posted", "sage": sage_result})
                elif isinstance(sage_result, dict) and sage_result.get("status") == "skipped":
                    update_record(record_id, {"status": "skipped", "sage": sage_result})
                else:
                    update_record(record_id, {"status": "unknown", "sage": sage_result})
        except Exception as exc:
            logger.exception("Sage post failed: %s", exc)
            sage_result = {"status": "error", "message": str(exc)}
            if record_id:
                update_record(record_id, {"status": "error", "error": str(exc)})
    else:
        logger.info("Sage disabled; skipping post")
        if record_id:
            update_record(record_id, {"status": "queued", "reason": "sage_disabled"})

    return {
        "status": "ok",
        "max_request_bytes": _max_request_bytes(),
        "parsed": _invoice_to_dict(invoice),
        "sage": sage_result,
        "record_id": record_id,
    }
