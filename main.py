from __future__ import annotations

import base64
import email
from email import policy
from email.parser import BytesParser
import logging
import os
from datetime import date, datetime
import re
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request, status

from app.firestore_queue import (
    enqueue_record,
    get_client_info,
    get_latest_parsed_record,
    get_latest_record,
    has_recent_posted_match,
    list_records,
    test_roundtrip,
    update_record,
)
from app.models import InvoiceData
from app.parsers.clf import parse_clf
from app.parsers.viridian import parse_viridian
from app.parsers.hunts import parse_hunts
from app.parsers.avogel import parse_avogel
from app.parsers.watson_pratt import parse_watson_pratt
from app.parsers.nestle import parse_nestle
from app.parsers.natures_plus import parse_natures_plus
from app.parsers.bionature import parse_bionature
from app.parsers.natures_aid import parse_natures_aid
from app.parsers.tonyrefail import parse_tonyrefail
from app.parsers.hunts import parse_hunts
from app.parse_utils import parse_date
from app.pdf_text import extract_text_from_pdf
from app.sage_client import (
    check_sage_auth,
    debug_refresh,
    debug_refresh_token,
    exchange_auth_code,
    post_purchase_credit_note,
    post_purchase_invoice,
    attach_pdf_to_sage,
    list_attachments,
    search_contacts,
    sage_env_hashes,
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


def _is_duplicate_post(invoice: InvoiceData) -> bool:
    if not FIRESTORE_ENABLED:
        return False


def _duplicate_payload(invoice: InvoiceData) -> Dict[str, Any]:
    invoice_date = (
        invoice.invoice_date.isoformat()
        if hasattr(invoice.invoice_date, "isoformat")
        else str(invoice.invoice_date)
    )
    return {
        "reason": "duplicate_local",
        "supplier_reference": invoice.supplier_reference,
        "invoice_date": invoice_date,
        "is_credit": invoice.is_credit,
    }
    try:
        invoice_date = (
            invoice.invoice_date.isoformat()
            if hasattr(invoice.invoice_date, "isoformat")
            else str(invoice.invoice_date)
        )
        return has_recent_posted_match(
            invoice.supplier_reference,
            invoice_date,
            invoice.is_credit,
        )
    except Exception as exc:
        logger.info("Duplicate check failed, continuing: %s", exc)
        return False


def _invoice_to_dict(invoice: Any) -> Dict[str, Any]:
    if hasattr(invoice, "model_dump"):
        return invoice.model_dump()  # Pydantic v2
    if hasattr(invoice, "dict"):
        return invoice.dict()  # Pydantic v1
    return dict(invoice)


def _serialize_for_storage(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _serialize_for_storage(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize_for_storage(v) for v in value]
    return value


def _invoice_from_payload(payload: Dict[str, Any]) -> InvoiceData:
    supplier_reference = payload.get("supplier_reference")
    invoice_date_raw = payload.get("invoice_date")
    if not supplier_reference or not invoice_date_raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing supplier_reference or invoice_date",
        )

    invoice_date_text = str(invoice_date_raw)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", invoice_date_text):
        try:
            invoice_date = date.fromisoformat(invoice_date_text)
        except ValueError:
            invoice_date = None
    else:
        invoice_date = parse_date(invoice_date_text)
    if not invoice_date:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid invoice_date")

    vat_net = float(payload.get("vat_net", 0) or 0)
    nonvat_net = float(payload.get("nonvat_net", 0) or 0)
    vat_amount = float(payload.get("vat_amount", 0) or 0)
    total = float(payload.get("total", vat_net + nonvat_net + vat_amount) or 0)

    return InvoiceData(
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


def _extract_pdf_from_raw_email(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw = payload.get("RawEmail") or payload.get("RawMessage") or payload.get("RawSource")
    if not raw:
        return None

    raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8", errors="ignore")
    for attempt in range(2):
        try:
            msg = BytesParser(policy=policy.default).parsebytes(raw_bytes)
        except Exception:
            msg = None

        if msg:
            for part in msg.walk():
                content_type = (part.get_content_type() or "").lower()
                filename = (part.get_filename() or "").lower()
                if "pdf" in content_type or filename.endswith(".pdf"):
                    payload_bytes = part.get_payload(decode=True)
                    if payload_bytes:
                        return {
                            "Name": part.get_filename() or "attachment.pdf",
                            "ContentType": content_type,
                            "ContentBytes": payload_bytes,
                        }
        # If first parse failed to find a PDF and the raw data looks base64-ish, try decoding.
        if attempt == 0 and isinstance(raw, str):
            try:
                raw_bytes = base64.b64decode(raw, validate=False)
            except Exception:
                break

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


def _text_looks_like_viridian(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "viridian international" in normalized
        or "viridian nutrition" in normalized
        or "viridian-nutrition.com" in normalized
        or "gb738632315" in normalized
    )


def _text_looks_like_hunts(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "hunt’s food group" in normalized
        or "hunt's food group" in normalized
        or "hunts food group" in normalized
        or "hub@huntsfoodgroup.co.uk" in normalized
        or "vat no: 813 0548 57" in normalized
    )


def _text_looks_like_avogel(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "a.vogel ltd" in normalized
        or "avogel.co.uk" in normalized
        or "vat no. 454 9330 37" in normalized
        or "vat no: 454 9330 37" in normalized
        or "sales i n voice" in normalized
    )


def _text_looks_like_watson_pratt(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "tax invoice" in normalized
        and "invoice number" in normalized
        and "amount gbp" in normalized
        and "vat number 125201466" in normalized
    )


def _text_looks_like_nestle(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "nestle uk ltd" in normalized
        and "sales invoice" in normalized
        and "vat reg no" in normalized
    )


def _text_looks_like_natures_plus(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "naturesplus" in normalized
        or "natures plus" in normalized
        or "vat reg no: gb718284519" in normalized
    )


def _text_looks_like_bionature(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "bio-nature limited" in normalized
        or "bionature.uk.com" in normalized
        or "vat reg no: 847 3436 08" in normalized
        or "vat reg no: 847 3436 08".replace(" ", "") in normalized.replace(" ", "")
    )


def _text_looks_like_natures_aid(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "natures aid ltd" in normalized
        or "naturesaid.co.uk" in normalized
        or "vat reg no: gb 604 7052 68" in normalized
        or "vat reg no: gb604705268" in normalized.replace(" ", "")
    )


def _text_looks_like_tonyrefail(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "tonyrefail apiary" in normalized
        or "tonyrefailapiary@googlemail.com" in normalized
        or "pure welsh honey" in normalized
    )


def _text_looks_like_hunts(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "hunt’s food group" in normalized
        or "hunt's food group" in normalized
        or "hunts food group" in normalized
        or "hub@huntsfoodgroup.co.uk" in normalized
        or "vat no: 813 0548 57" in normalized
    )


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


@app.get("/sage/env-hash")
async def sage_env_hash(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)
    return {"hashes": sage_env_hashes()}


@app.get("/sage/attachments")
async def sage_attachments(request: Request, context_type: str, context_id: str) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not SAGE_ENABLED:
        return {"status": "disabled"}
    try:
        data = list_attachments(context_type, context_id)
        return {"status": "ok", "attachments": data}
    except Exception as exc:
        logger.exception("Sage attachment list failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.get("/sage/contacts/search")
async def sage_contacts_search(request: Request, q: str) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not SAGE_ENABLED:
        return {"status": "disabled"}
    if not q or not q.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing query")
    try:
        return {"status": "ok", **search_contacts(q.strip())}
    except Exception as exc:
        logger.exception("Sage contact search failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.post("/sage/post")
async def sage_post(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON payload must be an object")

    invoice = _invoice_from_payload(payload)

    record_id = None
    if FIRESTORE_ENABLED:
        try:
            record_id = enqueue_record(
                {
                    "status": "manual_post",
                    "source": "manual",
                    "payload_meta": {"source": "sage_post"},
                    "parsed": _serialize_for_storage(_invoice_to_dict(invoice)),
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
        if _is_duplicate_post(invoice):
            duplicate = _duplicate_payload(invoice)
            skip = {"status": "skipped", "reason": "duplicate_local", "number": invoice.supplier_reference}
            if record_id:
                update_record(record_id, {"status": "skipped", "sage": skip, "duplicate": duplicate})
            return {"status": "ok", "sage": skip, "record_id": record_id}
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


@app.post("/sage/post-latest")
async def sage_post_latest(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)

    if not FIRESTORE_ENABLED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Firestore not enabled")

    latest = get_latest_parsed_record()
    if not latest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No parsed records found")

    record_id, record = latest
    parsed = record.get("parsed")
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Latest record missing parsed data")

    invoice = _invoice_from_payload(parsed)

    if not SAGE_ENABLED:
        logger.info("Sage disabled; skipping /sage/post-latest")
        update_record(record_id, {"status": "queued", "reason": "sage_disabled"})
        return {"status": "disabled", "record_id": record_id}

    try:
        if _is_duplicate_post(invoice):
            duplicate = _duplicate_payload(invoice)
            skip = {"status": "skipped", "reason": "duplicate_local", "number": invoice.supplier_reference}
            update_record(record_id, {"status": "skipped", "sage": skip, "duplicate": duplicate})
            return {"status": "ok", "sage": skip, "record_id": record_id}
        if invoice.is_credit:
            sage_result = post_purchase_credit_note(invoice)
        else:
            sage_result = post_purchase_invoice(invoice)
    except Exception as exc:
        logger.exception("Sage post failed: %s", exc)
        update_record(record_id, {"status": "error", "error": str(exc)})
        return {
            "status": "error",
            "message": str(exc),
            "record_id": record_id,
            "debug": {
                "invoice": _invoice_to_dict(invoice),
                "record_id": record_id,
            },
        }

    if isinstance(sage_result, dict) and sage_result.get("id"):
        logger.info("Sage created id: %s", sage_result.get("id"))

    if isinstance(sage_result, dict) and sage_result.get("id"):
        update_record(record_id, {"status": "posted", "sage": sage_result})
    elif isinstance(sage_result, dict) and sage_result.get("status") == "skipped":
        update_record(record_id, {"status": "skipped", "sage": sage_result})
    else:
        update_record(record_id, {"status": "unknown", "sage": sage_result})

    return {"status": "ok", "sage": sage_result, "record_id": record_id}


@app.get("/sage/queue-latest")
async def sage_queue_latest(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)

    if not FIRESTORE_ENABLED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Firestore not enabled")

    latest = get_latest_parsed_record()
    if not latest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No parsed records found")

    record_id, record = latest
    return {"status": "ok", "record_id": record_id, "record": record}


@app.get("/sage/queue-latest-any")
async def sage_queue_latest_any(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)

    if not FIRESTORE_ENABLED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Firestore not enabled")

    latest = get_latest_record()
    if not latest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No records found")

    record_id, record = latest
    return {"status": "ok", "record_id": record_id, "record": record}


@app.get("/sage/firestore-test")
async def sage_firestore_test(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)

    if not FIRESTORE_ENABLED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Firestore not enabled")

    try:
        result = test_roundtrip()
    except Exception as exc:
        logger.exception("Firestore test failed: %s", exc)
        return {"status": "error", "message": str(exc)}

    return {"status": "ok", "result": result}


@app.get("/sage/queue-list")
async def sage_queue_list(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)

    if not FIRESTORE_ENABLED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Firestore not enabled")

    return {"status": "ok", "records": list_records()}


@app.get("/sage/firestore-info")
async def sage_firestore_info(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)
    return {"status": "ok", "info": get_client_info()}


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
    raw_pdf_attachment = None if pdf_attachment else _extract_pdf_from_raw_email(payload)

    if not pdf_attachment and not raw_pdf_attachment:
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

    if raw_pdf_attachment:
        pdf_bytes = raw_pdf_attachment["ContentBytes"]
        pdf_attachment = {
            "Name": raw_pdf_attachment.get("Name"),
            "ContentType": raw_pdf_attachment.get("ContentType"),
            "ContentLength": len(pdf_bytes),
        }
        logger.info("Extracted PDF from RawEmail: %s", pdf_attachment.get("Name"))
    else:
        content = pdf_attachment.get("Content")
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PDF attachment missing content")

        try:
            pdf_bytes = base64.b64decode(content)
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid base64 content") from exc
        logger.info("Extracted PDF from attachment content: %s", pdf_attachment.get("Name"))

    text = extract_text_from_pdf(pdf_bytes)
    _log_pdf_text(text)

    sender_email = _extract_sender_email(payload) or ""
    is_clf_sender = sender_email.endswith("@clfdistribution.com")
    is_viridian_sender = sender_email.endswith("@viridian-nutrition.com")
    is_hunts_sender = sender_email.endswith("@huntsfoodgroup.co.uk")

    invoices: list[InvoiceData]
    if is_hunts_sender or _text_looks_like_hunts(text):
        if not is_hunts_sender:
            logger.info("Sender not Hunts domain but PDF looks like Hunts; using Hunts parser: %s", sender_email)
        invoices = parse_hunts(text)
    elif _text_looks_like_watson_pratt(text):
        invoices = [parse_watson_pratt(text)]
    elif _text_looks_like_nestle(text):
        invoices = [parse_nestle(text)]
    elif _text_looks_like_natures_plus(text):
        invoices = [parse_natures_plus(text)]
    elif _text_looks_like_bionature(text):
        invoices = [parse_bionature(text)]
    elif _text_looks_like_natures_aid(text):
        invoices = [parse_natures_aid(text)]
    elif _text_looks_like_tonyrefail(text):
        invoices = [parse_tonyrefail(text)]
    elif _text_looks_like_avogel(text):
        invoices = [parse_avogel(text)]
    elif is_viridian_sender or _text_looks_like_viridian(text):
        if not is_viridian_sender:
            logger.info("Sender not Viridian domain but PDF looks like Viridian; using Viridian parser: %s", sender_email)
        invoices = [parse_viridian(text)]
    else:
        if not is_clf_sender and _text_looks_like_clf(text):
            is_clf_sender = True
            logger.info("Sender not CLF domain but PDF looks like CLF; using CLF parser: %s", sender_email)
        if not is_clf_sender:
            logger.info("Sender email not CLF; defaulting to CLF parser: %s", sender_email)
        invoices = [parse_clf(text)]

    parsed_payloads = [_invoice_to_dict(inv) for inv in invoices]
    logger.info("Parsed invoice data: %s", parsed_payloads)

    record_ids: list[str] = []
    if FIRESTORE_ENABLED:
        try:
            for inv in invoices:
                record_id = enqueue_record(
                    {
                        "status": "parsed",
                        "source": "postmark",
                        "payload_meta": _payload_meta(payload),
                        "attachment": _attachment_meta(pdf_attachment, len(pdf_bytes)),
                        "parsed": _serialize_for_storage(_invoice_to_dict(inv)),
                    }
                )
                record_ids.append(record_id)
                logger.info("Enqueued Firestore record: %s", record_id)
        except Exception:
            logger.exception("Failed to write Firestore record")

    sage_results: list[dict[str, Any] | None] = []
    if SAGE_ENABLED:
        for idx, inv in enumerate(invoices):
            record_id = record_ids[idx] if idx < len(record_ids) else None
            try:
                if _is_duplicate_post(inv):
                    duplicate = _duplicate_payload(inv)
                    sage_result = {
                        "status": "skipped",
                        "reason": "duplicate_local",
                        "number": inv.supplier_reference,
                    }
                    if record_id:
                        update_record(
                            record_id,
                            {"status": "skipped", "sage": sage_result, "duplicate": duplicate},
                        )
                    sage_results.append(sage_result)
                    continue
                if inv.is_credit:
                    sage_result = post_purchase_credit_note(inv)
                else:
                    sage_result = post_purchase_invoice(inv)
                if isinstance(sage_result, dict):
                    if sage_result.get("id"):
                        logger.info("Sage created id: %s", sage_result.get("id"))
                    elif sage_result.get("status") == "skipped":
                        logger.info("Sage post skipped: %s", sage_result)
                if isinstance(sage_result, dict) and sage_result.get("id"):
                    try:
                        attachment_result = attach_pdf_to_sage(
                            "purchase_credit_note" if inv.is_credit else "purchase_invoice",
                            sage_result["id"],
                            pdf_attachment.get("Name") if pdf_attachment else None,
                            pdf_bytes,
                        )
                        logger.info("Attached PDF to Sage id: %s", sage_result.get("id"))
                        sage_result["attachment"] = {"status": "ok", "id": attachment_result.get("id")}
                    except Exception as exc:
                        logger.exception("Failed to attach PDF to Sage: %s", exc)
                        sage_result["attachment"] = {"status": "error", "message": str(exc)}
                if record_id:
                    if isinstance(sage_result, dict) and sage_result.get("id"):
                        update_record(record_id, {"status": "posted", "sage": sage_result})
                    elif isinstance(sage_result, dict) and sage_result.get("status") == "skipped":
                        update_record(record_id, {"status": "skipped", "sage": sage_result})
                    else:
                        update_record(record_id, {"status": "unknown", "sage": sage_result})
                sage_results.append(sage_result)
            except Exception as exc:
                logger.exception("Sage post failed: %s", exc)
                sage_result = {"status": "error", "message": str(exc)}
                if record_id:
                    update_record(record_id, {"status": "error", "error": str(exc)})
                sage_results.append(sage_result)
    else:
        logger.info("Sage disabled; skipping post")
        for record_id in record_ids:
            update_record(record_id, {"status": "queued", "reason": "sage_disabled"})

    return {
        "status": "ok",
        "max_request_bytes": _max_request_bytes(),
        "parsed": parsed_payloads if len(parsed_payloads) > 1 else parsed_payloads[0],
        "sage": sage_results if len(sage_results) > 1 else (sage_results[0] if sage_results else None),
        "record_id": record_ids if len(record_ids) > 1 else (record_ids[0] if record_ids else None),
    }
