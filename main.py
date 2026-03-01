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

from urllib.parse import quote, urlencode

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse

from app.firestore_queue import (
    enqueue_record,
    get_client_info,
    get_latest_parsed_record,
    get_latest_record,
    has_recent_posted_match,
    increment_rate_limit,
    list_records,
    find_records_by_reference,
    get_reference_lock,
    reserve_reference,
    reserve_message_id,
    test_roundtrip,
    update_record,
    update_message_status,
)
from app.models import InvoiceData
from app.parsers.clf import parse_clf
from app.parsers.viridian import parse_viridian
from app.parsers.hunts import parse_hunts
from app.parsers.absolute_aromas import parse_absolute_aromas
from app.parsers.pestokill import parse_pestokill
from app.parsers.avogel import parse_avogel
from app.parsers.emporio import parse_emporio
from app.parsers.watson_pratt import parse_watson_pratt
from app.parsers.nestle import parse_nestle
from app.parsers.natures_plus import parse_natures_plus
from app.parsers.bionature import parse_bionature
from app.parsers.natures_aid import parse_natures_aid
from app.parsers.tonyrefail import parse_tonyrefail
from app.parsers.essential import parse_essential
from app.parsers.lewtress import parse_lewtress
from app.parsers.biocare import parse_biocare
from app.parsers.kinetic import parse_kinetic
from app.parsers.hunts import parse_hunts
from app.parse_utils import parse_date
from app.pdf_text import extract_text_from_image, extract_text_from_pdf
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
    search_purchase_credit_notes,
    sage_invoice_exists,
    count_purchase_invoices,
    sage_env_hashes,
    search_purchase_invoices_by_reference,
    void_purchase_invoice,
    find_sage_invoice_id,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("invoice-ingest")

APP_VERSION = os.getenv("APP_VERSION", "1bc2f0e")

app = FastAPI()


@app.get("/version")
async def version() -> Dict[str, Any]:
    return {
        "status": "ok",
        "revision": os.getenv("K_REVISION"),
        "service": os.getenv("K_SERVICE"),
        "commit": os.getenv("COMMIT_SHA") or os.getenv("REVISION_ID"),
        "app_version": APP_VERSION,
    }


BASIC_USER = os.getenv("BASIC_USER")
BASIC_PASS = os.getenv("BASIC_PASS")
MAX_REQUEST_BYTES = os.getenv("MAX_REQUEST_BYTES")
LOG_PDF_TEXT = os.getenv("LOG_PDF_TEXT", "").lower() in {"1", "true", "yes", "on"}
SAGE_ENABLED = os.getenv("SAGE_ENABLED", "").lower() in {"1", "true", "yes", "on"}
FIRESTORE_ENABLED = os.getenv("FIRESTORE_ENABLED", "").lower() in {"1", "true", "yes", "on"}
ALLOWED_FORWARDERS = os.getenv("ALLOWED_FORWARDERS", "")
BLOCKLIST_KEYWORDS = os.getenv("BLOCKLIST_KEYWORDS", "")
RATE_LIMIT_PER_DAY = os.getenv("RATE_LIMIT_PER_DAY", "20")
REQUIRE_SES_SOURCE = os.getenv("REQUIRE_SES_SOURCE", "").lower() in {"1", "true", "yes", "on"}


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


def _forwarder_whitelist() -> list[str]:
    if not ALLOWED_FORWARDERS:
        return []
    return [addr.strip().lower() for addr in ALLOWED_FORWARDERS.split(",") if addr.strip()]


def _enforce_forwarder_whitelist(sender_email: str) -> None:
    allowed = _forwarder_whitelist()
    if not allowed:
        return
    if sender_email.lower() not in allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Sender not allowed",
        )


def _blocklist_keywords() -> list[str]:
    if not BLOCKLIST_KEYWORDS:
        return []
    return [kw.strip().lower() for kw in BLOCKLIST_KEYWORDS.split(",") if kw.strip()]


def _enforce_blocklist(subject: str) -> None:
    keywords = _blocklist_keywords()
    if not keywords:
        return
    subject_lower = (subject or "").lower()
    for kw in keywords:
        if kw and kw in subject_lower:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Blocked by subject keyword",
            )


def _rate_limit_key(sender_email: str, now: Optional[datetime] = None) -> str:
    stamp = (now or datetime.utcnow()).strftime("%Y-%m-%d")
    return f"{sender_email.lower()}:{stamp}"


def _enforce_rate_limit(sender_email: str) -> None:
    try:
        limit = int(RATE_LIMIT_PER_DAY)
    except ValueError:
        limit = 0
    if limit <= 0:
        return
    if not FIRESTORE_ENABLED:
        return
    key = _rate_limit_key(sender_email)
    count = increment_rate_limit(key, limit)
    if count > limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded: {count}/{limit} per day",
        )


def _is_duplicate_post(invoice: InvoiceData) -> bool:
    if not FIRESTORE_ENABLED:
        return False
    try:
        invoice_date = (
            invoice.invoice_date.isoformat()
            if hasattr(invoice.invoice_date, "isoformat")
            else str(invoice.invoice_date)
        )
        is_dup = has_recent_posted_match(
            invoice.supplier_reference,
            invoice_date,
            invoice.is_credit,
        )
        if is_dup:
            logger.info(
                "Duplicate local match",
                extra={
                    "supplier_reference": invoice.supplier_reference,
                    "invoice_date": invoice_date,
                    "is_credit": invoice.is_credit,
                },
            )
        return is_dup
    except Exception as exc:
        logger.info("Duplicate check failed, continuing: %s", exc)
        return False


def _duplicate_payload(invoice: InvoiceData, reason: str) -> Dict[str, Any]:
    invoice_date = (
        invoice.invoice_date.isoformat()
        if hasattr(invoice.invoice_date, "isoformat")
        else str(invoice.invoice_date)
    )
    return {
        "reason": reason,
        "supplier_reference": invoice.supplier_reference,
        "invoice_date": invoice_date,
        "is_credit": invoice.is_credit,
    }


def _sage_duplicate_exists(invoice: InvoiceData) -> Optional[bool]:
    if not SAGE_ENABLED:
        return None
    try:
        return sage_invoice_exists(invoice)
    except Exception as exc:
        logger.info("Sage duplicate lookup failed; keeping local decision: %s", exc)
        return None


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
        contact_id=payload.get("contact_id"),
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


def _is_statement_filename(name: str) -> bool:
    return "statement" in (name or "").lower()


def _is_invoice_filename(name: str) -> bool:
    lowered = (name or "").lower()
    return any(
        kw in lowered
        for kw in (
            "invoice",
            "tax invoice",
            "credit memo",
            "credit note",
        )
    )


def _select_pdf_attachment(candidates: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    non_statement = [att for att in candidates if not _is_statement_filename(att.get("Name") or "")]
    invoice_like = [att for att in non_statement if _is_invoice_filename(att.get("Name") or "")]
    if invoice_like:
        return invoice_like[0]
    if non_statement:
        return non_statement[0]
    return None


def _find_first_pdf_attachment(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    attachments = payload.get("Attachments") or []
    if not isinstance(attachments, list):
        return None

    candidates: list[Dict[str, Any]] = []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        content_type = (attachment.get("ContentType") or "").lower()
        name = (attachment.get("Name") or "").lower()
        if "pdf" in content_type or name.endswith(".pdf"):
            if attachment.get("Content"):
                candidates.append(attachment)

    return _select_pdf_attachment(candidates)


def _find_image_attachments(payload: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Return all image attachments (JPEG, PNG, etc.) decoded from the Postmark payload."""
    attachments = payload.get("Attachments") or []
    if not isinstance(attachments, list):
        return []
    _IMAGE_TYPES = ("image/jpeg", "image/png", "image/gif", "image/webp", "image/tiff")
    _IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".tiff")
    results: list[Dict[str, Any]] = []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        content_type = (attachment.get("ContentType") or "").lower()
        name = (attachment.get("Name") or "").lower()
        if any(t in content_type for t in _IMAGE_TYPES) or any(name.endswith(ext) for ext in _IMAGE_EXTS):
            content = attachment.get("Content")
            if content:
                try:
                    img_bytes = base64.b64decode(content)
                    results.append({
                        "Name": attachment.get("Name") or "image.jpg",
                        "ContentType": content_type,
                        "ContentBytes": img_bytes,
                    })
                except Exception:
                    pass
    return results


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
            candidates: list[Dict[str, Any]] = []
            for part in msg.walk():
                content_type = (part.get_content_type() or "").lower()
                filename = (part.get_filename() or "").lower()
                if "pdf" in content_type or filename.endswith(".pdf"):
                    payload_bytes = part.get_payload(decode=True)
                    if payload_bytes:
                        candidates.append(
                            {
                                "Name": part.get_filename() or "attachment.pdf",
                                "ContentType": content_type,
                                "ContentBytes": payload_bytes,
                            }
                        )
            return _select_pdf_attachment(candidates)
        # If first parse failed to find a PDF and the raw data looks base64-ish, try decoding.
        if attempt == 0 and isinstance(raw, str):
            try:
                raw_bytes = base64.b64decode(raw, validate=False)
            except Exception:
                break

    return None


def _extract_images_from_raw_email(payload: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Extract image attachments from a raw MIME email in the Postmark payload."""
    raw = payload.get("RawEmail") or payload.get("RawMessage") or payload.get("RawSource")
    if not raw:
        return []
    raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8", errors="ignore")
    _IMAGE_TYPES = ("image/jpeg", "image/png", "image/gif", "image/webp", "image/tiff")
    _IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".tiff")
    results: list[Dict[str, Any]] = []
    for attempt in range(2):
        try:
            msg = BytesParser(policy=policy.default).parsebytes(raw_bytes)
        except Exception:
            msg = None
        if msg:
            for part in msg.walk():
                content_type = (part.get_content_type() or "").lower()
                filename = part.get_filename() or ""
                name_lower = filename.lower()
                if any(t in content_type for t in _IMAGE_TYPES) or any(name_lower.endswith(ext) for ext in _IMAGE_EXTS):
                    part_bytes = part.get_payload(decode=True)
                    if part_bytes:
                        results.append({
                            "Name": filename or "image.jpg",
                            "ContentType": content_type,
                            "ContentBytes": part_bytes,
                        })
            if results:
                return results
        if attempt == 0 and isinstance(raw, str):
            try:
                raw_bytes = base64.b64decode(raw, validate=False)
            except Exception:
                break
    return results


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
    return (
        "clf distribution" in normalized
        or "clf distribution ltd" in normalized
        or "gb712190568" in normalized
    )


def _text_looks_like_viridian(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "viridian international" in normalized
        or "viridian nutrition" in normalized
        or "viridian-nutrition.com" in normalized
        or "viridian" in normalized
        or "gb738632315" in normalized
    )


def _text_looks_like_hunts(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "hunt’s food group" in normalized
        or "hunt's food group" in normalized
        or "hunts food group" in normalized
        or "huntsfoodgroup" in normalized
        or "hub@huntsfoodgroup.co.uk" in normalized
        or "vat no: 813 0548 57" in normalized
    )


def _text_looks_like_essential(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "essential-trading.coop" in normalized
        or "www.essential-trading.coop" in normalized
        or "sales@essential-trading.coop" in normalized
        or "essential trading cooperative" in normalized
        or "essential trading co-operative" in normalized
        or "essential trading co operative" in normalized
        or "essential trading cooperative ltd" in normalized
        or "essential trading co-operative ltd" in normalized
        or "gb303067304" in normalized
    )


def _text_looks_like_avogel(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "a.vogel ltd" in normalized
        or "a vogel ltd" in normalized
        or "a.vogel" in normalized
        or "avogel.co.uk" in normalized
        or "vat no. 454 9330 37" in normalized
        or "vat no: 454 9330 37" in normalized
        or "sales i n voice" in normalized
    )


def _text_looks_like_pestokill(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "pestokill" in normalized
        or "horizon environment services" in normalized
        or "437 6371 34" in normalized
    )


def _text_looks_like_lewtress(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "lewtress natural health" in normalized
        or "827199789" in normalized
    )


def _text_looks_like_biocare(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "biocare limited" in normalized
        or "biocare.co.uk" in normalized
        or "249786641" in normalized
    )


def _text_looks_like_kinetic(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "kinetic enterprises" in normalized
        or "6058949" in normalized
    )


def _text_looks_like_absolute_aromas(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "absolute aromas" in normalized
        or "absolute-aromas.com" in normalized
        or "278 1735 71" in normalized
    )


def _text_looks_like_emporio(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "emporio uk ltd" in normalized
        or "emporiouk.com" in normalized
        or "900 2642 72" in normalized
    )


def _text_looks_like_watson_pratt(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "tax invoice" in normalized
        and "invoice number" in normalized
        and "amount gbp" in normalized
        and bool(re.search(r"vat number\s+125201466", normalized))
    )


def _filename_looks_like_watson_pratt(name: str) -> bool:
    return bool(re.search(r"\bIN-\d+\b", name or "", re.IGNORECASE))


def _text_looks_like_nestle(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "nestle uk ltd" in normalized
        or "nestle" in normalized
        or "nestlé" in normalized
        or "nestle uk" in normalized
        or "vat reg no" in normalized and "nestle" in normalized
    )


def _text_looks_like_natures_plus(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "naturesplus" in normalized
        or "natures plus" in normalized
        or "nature's plus" in normalized
        or "vat reg no: gb718284519" in normalized
    )


def _text_looks_like_bionature(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "bio-nature limited" in normalized
        or "bionature.uk.com" in normalized
        or "bio nature" in normalized
        or "bionature" in normalized
        or "vat reg no: 847 3436 08" in normalized
        or "vat reg no: 847 3436 08".replace(" ", "") in normalized.replace(" ", "")
    )


def _text_looks_like_natures_aid(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "natures aid ltd" in normalized
        or "naturesaid.co.uk" in normalized
        or "nature's aid" in normalized
        or "natures aid" in normalized
        or "vat reg no: gb 604 7052 68" in normalized
        or "vat reg no: gb604705268" in normalized.replace(" ", "")
    )


def _text_looks_like_tonyrefail(text: str) -> bool:
    normalized = (text or "").lower()
    return (
        "tonyrefail apiary" in normalized
        or "tonyrefailapiary@googlemail.com" in normalized
        or "tonyrefail" in normalized
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

    message_id = payload.get("MessageID") or payload.get("MessageId")
    if FIRESTORE_ENABLED and message_id:
        try:
            reserved = reserve_message_id(
                str(message_id),
                {"status": "received", "source": "postmark"},
            )
            if not reserved:
                logger.info(
                    "Duplicate message_id detected",
                    extra={"message_id": message_id, "source": "postmark"},
                )
                return {
                    "status": "ok",
                    "max_request_bytes": _max_request_bytes(),
                    "message": "Duplicate message_id",
                    "message_id": message_id,
                }
        except Exception:
            logger.exception("Failed to reserve message_id; continuing")

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


@app.get("/sage/credit-notes/search")
async def sage_credit_notes_search(request: Request, q: str) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not SAGE_ENABLED:
        return {"status": "disabled"}
    if not q or not q.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing query")
    try:
        return {"status": "ok", **search_purchase_credit_notes(q.strip())}
    except Exception as exc:
        logger.exception("Sage credit note search failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.get("/sage/purchase-invoices/count")
async def sage_purchase_invoices_count(
    request: Request, from_date: str, to_date: str, include_credits: bool = False
) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not SAGE_ENABLED:
        return {"status": "disabled"}
    try:
        date.fromisoformat(from_date)
        date.fromisoformat(to_date)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="from_date and to_date must be YYYY-MM-DD",
        ) from exc
    try:
        result = count_purchase_invoices(from_date, to_date, include_credits=include_credits)
        return {
            "status": "ok",
            "from_date": from_date,
            "to_date": to_date,
            "include_credits": include_credits,
            **result,
        }
    except Exception as exc:
        logger.exception("Sage invoice count failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.get("/debug/duplicate-reason")
async def debug_duplicate_reason(
    request: Request,
    ref: str,
    invoice_date: Optional[str] = None,
    is_credit: Optional[bool] = None,
) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not FIRESTORE_ENABLED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Firestore not enabled")
    if not ref or not ref.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing ref")

    records = find_records_by_reference(ref.strip())
    trimmed: list[Dict[str, Any]] = []
    for item in records:
        data = item.get("data") or {}
        trimmed.append(
            {
                "id": item.get("id"),
                "status": data.get("status"),
                "duplicate": data.get("duplicate"),
                "sage": data.get("sage"),
                "error": data.get("error"),
                "created_at": data.get("created_at"),
                "message_id": (data.get("payload_meta") or {}).get("message_id"),
                "invoice_date": (data.get("parsed") or {}).get("invoice_date"),
            }
        )

    lock_info = None
    if invoice_date:
        lock_info = get_reference_lock(ref.strip(), invoice_date.strip(), is_credit)

    return {
        "status": "ok",
        "reference": ref.strip(),
        "record_count": len(trimmed),
        "records": trimmed,
        "reference_lock": lock_info,
    }


@app.get("/debug/latest")
async def debug_latest_record(request: Request, parsed_only: bool = True) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not FIRESTORE_ENABLED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Firestore not enabled")

    result = get_latest_parsed_record() if parsed_only else get_latest_record()
    if not result:
        return {"status": "ok", "record": None}

    record_id, data = result
    return {
        "status": "ok",
        "record": {
            "id": record_id,
            "status": data.get("status"),
            "error": data.get("error"),
            "created_at": data.get("created_at"),
            "message_id": (data.get("payload_meta") or {}).get("message_id"),
            "payload_meta": data.get("payload_meta"),
            "attachment": data.get("attachment"),
            "parsed": data.get("parsed"),
            "duplicate": data.get("duplicate"),
            "sage": data.get("sage"),
        },
    }


@app.post("/sage/post")
async def sage_post(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON payload must be an object")

    message_id = payload.get("MessageID") or payload.get("MessageId")
    if FIRESTORE_ENABLED and message_id:
        try:
            reserved = reserve_message_id(
                str(message_id),
                {"status": "received", "source": "postmark"},
            )
            if not reserved:
                logger.info(
                    "Duplicate message_id detected",
                    extra={"message_id": message_id, "source": "postmark"},
                )
                return {
                    "status": "ok",
                    "max_request_bytes": _max_request_bytes(),
                    "message": "Duplicate message_id",
                    "message_id": message_id,
                }
        except Exception:
            logger.exception("Failed to reserve message_id; continuing")

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
            sage_exists = _sage_duplicate_exists(invoice)
            if sage_exists is True:
                duplicate = _duplicate_payload(invoice, "duplicate_sage")
                skip = {"status": "skipped", "reason": "duplicate_sage", "number": invoice.supplier_reference}
                if record_id:
                    update_record(record_id, {"status": "skipped", "sage": skip, "duplicate": duplicate})
                return {"status": "ok", "sage": skip, "record_id": record_id}
            if sage_exists is None:
                duplicate = _duplicate_payload(invoice, "duplicate_local")
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
            sage_exists = _sage_duplicate_exists(invoice)
            if sage_exists is True:
                duplicate = _duplicate_payload(invoice, "duplicate_sage")
                skip = {"status": "skipped", "reason": "duplicate_sage", "number": invoice.supplier_reference}
                update_record(record_id, {"status": "skipped", "sage": skip, "duplicate": duplicate})
                return {"status": "ok", "sage": skip, "record_id": record_id}
            if sage_exists is None:
                duplicate = _duplicate_payload(invoice, "duplicate_local")
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


@app.post("/sage/post-by-reference")
async def sage_post_by_reference(
    request: Request,
    supplier_reference: str,
    invoice_date: Optional[str] = None,
    force: bool = False,
    record_id: Optional[str] = None,
    contact_id_override: Optional[str] = None,
) -> Dict[str, Any]:
    _check_basic_auth(request)

    if not FIRESTORE_ENABLED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Firestore not enabled")
    if not supplier_reference or not supplier_reference.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing supplier_reference")

    records = find_records_by_reference(supplier_reference.strip())
    if not records:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No records found for reference")

    selected_record_id = None
    record = None
    # If caller specified an exact record_id, use that directly
    if record_id:
        for item in records:
            if item.get("id") == record_id:
                selected_record_id = record_id
                record = item.get("data")
                break
        if not selected_record_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"record_id {record_id} not found")
    else:
        invoice_date_text = invoice_date.strip() if invoice_date else None
        if invoice_date_text:
            for item in records:
                parsed = (item.get("data") or {}).get("parsed") or {}
                if str(parsed.get("invoice_date")) == invoice_date_text:
                    selected_record_id = item.get("id")
                    record = item.get("data")
                    break
        if not record:
            selected_record_id = records[0].get("id")
            record = records[0].get("data") if records else None
    record_id = selected_record_id

    if not record_id or not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No matching record found")

    parsed = record.get("parsed")
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Record missing parsed data")

    invoice = _invoice_from_payload(parsed)
    if contact_id_override:
        invoice = invoice.model_copy(update={"contact_id": contact_id_override}) if hasattr(invoice, "model_copy") else invoice.__class__(**{**_invoice_to_dict(invoice), "contact_id": contact_id_override})
    logger.info("post-by-reference: contact_id=%s supplier=%s ref=%s", invoice.contact_id, invoice.supplier, invoice.supplier_reference)

    if not SAGE_ENABLED:
        logger.info("Sage disabled; skipping /sage/post-by-reference")
        update_record(record_id, {"status": "queued", "reason": "sage_disabled"})
        return {"status": "disabled", "record_id": record_id}

    try:
        if not force and FIRESTORE_ENABLED:
            ref_date = (
                invoice.invoice_date.isoformat()
                if hasattr(invoice.invoice_date, "isoformat")
                else str(invoice.invoice_date)
            )
            reserved = reserve_reference(invoice.supplier_reference, ref_date, invoice.is_credit)
            if not reserved:
                logger.info(
                    "Duplicate reference lock detected",
                    extra={
                        "supplier_reference": invoice.supplier_reference,
                        "invoice_date": ref_date,
                        "is_credit": invoice.is_credit,
                    },
                )
                sage_exists = _sage_duplicate_exists(invoice)
                if sage_exists is True:
                    duplicate = _duplicate_payload(invoice, "duplicate_sage")
                    skip = {"status": "skipped", "reason": "duplicate_sage", "number": invoice.supplier_reference}
                    update_record(record_id, {"status": "skipped", "sage": skip, "duplicate": duplicate})
                    return {"status": "ok", "sage": skip, "record_id": record_id}
                if sage_exists is None:
                    duplicate = _duplicate_payload(invoice, "reference_locked")
                    skip = {
                        "status": "skipped",
                        "reason": "reference_locked",
                        "number": invoice.supplier_reference,
                    }
                    update_record(record_id, {"status": "skipped", "sage": skip, "duplicate": duplicate})
                    return {"status": "ok", "sage": skip, "record_id": record_id}
        if not force and _is_duplicate_post(invoice):
            duplicate = _duplicate_payload(invoice, "duplicate_local")
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


@app.get("/sage/search-invoices")
async def sage_search_invoices(request: Request, reference: str) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not reference or not reference.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing reference")
    sage_id = find_sage_invoice_id(reference.strip())
    return {"status": "ok", "reference": reference.strip(), "sage_id": sage_id, "found": sage_id is not None}


@app.get("/sage/lookup-invoice")
async def sage_lookup_invoice(request: Request, sage_id: str) -> Dict[str, Any]:
    """Direct GET /purchase_invoices/{id} — verify an invoice exists in Sage."""
    from app.sage_client import _refresh_access_token, _sage_headers, SAGE_API_BASE, _get_env
    import requests as _req
    _check_basic_auth(request)
    access_token = _refresh_access_token()
    business_id = _get_env("SAGE_BUSINESS_ID")
    resp = _req.get(f"{SAGE_API_BASE}/purchase_invoices/{sage_id.strip()}", headers=_sage_headers(access_token, business_id), timeout=30)
    if resp.status_code == 404:
        return {"status": "not_found", "sage_id": sage_id}
    if resp.status_code >= 400:
        return {"status": "error", "http_status": resp.status_code, "body": resp.text[:500]}
    d = resp.json()
    return {"status": "ok", "sage_id": sage_id, "reference": d.get("reference"), "vendor_reference": d.get("vendor_reference"), "contact": (d.get("contact") or {}).get("displayed_as"), "total": d.get("total_amount"), "date": d.get("date"), "invoice_status": (d.get("status") or {}).get("id")}


@app.get("/sage/debug-businesses")
async def sage_debug_businesses(request: Request) -> Dict[str, Any]:
    """Check what Sage businesses the token has access to and what SAGE_BUSINESS_ID is set to."""
    from app.sage_client import _refresh_access_token, _sage_headers, SAGE_API_BASE, _get_env
    import requests as _req
    _check_basic_auth(request)
    access_token = _refresh_access_token()
    business_id = _get_env("SAGE_BUSINESS_ID")
    resp = _req.get(f"{SAGE_API_BASE}/businesses", headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, timeout=30)
    businesses = []
    if resp.status_code < 400:
        for b in (resp.json().get("$items") or []):
            businesses.append({"id": b.get("id"), "name": b.get("name"), "displayed_as": b.get("displayed_as")})
    return {"status": "ok", "configured_business_id": business_id, "businesses": businesses, "businesses_http_status": resp.status_code}


@app.post("/sage/test-invoice-roundtrip")
async def sage_test_invoice_roundtrip(request: Request) -> Dict[str, Any]:
    """Create a minimal test purchase invoice in Sage, then immediately GET it back.

    This diagnoses the mystery of invoices that return 200 on POST but 404 on GET.
    The test invoice will be voided automatically after the check.
    """
    from app.sage_client import _refresh_access_token, _sage_headers, SAGE_API_BASE, _get_env
    import requests as _req
    import time as _time

    _check_basic_auth(request)
    if not SAGE_ENABLED:
        return {"status": "disabled"}

    access_token = _refresh_access_token()
    business_id = _get_env("SAGE_BUSINESS_ID")
    contact_id = _get_env("SAGE_CONTACT_ID")
    ledger_id = _get_env("SAGE_LEDGER_5001_ID")

    test_ref = f"TEST-RT-{int(_time.time())}"
    payload = {
        "purchase_invoice": {
            "contact_id": contact_id,
            "date": "2026-02-28",
            "due_date": "2026-03-30",
            "reference": test_ref,
            "vendor_reference": test_ref,
            "invoice_lines": [
                {
                    "description": "Roundtrip test — delete me",
                    "ledger_account_id": ledger_id,
                    "quantity": 1,
                    "unit_price": 1.00,
                    "net_amount": 1.00,
                    "tax_rate_id": _get_env("SAGE_TAX_ZERO_ID") or "GB_ZERO",
                    "tax_amount": 0,
                    "total_amount": 1.00,
                }
            ],
        }
    }

    post_resp = _req.post(
        f"{SAGE_API_BASE}/purchase_invoices",
        headers=_sage_headers(access_token, business_id),
        json=payload,
        timeout=30,
    )
    post_status = post_resp.status_code
    try:
        post_body = post_resp.json()
    except Exception:
        post_body = {"raw": post_resp.text[:1000]}

    created_id = post_body.get("id") if isinstance(post_body, dict) else None

    get_status = None
    get_body = None
    if created_id:
        get_resp = _req.get(
            f"{SAGE_API_BASE}/purchase_invoices/{created_id}",
            headers=_sage_headers(access_token, business_id),
            timeout=30,
        )
        get_status = get_resp.status_code
        try:
            get_body = get_resp.json()
        except Exception:
            get_body = {"raw": get_resp.text[:500]}

        # Void the test invoice
        _req.delete(
            f"{SAGE_API_BASE}/purchase_invoices/{created_id}",
            headers=_sage_headers(access_token, business_id),
            timeout=30,
        )

    return {
        "status": "ok",
        "test_ref": test_ref,
        "business_id": business_id,
        "contact_id": contact_id,
        "post_http_status": post_status,
        "post_response_id": created_id,
        "post_response_reference": post_body.get("reference") if isinstance(post_body, dict) else None,
        "post_response_contact": (post_body.get("contact") or {}).get("displayed_as") if isinstance(post_body, dict) else None,
        "post_response_status": (post_body.get("status") or {}).get("id") if isinstance(post_body, dict) else None,
        "post_response_keys": list(post_body.keys()) if isinstance(post_body, dict) else None,
        "get_http_status": get_status,
        "get_response_id": get_body.get("id") if isinstance(get_body, dict) else None,
        "roundtrip_ok": get_status == 200 if get_status is not None else None,
    }


@app.get("/sage/debug-search")
async def sage_debug_search(request: Request, ref: str) -> Dict[str, Any]:
    """Raw Sage API search — returns first strategy's $items to diagnose field names."""
    from app.sage_client import _refresh_access_token, _sage_headers, SAGE_API_BASE, _get_env
    import requests as _req
    _check_basic_auth(request)
    access_token = _refresh_access_token()
    business_id = _get_env("SAGE_BUSINESS_ID")
    results = {}
    for label, params in [
        ("search", {"search": ref, "items_per_page": 10}),
        ("vendor_reference", {"vendor_reference": ref}),
        ("filter_vr", {"filter": f"vendor_reference eq '{ref}'"}),
        ("filter_ref", {"filter": f"reference eq '{ref}'"}),
    ]:
        resp = _req.get(f"{SAGE_API_BASE}/purchase_invoices", headers=_sage_headers(access_token, business_id), params=params, timeout=30)
        items = resp.json().get("$items", []) if resp.status_code < 400 else []
        results[label] = [{"id": i.get("id","")[:16], "ref": i.get("reference"), "vr": i.get("vendor_reference"), "disp": i.get("displayed_as")} for i in items[:5]]
    return {"status": "ok", "ref": ref, "results": results}


@app.delete("/sage/void-invoice")
async def sage_void_invoice(request: Request, sage_id: str) -> Dict[str, Any]:
    _check_basic_auth(request)
    if not sage_id or not sage_id.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing sage_id")
    result = void_purchase_invoice(sage_id.strip())
    return {"status": "ok", "result": result}


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


SAGE_CALLBACK_PATH = "/sage/callback"
POSTMAN_REDIRECT_URI = "https://oauth.pstmn.io/v1/browser-callback"


def _sage_callback_uri(request: Request) -> str:
    """Build the absolute callback URI from the incoming request's host."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host") or request.url.netloc
    return f"{scheme}://{host}{SAGE_CALLBACK_PATH}"


def _sage_auth_url(client_id: str, redirect_uri: str) -> str:
    params = urlencode(
        {
            "filter": "apiv3.1",
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "full_access",
            "state": "123",
        },
        quote_via=quote,
    )
    return f"https://www.sageone.com/oauth2/auth/central?{params}"


@app.get("/sage/auth-url")
async def sage_auth_url(request: Request, use_callback: bool = True) -> Dict[str, str]:
    _check_basic_auth(request)
    client_id = os.getenv("SAGE_CLIENT_ID")
    if not client_id:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing SAGE_CLIENT_ID")
    redirect_uri = _sage_callback_uri(request) if use_callback else POSTMAN_REDIRECT_URI
    return {"url": _sage_auth_url(client_id, redirect_uri), "redirect_uri": redirect_uri}


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

    redirect_uri = payload.get("redirect_uri", POSTMAN_REDIRECT_URI)

    try:
        tokens = exchange_auth_code(str(code).strip(), redirect_uri=redirect_uri)
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


@app.get("/sage/callback", response_class=HTMLResponse)
async def sage_callback(request: Request, code: str = "", error: str = "", state: str = ""):
    # No basic auth here -- browser redirect from Sage can't carry credentials.
    # The OAuth code is single-use and requires client_secret to exchange.

    if error:
        return HTMLResponse(
            content=f"<h1>Sage Auth Failed</h1><p>{error}</p>",
            status_code=400,
        )

    if not code:
        return HTMLResponse(
            content="<h1>Sage Auth Failed</h1><p>No authorization code received.</p>",
            status_code=400,
        )

    redirect_uri = _sage_callback_uri(request)
    try:
        tokens = exchange_auth_code(code.strip(), redirect_uri=redirect_uri)
    except Exception as exc:
        logger.exception("Sage callback exchange failed: %s", exc)
        return HTMLResponse(
            content=f"<h1>Sage Auth Failed</h1><p>Token exchange failed: {exc}</p>",
            status_code=502,
        )

    logger.info("Sage callback: auth complete, token stored")
    return HTMLResponse(
        content=(
            "<h1>Sage Auth Complete</h1>"
            "<p>Refresh token has been stored in Secret Manager.</p>"
            f"<p>Expires in: {tokens.get('expires_in')} seconds</p>"
        ),
        status_code=200,
    )


def _detect_and_parse(
    text: str,
    sender_email: str,
    attachment_name: str,
    raise_on_unknown: bool = True,
) -> Optional[list[InvoiceData]]:
    """Detect supplier from text/sender and return parsed invoices.

    Returns None (or raises HTTPException when raise_on_unknown=True) if no supplier matched.
    """
    is_clf_sender = sender_email.endswith("@clfdistribution.com")
    is_viridian_sender = sender_email.endswith("@viridian-nutrition.com")
    is_hunts_sender = sender_email.endswith("@huntsfoodgroup.co.uk")
    is_essential_sender = sender_email.endswith("@essential-trading.coop")

    if is_hunts_sender or _text_looks_like_hunts(text):
        if not is_hunts_sender:
            logger.info("Sender not Hunts domain but text looks like Hunts; using Hunts parser: %s", sender_email)
        return parse_hunts(text)
    elif is_essential_sender or _text_looks_like_essential(text):
        if not is_essential_sender:
            logger.info("Sender not Essential domain but text looks like Essential; using Essential parser: %s", sender_email)
        return [parse_essential(text)]
    elif is_clf_sender or _text_looks_like_clf(text):
        if not is_clf_sender:
            logger.info("Sender not CLF domain but text looks like CLF; using CLF parser: %s", sender_email)
        return [parse_clf(text)]
    elif is_viridian_sender or _text_looks_like_viridian(text):
        if not is_viridian_sender:
            logger.info("Sender not Viridian domain but text looks like Viridian; using Viridian parser: %s", sender_email)
        return [parse_viridian(text)]
    elif _text_looks_like_watson_pratt(text) or _filename_looks_like_watson_pratt(attachment_name):
        if not _text_looks_like_watson_pratt(text):
            logger.info("Text did not match Watson & Pratt heuristics; matched by filename: %s", attachment_name)
        return [parse_watson_pratt(text)]
    elif _text_looks_like_nestle(text):
        return [parse_nestle(text)]
    elif _text_looks_like_natures_plus(text):
        return [parse_natures_plus(text)]
    elif _text_looks_like_bionature(text):
        return [parse_bionature(text)]
    elif _text_looks_like_natures_aid(text):
        return [parse_natures_aid(text)]
    elif _text_looks_like_tonyrefail(text):
        return [parse_tonyrefail(text)]
    elif _text_looks_like_avogel(text):
        return [parse_avogel(text)]
    elif _text_looks_like_emporio(text):
        return [parse_emporio(text)]
    elif _text_looks_like_pestokill(text):
        return [parse_pestokill(text)]
    elif _text_looks_like_absolute_aromas(text):
        return [parse_absolute_aromas(text)]
    elif _text_looks_like_lewtress(text):
        return [parse_lewtress(text)]
    elif _text_looks_like_biocare(text):
        return [parse_biocare(text)]
    elif _text_looks_like_kinetic(text):
        return [parse_kinetic(text)]
    else:
        logger.warning("No supplier parser matched", extra={"sender": sender_email, "attachment": attachment_name})
        if raise_on_unknown:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported supplier")
        return None


@app.post("/postmark/inbound")
async def postmark_inbound(request: Request) -> Dict[str, Any]:
    _check_basic_auth(request)
    _enforce_request_size(request)
    if REQUIRE_SES_SOURCE:
        source = (request.headers.get("x-source") or "").strip().lower()
        if source != "ses":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid source")

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON payload must be an object")

    message_id = payload.get("MessageID") or payload.get("MessageId")
    if FIRESTORE_ENABLED and message_id:
        try:
            reserved = reserve_message_id(
                str(message_id),
                {"status": "received", "source": "postmark"},
            )
            if not reserved:
                logger.info(
                    "Duplicate message_id detected",
                    extra={"message_id": message_id, "source": "postmark"},
                )
                return {
                    "status": "ok",
                    "max_request_bytes": _max_request_bytes(),
                    "message": "Duplicate message_id",
                    "message_id": message_id,
                }
        except Exception:
            logger.exception("Failed to reserve message_id; continuing")

    pdf_attachment = _find_first_pdf_attachment(payload)
    raw_pdf_attachment = None if pdf_attachment else _extract_pdf_from_raw_email(payload)

    if not pdf_attachment and not raw_pdf_attachment:
        # Try image attachments via OCR before giving up.
        image_attachments = _find_image_attachments(payload)
        if not image_attachments:
            image_attachments = _extract_images_from_raw_email(payload)

        if not image_attachments:
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

        # We have image attachments — process each via OCR.
        ocr_sender_email = _extract_sender_email(payload) or ""
        _enforce_blocklist(payload.get("Subject") or "")
        _enforce_forwarder_whitelist(ocr_sender_email)
        _enforce_rate_limit(ocr_sender_email)
        logger.info("Processing %d image attachment(s) via OCR from %s", len(image_attachments), ocr_sender_email)

        img_record_ids: list[str] = []
        img_sage_results: list[dict[str, Any] | None] = []
        img_parsed_payloads: list[Any] = []

        for img_att in image_attachments:
            img_bytes = img_att["ContentBytes"]
            img_name = img_att.get("Name") or "image.jpg"
            logger.info("OCR: processing %s (%d bytes)", img_name, len(img_bytes))
            try:
                img_text = extract_text_from_image(img_bytes)
                _log_pdf_text(img_text)
            except Exception:
                logger.exception("OCR failed for image: %s", img_name)
                continue

            if not img_text.strip():
                logger.warning("OCR produced no text for image: %s", img_name)
                continue

            img_invoices = _detect_and_parse(img_text, ocr_sender_email, img_name, raise_on_unknown=False)
            if img_invoices is None:
                logger.warning("No supplier matched for OCR image: %s", img_name)
                continue

            for inv in img_invoices:
                img_parsed_dict = _invoice_to_dict(inv)
                img_parsed_payloads.append(img_parsed_dict)
                logger.info("Parsed image invoice: %s", img_parsed_dict)

                rec_id: Optional[str] = None
                if FIRESTORE_ENABLED:
                    try:
                        rec_id = enqueue_record(
                            {
                                "status": "parsed",
                                "source": "postmark_image_ocr",
                                "payload_meta": _payload_meta(payload),
                                "attachment": {
                                    "name": img_name,
                                    "content_type": img_att.get("ContentType"),
                                    "size_bytes": len(img_bytes),
                                },
                                "parsed": _serialize_for_storage(img_parsed_dict),
                            }
                        )
                        img_record_ids.append(rec_id)
                        logger.info("Enqueued Firestore record for image invoice: %s", rec_id)
                    except Exception:
                        logger.exception("Failed to write Firestore record for image invoice")

                if SAGE_ENABLED:
                    try:
                        img_sage_result: dict[str, Any] | None = None
                        skip_post = False
                        if FIRESTORE_ENABLED:
                            ref_date = (
                                inv.invoice_date.isoformat()
                                if hasattr(inv.invoice_date, "isoformat")
                                else str(inv.invoice_date)
                            )
                            reserved = reserve_reference(inv.supplier_reference, ref_date, inv.is_credit)
                            if not reserved:
                                sage_exists = _sage_duplicate_exists(inv)
                                dup_reason = "duplicate_sage" if sage_exists is True else "reference_locked"
                                img_sage_result = {
                                    "status": "skipped",
                                    "reason": dup_reason,
                                    "number": inv.supplier_reference,
                                }
                                skip_post = True

                        if not skip_post and _is_duplicate_post(inv):
                            sage_exists = _sage_duplicate_exists(inv)
                            dup_reason = "duplicate_sage" if sage_exists is True else "duplicate_local"
                            img_sage_result = {
                                "status": "skipped",
                                "reason": dup_reason,
                                "number": inv.supplier_reference,
                            }
                            skip_post = True

                        if not skip_post:
                            if inv.is_credit:
                                img_sage_result = post_purchase_credit_note(inv)
                            else:
                                img_sage_result = post_purchase_invoice(inv)

                        # Determine Sage ID — either from fresh post or by looking up existing invoice
                        sage_id_for_attach: Optional[str] = None
                        if isinstance(img_sage_result, dict) and img_sage_result.get("id"):
                            sage_id_for_attach = img_sage_result["id"]
                            logger.info("Sage created id: %s", sage_id_for_attach)
                        elif skip_post and FIRESTORE_ENABLED:
                            # Invoice already exists — find Sage ID from stored Firestore records.
                            # Prefer records whose stored Sage contact matches the current invoice
                            # so we skip stale records from a different supplier (e.g. CLF phantom IDs).
                            try:
                                existing_recs = find_records_by_reference(inv.supplier_reference)
                                for existing_rec in existing_recs:
                                    existing_sage = (existing_rec.get("data") or {}).get("sage") or {}
                                    existing_sage_id = existing_sage.get("id")
                                    if not existing_sage_id:
                                        continue
                                    if inv.contact_id:
                                        existing_contact_id = (existing_sage.get("contact") or {}).get("id")
                                        if existing_contact_id and existing_contact_id != inv.contact_id:
                                            logger.info(
                                                "Skipping Firestore sage_id %s for attachment (contact mismatch: %s != %s)",
                                                existing_sage_id, existing_contact_id, inv.contact_id,
                                            )
                                            continue
                                    sage_id_for_attach = existing_sage_id
                                    logger.info("Found existing Sage id for attachment from Firestore: %s", sage_id_for_attach)
                                    break
                            except Exception as exc:
                                logger.warning("Could not find Sage ID from Firestore for %s: %s", inv.supplier_reference, exc)

                        if sage_id_for_attach:
                            try:
                                import img2pdf  # type: ignore[import]
                                pdf_bytes_for_attach = img2pdf.convert(img_bytes)
                                attach_name = re.sub(r"\.[^.]+$", ".pdf", img_name) if "." in img_name else f"{img_name}.pdf"
                                attachment_result = attach_pdf_to_sage(
                                    "purchase_credit_note" if inv.is_credit else "purchase_invoice",
                                    sage_id_for_attach,
                                    attach_name,
                                    pdf_bytes_for_attach,
                                )
                                attach_status = {"status": "ok", "id": attachment_result.get("id")}
                                logger.info("Attached image (as PDF) to Sage id: %s", sage_id_for_attach)
                            except Exception as exc:
                                logger.exception("Failed to attach image to Sage: %s", exc)
                                attach_status = {"status": "error", "message": str(exc)}
                            if img_sage_result is None:
                                img_sage_result = {}
                            img_sage_result["attachment"] = attach_status

                        if rec_id:
                            if isinstance(img_sage_result, dict) and img_sage_result.get("id"):
                                update_record(rec_id, {"status": "posted", "sage": img_sage_result})
                            elif isinstance(img_sage_result, dict) and img_sage_result.get("status") == "skipped":
                                update_record(rec_id, {"status": "skipped", "sage": img_sage_result})
                            else:
                                update_record(rec_id, {"status": "unknown", "sage": img_sage_result})
                        img_sage_results.append(img_sage_result)
                    except Exception as exc:
                        logger.exception("Sage post failed for image invoice: %s", exc)
                        img_sage_result = {"status": "error", "message": str(exc)}
                        if rec_id:
                            update_record(rec_id, {"status": "error", "error": str(exc)})
                        img_sage_results.append(img_sage_result)

        if FIRESTORE_ENABLED and message_id:
            try:
                msg_status = "parsed"
                if SAGE_ENABLED and img_sage_results:
                    if any(isinstance(r, dict) and r.get("status") == "error" for r in img_sage_results):
                        msg_status = "error"
                    elif any(isinstance(r, dict) and r.get("id") for r in img_sage_results):
                        msg_status = "posted"
                update_message_status(str(message_id), {"status": msg_status, "record_id": img_record_ids})
            except Exception:
                logger.exception("Failed to update message_id status for image invoices")

        return {
            "status": "ok",
            "max_request_bytes": _max_request_bytes(),
            "parsed": img_parsed_payloads if len(img_parsed_payloads) > 1 else (img_parsed_payloads[0] if img_parsed_payloads else None),
            "sage": img_sage_results if len(img_sage_results) > 1 else (img_sage_results[0] if img_sage_results else None),
            "record_id": img_record_ids if len(img_record_ids) > 1 else (img_record_ids[0] if img_record_ids else None),
            "source": "image_ocr",
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
    _enforce_blocklist(payload.get("Subject") or "")
    _enforce_forwarder_whitelist(sender_email)
    _enforce_rate_limit(sender_email)
    logger.info(
        "Postmark sender fields: From=%s FromFull.Email=%s OriginalSender=%s",
        payload.get("From"),
        (payload.get("FromFull") or {}).get("Email"),
        payload.get("OriginalSender"),
    )
    invoices = _detect_and_parse(text, sender_email, pdf_attachment.get("Name") or "" if pdf_attachment else "")

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
                if FIRESTORE_ENABLED:
                    ref_date = (
                        inv.invoice_date.isoformat()
                        if hasattr(inv.invoice_date, "isoformat")
                        else str(inv.invoice_date)
                    )
                    reserved = reserve_reference(inv.supplier_reference, ref_date, inv.is_credit)
                    if not reserved:
                        logger.info(
                            "Duplicate reference lock detected",
                            extra={
                                "supplier_reference": inv.supplier_reference,
                                "invoice_date": ref_date,
                                "is_credit": inv.is_credit,
                            },
                        )
                        sage_exists = _sage_duplicate_exists(inv)
                        if sage_exists is True:
                            duplicate = _duplicate_payload(inv, "duplicate_sage")
                            sage_result = {
                                "status": "skipped",
                                "reason": "duplicate_sage",
                                "number": inv.supplier_reference,
                            }
                            if record_id:
                                update_record(
                                    record_id,
                                    {"status": "skipped", "sage": sage_result, "duplicate": duplicate},
                                )
                            sage_results.append(sage_result)
                            continue
                        if sage_exists is None:
                            duplicate = _duplicate_payload(inv, "reference_locked")
                            sage_result = {
                                "status": "skipped",
                                "reason": "reference_locked",
                                "number": inv.supplier_reference,
                            }
                            if record_id:
                                update_record(
                                    record_id,
                                    {"status": "skipped", "sage": sage_result, "duplicate": duplicate},
                                )
                            sage_results.append(sage_result)
                            continue
                if _is_duplicate_post(inv):
                    sage_exists = _sage_duplicate_exists(inv)
                    if sage_exists is True:
                        duplicate = _duplicate_payload(inv, "duplicate_sage")
                        sage_result = {
                            "status": "skipped",
                            "reason": "duplicate_sage",
                            "number": inv.supplier_reference,
                        }
                        if record_id:
                            update_record(
                                record_id,
                                {"status": "skipped", "sage": sage_result, "duplicate": duplicate},
                            )
                        sage_results.append(sage_result)
                        continue
                    if sage_exists is None:
                        duplicate = _duplicate_payload(inv, "duplicate_local")
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

    if FIRESTORE_ENABLED and message_id:
        try:
            msg_status = "parsed"
            if SAGE_ENABLED and sage_results:
                if any(isinstance(r, dict) and r.get("status") == "error" for r in sage_results):
                    msg_status = "error"
                elif any(isinstance(r, dict) and r.get("status") == "skipped" for r in sage_results) and not any(
                    isinstance(r, dict) and r.get("id") for r in sage_results
                ):
                    msg_status = "skipped"
                elif any(isinstance(r, dict) and r.get("id") for r in sage_results):
                    msg_status = "posted"
            update_message_status(str(message_id), {"status": msg_status, "record_id": record_ids})
        except Exception:
            logger.exception("Failed to update message_id status")

    return {
        "status": "ok",
        "max_request_bytes": _max_request_bytes(),
        "parsed": parsed_payloads if len(parsed_payloads) > 1 else parsed_payloads[0],
        "sage": sage_results if len(sage_results) > 1 else (sage_results[0] if sage_results else None),
        "record_id": record_ids if len(record_ids) > 1 else (record_ids[0] if record_ids else None),
    }
