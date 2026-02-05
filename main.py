# main.py
from __future__ import annotations

import base64
import binascii
import logging
import os
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from app.pdf_text import extract_text_from_pdf
from app.parsers.clf import parse_clf

logger = logging.getLogger("invoice_ingest")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI()


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        logger.warning("Invalid int env %s=%r; using default=%d", name, raw, default)
        return default


MAX_REQUEST_BYTES = _get_int_env("MAX_REQUEST_BYTES", 2_000_000)
MAX_PDF_BYTES = _get_int_env("MAX_PDF_BYTES", 2_500_000)  # harmless even if your PDFs are tiny

BASIC_USER = os.getenv("BASIC_USER")
BASIC_PASS = os.getenv("BASIC_PASS")


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=401,
        detail="Unauthorized",
        headers={"WWW-Authenticate": 'Basic realm="invoice-ingest"'},
    )


def _check_basic_auth(request: Request) -> None:
    if not BASIC_USER or not BASIC_PASS:
        # Security footgun prevention: if you forgot to set env vars, reject.
        raise HTTPException(status_code=500, detail="Auth not configured (BASIC_USER/BASIC_PASS missing)")

    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("basic "):
        raise _unauthorized()

    b64 = auth.split(" ", 1)[1].strip()
    try:
        decoded = base64.b64decode(b64).decode("utf-8", errors="strict")
    except Exception:
        raise _unauthorized()

    if ":" not in decoded:
        raise _unauthorized()

    user, pw = decoded.split(":", 1)
    if user != BASIC_USER or pw != BASIC_PASS:
        raise _unauthorized()


def _find_first_pdf_attachment(payload: dict[str, Any]) -> dict[str, Any] | None:
    attachments = payload.get("Attachments") or []
    if not isinstance(attachments, list):
        return None

    for att in attachments:
        if not isinstance(att, dict):
            continue
        name = (att.get("Name") or "").strip()
        ctype = (att.get("ContentType") or "").strip().lower()
        content = att.get("Content")

        looks_like_pdf = (
            "pdf" in ctype
            or name.lower().endswith(".pdf")
        )

        if looks_like_pdf and isinstance(content, str) and content.strip():
            return att

    return None


def _invoice_to_dict(invoice: Any) -> dict[str, Any]:
    # Pydantic v2 uses model_dump(), v1 uses dict()
    if hasattr(invoice, "model_dump"):
        return invoice.model_dump()
    if hasattr(invoice, "dict"):
        return invoice.dict()
    return dict(invoice)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/postmark/inbound")
async def postmark_inbound(request: Request) -> JSONResponse:
    # 1) Auth
    _check_basic_auth(request)

    # 2) Size guard BEFORE reading body
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            cl = int(content_length)
            if cl > MAX_REQUEST_BYTES:
                logger.info("Inbound: reject Content-Length=%d limit=%d", cl, MAX_REQUEST_BYTES)
                raise HTTPException(status_code=413, detail="Request too large")
        except ValueError:
            # If it's malformed, ignore and let JSON parsing fail naturally.
            pass

    # 3) Parse JSON
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON payload must be an object")

    attachments = payload.get("Attachments") or []
    att_count = len(attachments) if isinstance(attachments, list) else 0
    logger.info("Inbound: attachments=%d max_request_bytes=%d", att_count, MAX_REQUEST_BYTES)

    # 4) Find first PDF attachment
    att = _find_first_pdf_attachment(payload)
    if not att:
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "message": "No PDF attachment found",
                "attachments": att_count,
                "max_request_bytes": MAX_REQUEST_BYTES,
            },
        )

    pdf_name = (att.get("Name") or "attachment.pdf").strip()
    pdf_b64 = att.get("Content") or ""
    pdf_ctype = (att.get("ContentType") or "").strip()

    # 5) Decode base64 PDF
    try:
        pdf_bytes = base64.b64decode(pdf_b64, validate=True)
    except (binascii.Error, ValueError):
        raise HTTPException(status_code=400, detail="Attachment base64 decode failed")

    pdf_len = len(pdf_bytes)
    logger.info("Inbound: pdf=%r content_type=%r bytes=%d", pdf_name, pdf_ctype, pdf_len)

    if pdf_len > MAX_PDF_BYTES:
        # You said you're usually ~250KB, so this should never trip,
        # but it prevents a future “whoops that was a 40MB scan” incident.
        raise HTTPException(status_code=413, detail=f"PDF too large ({pdf_len} bytes)")

    # 6) Extract text
    try:
        text = extract_text_from_pdf(pdf_bytes)
    except Exception as e:
        logger.exception("PDF text extraction failed: %s", e)
        raise HTTPException(status_code=422, detail="PDF text extraction failed")

    # Temporary debugging aid while you tune parsing (remove later)
    logger.info("PDF text head: %r", text[:800])

    # 7) Parse invoice (CLF)
    try:
        invoice = parse_clf(text)
    except Exception as e:
        logger.exception("Invoice parsing failed: %s", e)
        raise HTTPException(status_code=422, detail=f"Invoice parsing failed: {e}")

    parsed = _invoice_to_dict(invoice)
    logger.info(
        "Parsed invoice: supplier_ref=%r invoice_date=%r postcode=%r ledger=%r total=%r warnings=%d",
        parsed.get("supplier_reference"),
        parsed.get("invoice_date"),
        parsed.get("deliver_to_postcode"),
        parsed.get("ledger_account"),
        parsed.get("total"),
        len(parsed.get("warnings") or []),
    )

    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "max_request_bytes": MAX_REQUEST_BYTES,
            "attachments": att_count,
            "pdf": {"name": pdf_name, "content_type": pdf_ctype, "bytes": pdf_len},
            "parsed": parsed,
        },
    )
