from __future__ import annotations

import base64
import logging
import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request, status

from app.parsers.clf import parse_clf
from app.pdf_text import extract_text_from_pdf


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("invoice-ingest")

app = FastAPI()


BASIC_USER = os.getenv("BASIC_USER")
BASIC_PASS = os.getenv("BASIC_PASS")
MAX_REQUEST_BYTES = os.getenv("MAX_REQUEST_BYTES")


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


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


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
    logger.info("Extracted text head: %s", text[:800])

    invoice = parse_clf(text)
    logger.info("Parsed invoice data: %s", _invoice_to_dict(invoice))

    return {
        "status": "ok",
        "max_request_bytes": _max_request_bytes(),
        "parsed": _invoice_to_dict(invoice),
    }
