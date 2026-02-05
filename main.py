import base64
import hashlib
import json
import logging
import os
import sys

from fastapi import FastAPI, HTTPException, Request

app = FastAPI()

# ---- Logging that shows up on Cloud Run ----
def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("invoice_ingest")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(handler)

    logger.propagate = False
    return logger


logger = _setup_logging()

# ---- Config ----
BASIC_USER = os.environ.get("BASIC_USER", "")
BASIC_PASS = os.environ.get("BASIC_PASS", "")
MAX_REQUEST_BYTES = int(os.environ.get("MAX_REQUEST_BYTES", "2000000"))  # default 2MB


@app.get("/")
def health():
    return {"ok": True, "service": "invoice-ingest"}


def _basic_auth_ok(req: Request) -> bool:
    auth = req.headers.get("authorization", "")
    if not (BASIC_USER and BASIC_PASS and auth.startswith("Basic ")):
        return False
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        return False
    return decoded == f"{BASIC_USER}:{BASIC_PASS}"


async def _read_body_with_limit(req: Request, limit: int) -> bytes:
    """
    Enforce a hard limit even if Content-Length is missing (chunked transfer).
    """
    cl = req.headers.get("content-length")
    if cl:
        try:
            n = int(cl)
        except ValueError:
            raise HTTPException(400, "Invalid Content-Length")
        if n > limit:
            logger.warning("Inbound: reject Content-Length=%s limit=%s", n, limit)
            raise HTTPException(413, "Request too large")

        body = await req.body()
        if len(body) > limit:
            logger.warning("Inbound: reject after read bytes=%s limit=%s", len(body), limit)
            raise HTTPException(413, "Request too large")
        return body

    buf = bytearray()
    async for chunk in req.stream():
        buf.extend(chunk)
        if len(buf) > limit:
            logger.warning("Inbound: reject streaming bytes=%s limit=%s", len(buf), limit)
            raise HTTPException(413, "Request too large")
    return bytes(buf)


@app.post("/postmark/inbound")
async def postmark_inbound(req: Request):
    if not _basic_auth_ok(req):
        raise HTTPException(401, "Unauthorized")

    # Size guard BEFORE JSON/base64 work
    body_bytes = await _read_body_with_limit(req, MAX_REQUEST_BYTES)

    try:
        payload = json.loads(body_bytes.decode("utf-8") if body_bytes else "{}")
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    attachments = payload.get("Attachments", []) or []
    logger.info("Inbound: attachments=%s max_request_bytes=%s", len(attachments), MAX_REQUEST_BYTES)

    pdfs = [
        a for a in attachments
        if a.get("ContentType") == "application/pdf"
        or a.get("Name", "").lower().endswith(".pdf")
    ]
    if not pdfs:
        return {
            "status": "no_pdf",
            "attachment_names": [a.get("Name") for a in attachments],
            "max_request_bytes": MAX_REQUEST_BYTES,
        }

    pdf = pdfs[0]
    pdf_name = pdf.get("Name") or "invoice.pdf"

    try:
        pdf_bytes = base64.b64decode(pdf["Content"])
    except Exception:
        logger.exception("Inbound: failed to decode PDF content")
        raise HTTPException(400, "Bad PDF attachment encoding")

    pdf_sha256 = hashlib.sha256(pdf_bytes).hexdigest()
    logger.info("Inbound: pdf=%s size=%s sha256=%s", pdf_name, len(pdf_bytes), pdf_sha256)

    return {
        "status": "ok",
        "pdf_name": pdf_name,
        "pdf_size": len(pdf_bytes),
        "pdf_sha256": pdf_sha256,
        "max_request_bytes": MAX_REQUEST_BYTES,
    }
