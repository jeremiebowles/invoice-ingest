import base64
import hashlib
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

# Reject huge requests before JSON parsing/base64 decode
# (Postmark payloads with a PDF attached are typically far smaller than this.)
MAX_REQUEST_BYTES = int(os.environ.get("MAX_REQUEST_BYTES", "2000000"))  # 2 MB default


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


@app.post("/postmark/inbound")
async def postmark_inbound(req: Request):
    # 1) Auth first (cheap)
    if not _basic_auth_ok(req):
        raise HTTPException(401, "Unauthorized")

    # 2) Size guard before reading/parsing the body (cheaper than json/base64)
    content_length = req.headers.get("content-length")
    if content_length:
        try:
            n = int(content_length)
        except ValueError:
            raise HTTPException(400, "Invalid Content-Length")
        if n > MAX_REQUEST_BYTES:
            logger.warning("Inbound: rejected oversized request bytes=%s limit=%s", n, MAX_REQUEST_BYTES)
            raise HTTPException(413, "Request too large")

    # 3) Now it's safe to parse JSON
    payload = await req.json()

    attachments = payload.get("Attachments", []) or []
    logger.info("Inbound: attachments=%s", len(attachments))

    from_email = (payload.get("FromFull") or {}).get("Email") or payload.get("From")
    subject = payload.get("Subject")
    to_email = payload.get("To")
    logger.info("Inbound: from=%s to=%s subject=%s", from_email, to_email, subject)

    # Pick first PDF attachment
    pdfs = [
        a for a in attachments
        if a.get("ContentType") == "application/pdf"
        or a.get("Name", "").lower().endswith(".pdf")
    ]
    if not pdfs:
        names = [a.get("Name") for a in attachments]
        ctypes = [a.get("ContentType") for a in attachments]
        logger.info("Inbound: no PDF found. names=%s content_types=%s", names, ctypes)
        return {"status": "no_pdf", "attachment_names": names}

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
