import base64
import hashlib
import logging
import os

from fastapi import FastAPI, HTTPException, Request

app = FastAPI()

# Basic auth credentials (set as Cloud Run environment variables)
BASIC_USER = os.environ.get("BASIC_USER", "")
BASIC_PASS = os.environ.get("BASIC_PASS", "")


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
    if not _basic_auth_ok(req):
        raise HTTPException(401, "Unauthorized")

    payload = await req.json()

    attachments = payload.get("Attachments", []) or []
    logging.info("Inbound: attachments=%s", len(attachments))

    # Log useful routing info (safe / non-sensitive)
    from_email = (payload.get("FromFull") or {}).get("Email") or payload.get("From")
    subject = payload.get("Subject")
    to_email = payload.get("To")
    logging.info("Inbound: from=%s to=%s subject=%s", from_email, to_email, subject)

    # Pick first PDF attachment
    pdfs = [
        a for a in attachments
        if a.get("ContentType") == "application/pdf"
        or a.get("Name", "").lower().endswith(".pdf")
    ]
    if not pdfs:
        names = [a.get("Name") for a in attachments]
        ctypes = [a.get("ContentType") for a in attachments]
        logging.info("Inbound: no PDF found. names=%s content_types=%s", names, ctypes)
        return {"status": "no_pdf", "attachment_names": names}

    pdf = pdfs[0]
    pdf_name = pdf.get("Name") or "invoice.pdf"

    # Decode PDF bytes from Postmark base64 attachment content
    try:
        pdf_bytes = base64.b64decode(pdf["Content"])
    except Exception as e:
        logging.exception("Inbound: failed to decode PDF content")
        raise HTTPException(400, f"Bad PDF attachment encoding: {e}")

    pdf_sha256 = hashlib.sha256(pdf_bytes).hexdigest()
    logging.info(
        "Inbound: pdf=%s size=%s sha256=%s",
        pdf_name,
        len(pdf_bytes),
        pdf_sha256,
    )

    # TODO (next step): call your extractor here and log the parsed invoice fields.
    # parsed = extract_invoice_from_pdf(pdf_bytes)
    # logging.info("Parsed: %s", parsed)

    # For now: prove the pipeline works end-to-end.
    return {
        "status": "ok",
        "pdf_name": pdf_name,
        "pdf_size": len(pdf_bytes),
        "pdf_sha256": pdf_sha256,
    }
