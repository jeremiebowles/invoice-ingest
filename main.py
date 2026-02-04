import base64
import os
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

@app.get("/")
def health():
    return {"ok": True, "service": "invoice-ingest"}

def _basic_auth_ok(req: Request) -> bool:
    user = os.environ.get("BASIC_USER", "")
    pw = os.environ.get("BASIC_PASS", "")
    auth = req.headers.get("authorization", "")
    if not (user and pw and auth.startswith("Basic ")):
        return False
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        return False
    return decoded == f"{user}:{pw}"

@app.post("/postmark/inbound")
async def postmark_inbound(req: Request):
    if not _basic_auth_ok(req):
        raise HTTPException(401, "Unauthorized")

    payload = await req.json()
    attachments = payload.get("Attachments", []) or []

    pdfs = [
        a for a in attachments
        if a.get("ContentType") == "application/pdf"
        or a.get("Name", "").lower().endswith(".pdf")
    ]
    if not pdfs:
        return {"status": "no_pdf"}

    pdf = pdfs[0]
    pdf_bytes = base64.b64decode(pdf["Content"])

    # For now: prove the pipeline works end-to-end.
    return {
        "status": "ok",
        "pdf_name": pdf.get("Name"),
        "pdf_size": len(pdf_bytes),
    }
