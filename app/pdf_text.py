# app/pdf_text.py
from __future__ import annotations

from io import BytesIO
import pdfplumber


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """
    Robust-ish text extraction for invoice PDFs.
    Returns one big string.
    """
    chunks: list[str] = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            # Normalize a bit to make regex parsing less fragile
            text = text.replace("\u00a0", " ")  # nbsp
            chunks.append(text)

    return "\n\n".join(chunks).strip()
