from __future__ import annotations

from io import BytesIO

import pdfplumber


def extract_text_from_image(image_bytes: bytes) -> str:
    """Extract text from an image using Google Cloud Vision document_text_detection."""
    from google.cloud import vision  # type: ignore[import]

    client = vision.ImageAnnotatorClient()
    image = vision.Image(content=image_bytes)
    response = client.document_text_detection(image=image)
    if response.error.message:
        raise RuntimeError(f"Cloud Vision error: {response.error.message}")
    return response.full_text_annotation.text or ""


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    if not pdf_bytes:
        return ""

    text_parts: list[str] = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            page_text = page_text.replace("\u00a0", " ")
            if page_text:
                text_parts.append(page_text)

    return "\n".join(text_parts).strip()
