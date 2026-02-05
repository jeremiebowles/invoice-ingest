# app/models.py
from __future__ import annotations

from pydantic import BaseModel, Field
from datetime import date
from typing import Literal


LedgerBucket = Literal["shop_1", "shop_2", "shop_3", "unknown"]


class InvoiceData(BaseModel):
    supplier: str
    supplier_reference: str
    invoice_date: date
    due_date: date | None = None

    description: str = "Purchases"
    ledger_bucket: LedgerBucket = "unknown"

    vat_net: float = Field(ge=0)
    nonvat_net: float = Field(ge=0)
    vat_amount: float = Field(ge=0)
    total: float = Field(ge=0)

    warnings: list[str] = []
