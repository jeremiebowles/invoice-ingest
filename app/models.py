from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class InvoiceData(BaseModel):
    supplier: str
    supplier_reference: str
    invoice_date: date
    due_date: Optional[date] = None
    description: str = "Purchases"
    is_credit: bool = False
    deliver_to_postcode: Optional[str] = None
    ledger_account: Optional[int] = None
    vat_net: float = Field(ge=0)
    nonvat_net: float = Field(ge=0)
    vat_amount: float = Field(ge=0)
    total: float = Field(ge=0)
    warnings: List[str] = Field(default_factory=list)
