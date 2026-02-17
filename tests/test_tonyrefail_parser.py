"""Tests for the Tonyrefail Apiary invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.tonyrefail import parse_tonyrefail

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice IN06254 → CF11 9DX (ledger 5004), Beanfreaks Canton
# All zero-rated (honey), no VAT
# ---------------------------------------------------------------------------

class TestTonyrefailInvoice06254:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Tonyrefail Invoice IN06254.txt")
        self.result = parse_tonyrefail(text)

    def test_supplier(self):
        assert self.result.supplier == "Tonyrefail Apiary"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "IN06254"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 9, 19)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 10, 12)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_all_zero_rated(self):
        """Honey is zero-rated food, no VAT."""
        assert self.result.vat_net == 0.0
        assert self.result.vat_amount == 0.0

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(132.00, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(132.00, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []
