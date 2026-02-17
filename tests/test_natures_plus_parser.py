"""Tests for the Nature's Plus invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.natures_plus import parse_natures_plus

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice 13103372 → CF24 3LP (ledger 5002), Ship To Roath
# Bill To is CF10 1AE (Royal Arcade) - must use Ship To postcode
# All items 20% VAT (supplements)
# ---------------------------------------------------------------------------

class TestNaturesPlusInvoice13103372:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Natures Plus Invoice 13103372.txt")
        self.result = parse_natures_plus(text)

    def test_supplier(self):
        assert self.result.supplier == "Natures Plus"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "13103372"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 2, 25)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 3, 27)

    def test_postcode_ship_to_not_bill_to(self):
        """Ship To is CF24 3LP (Roath), not Bill To CF10 1AE (Royal Arcade)."""
        assert self.result.deliver_to_postcode == "CF24 3LP"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5002

    def test_vat_net(self):
        assert self.result.vat_net == pytest.approx(160.19, abs=0.02)

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(0.0, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(32.03, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(192.22, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []
