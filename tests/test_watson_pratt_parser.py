"""Tests for the Watson & Pratt invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.watson_pratt import parse_watson_pratt

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice IN-111662 → CF11 9DX (ledger 5004), Beanfreaks Canton
# Mostly zero-rated produce + one 20% delivery charge
# ---------------------------------------------------------------------------

class TestWatsonPrattInvoice111662:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Watson Pratt Invoice IN-111662.txt")
        self.result = parse_watson_pratt(text)

    def test_supplier(self):
        assert self.result.supplier == "Watson & Pratt"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "IN-111662"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 11, 19)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 12, 17)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        """Only 20% item is delivery charge: 2.50."""
        assert self.result.vat_net == pytest.approx(2.50, abs=0.02)

    def test_nonvat_net(self):
        """Zero-rated produce: 214.58 - 2.50 = 212.08."""
        assert self.result.nonvat_net == pytest.approx(212.08, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(0.50, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(215.08, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []
