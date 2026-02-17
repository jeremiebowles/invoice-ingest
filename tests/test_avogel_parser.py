"""Tests for the A.Vogel invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.avogel import parse_avogel

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice 01633256 → CF24 3LP (ledger 5002), mixed 20% + zero-rated
# ---------------------------------------------------------------------------

class TestAVogelInvoice01633256:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("AVogel BEA028.txt")
        self.result = parse_avogel(text)

    def test_supplier(self):
        assert self.result.supplier == "A.Vogel"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "01633256"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 9, 23)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 10, 23)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF24 3LP"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5002

    def test_vat_net(self):
        """20% rated goods: 161.74."""
        assert self.result.vat_net == pytest.approx(161.74, abs=0.02)

    def test_nonvat_net(self):
        """Zero-rated goods (Herbamare): 6.55."""
        assert self.result.nonvat_net == pytest.approx(6.55, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(32.30, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(200.59, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []
