"""Tests for the Essential Trading invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.essential import parse_essential

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice 42276 → CF11 9DX (ledger 5004), Beanfreaks Canton
# Mixed T0 (zero-rated) + T1 (20% VAT on Henna)
# ---------------------------------------------------------------------------

class TestEssentialInvoice42276:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Essential Invoice 42276.txt")
        self.result = parse_essential(text)

    def test_supplier(self):
        assert self.result.supplier == "Essential Trading"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "42276"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 2, 17)

    def test_due_date(self):
        assert self.result.due_date is None

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        """T1 line: 20% rated net = 7.67."""
        assert self.result.vat_net == pytest.approx(7.67, abs=0.02)

    def test_nonvat_net(self):
        """T0 line: zero-rated net = 208.17."""
        assert self.result.nonvat_net == pytest.approx(208.17, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(1.53, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(217.37, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Invoice 41678 → CF11 9DX (ledger 5004), Beanfreaks Canton
# Mixed T0 (zero-rated food) + T1 (20% VAT on chocolate bars)
# ---------------------------------------------------------------------------

class TestEssentialInvoice41678:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Essential Invoice 41678.txt")
        self.result = parse_essential(text)

    def test_supplier(self):
        assert self.result.supplier == "Essential Trading"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "41678"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 2, 10)

    def test_due_date(self):
        assert self.result.due_date is None

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        """T1 line: 20% rated net = 55.60."""
        assert self.result.vat_net == pytest.approx(55.60, abs=0.02)

    def test_nonvat_net(self):
        """T0 line: zero-rated net = 236.89."""
        assert self.result.nonvat_net == pytest.approx(236.89, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(11.12, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(303.61, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []
