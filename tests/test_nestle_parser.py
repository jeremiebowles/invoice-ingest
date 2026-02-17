"""Tests for the Nestle invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.nestle import parse_nestle

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice 1337640820 → CF10 1AE (ledger 5001), 2-page invoice + 2 T&C pages
# All items 20% VAT (Solgar supplements)
# ---------------------------------------------------------------------------

class TestNestleInvoice1337640820:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Nestle Invoice 1337640820.txt")
        self.result = parse_nestle(text)

    def test_supplier(self):
        assert self.result.supplier == "Nestle"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "1337640820"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 11, 20)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 12, 16)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF10 1AE"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5001

    def test_vat_net(self):
        assert self.result.vat_net == pytest.approx(237.39, abs=0.02)

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(0.0, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(47.48, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(284.87, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Invoice 1337721448 → CF11 9DX (ledger 5004), 1-page invoice + 1 T&C page
# Delivery to Canton, billed to Royal Arcade; all 20% VAT
# ---------------------------------------------------------------------------

class TestNestleInvoice1337721448:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Nestle Invoice 1337721448.txt")
        self.result = parse_nestle(text)

    def test_supplier(self):
        assert self.result.supplier == "Nestle"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "1337721448"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 12, 9)

    def test_due_date(self):
        assert self.result.due_date == date(2026, 1, 16)

    def test_postcode_delivery_not_billing(self):
        """Delivery to CF11 9DX (Canton), not billing CF10 1AE (Royal Arcade)."""
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        assert self.result.vat_net == pytest.approx(146.73, abs=0.02)

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(0.0, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(29.35, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(176.08, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []
