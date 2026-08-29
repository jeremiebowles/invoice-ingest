"""Tests for the Breccles invoice parser using a real invoice fixture."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.breccles import parse_breccles

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


class TestBrecclesInvoiceINV1085:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Breccles Invoice INV-1085.txt")
        self.result = parse_breccles(text)

    def test_supplier(self):
        assert self.result.supplier == "Breccles"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "INV-1085"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 8, 29)

    def test_due_date(self):
        assert self.result.due_date == date(2026, 9, 28)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        assert self.result.vat_net == pytest.approx(124.86, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(24.97, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(149.83, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []
