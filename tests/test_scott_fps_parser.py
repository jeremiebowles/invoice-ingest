"""Tests for the Scott FPS invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.scott_fps import parse_scott_fps

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


class TestScottFPSInvoiceINV10289:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Scott FPS Invoice INV-10289.txt")
        self.result = parse_scott_fps(text)

    def test_supplier(self):
        assert self.result.supplier == "Scott FPS"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "INV-10289"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 4, 9)

    def test_due_date(self):
        assert self.result.due_date == date(2026, 5, 9)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        assert self.result.vat_net == pytest.approx(43.80, abs=0.02)

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(0.0, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(8.76, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(52.56, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []
