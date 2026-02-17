"""Tests for the Bio-Nature invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.bionature import parse_bionature

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice 168386 → CF10 1AE (ledger 5001), 2-page invoice, all 20% VAT
# ---------------------------------------------------------------------------

class TestBioNatureInvoice168386:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("BioNature Invoice 168386.txt")
        self.result = parse_bionature(text)

    def test_supplier(self):
        assert self.result.supplier == "Bio-Nature"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "168386"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 2, 3)

    def test_due_date(self):
        assert self.result.due_date == date(2026, 3, 5)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF10 1AE"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5001

    def test_vat_net(self):
        assert self.result.vat_net == pytest.approx(275.06, abs=0.02)

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(0.0, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(54.98, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(330.04, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Invoice 162631 → CF11 9DX (ledger 5004), 1-page invoice, all 20% VAT
# Includes a free item (£0.00 line)
# ---------------------------------------------------------------------------

class TestBioNatureInvoice162631:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("BioNature Invoice 162631.txt")
        self.result = parse_bionature(text)

    def test_supplier(self):
        assert self.result.supplier == "Bio-Nature"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "162631"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 5, 19)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 6, 18)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        assert self.result.vat_net == pytest.approx(127.62, abs=0.02)

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(0.0, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(25.52, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(153.14, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []
