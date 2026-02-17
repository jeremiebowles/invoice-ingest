"""Tests for the Hunts invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.hunts import parse_hunts

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Multi-invoice PDF: 3 invoices across 4 pages (pages 1-2 are one invoice)
# ---------------------------------------------------------------------------

class TestHuntsMultiInvoiceSplit:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Hunts Reprinted Invoices CONTQD.txt")
        self.results = parse_hunts(text)

    def test_three_invoices_from_four_pages(self):
        """Pages 1-2 merge into one invoice; pages 3 and 4 are separate."""
        assert len(self.results) == 3

    def test_all_supplier_hunts(self):
        for r in self.results:
            assert r.supplier == "Hunts"

    def test_all_not_credit(self):
        for r in self.results:
            assert r.is_credit is False


# ---------------------------------------------------------------------------
# Invoice 510-303515 → CF24 3LP (ledger 5002), Beanfreaks Roath
# 2-page invoice, mixed zero-rated + 20% VAT
# ---------------------------------------------------------------------------

class TestHuntsInvoice303515:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Hunts Reprinted Invoices CONTQD.txt")
        self.result = parse_hunts(text)[0]

    def test_invoice_number(self):
        assert self.result.supplier_reference == "510-303515"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 2, 27)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 3, 29)

    def test_postcode(self):
        """Delivery to Roath (CF24 3LP), not billing Canton (CF11 9DX)."""
        assert self.result.deliver_to_postcode == "CF24 3LP"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5002

    def test_vat_net(self):
        """20% rated goods: 115.20."""
        assert self.result.vat_net == pytest.approx(115.20, abs=0.02)

    def test_nonvat_net(self):
        """Zero-rated goods: 148.22."""
        assert self.result.nonvat_net == pytest.approx(148.22, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(23.04, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(286.46, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Invoice 510-306887 → CF11 9DX (ledger 5004), Beanfreaks Canton
# Single page, 100% zero-rated (one item: Alpro Custard)
# ---------------------------------------------------------------------------

class TestHuntsInvoice306887:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Hunts Reprinted Invoices CONTQD.txt")
        self.result = parse_hunts(text)[1]

    def test_invoice_number(self):
        assert self.result.supplier_reference == "510-306887"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 3, 4)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 4, 3)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_zero_vat(self):
        assert self.result.vat_net == 0.0
        assert self.result.vat_amount == 0.0

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(1.76, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(1.76, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Invoice 510-306889 → CF11 9DX (ledger 5004), Beanfreaks Canton
# Single page, mixed zero-rated + 20% VAT
# ---------------------------------------------------------------------------

class TestHuntsInvoice306889:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Hunts Reprinted Invoices CONTQD.txt")
        self.result = parse_hunts(text)[2]

    def test_invoice_number(self):
        assert self.result.supplier_reference == "510-306889"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 3, 4)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 4, 3)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        """20% rated goods: 26.70."""
        assert self.result.vat_net == pytest.approx(26.70, abs=0.02)

    def test_nonvat_net(self):
        """Zero-rated goods: 73.22."""
        assert self.result.nonvat_net == pytest.approx(73.22, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(5.34, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(105.26, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []
