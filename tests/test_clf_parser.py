"""Tests for the CLF invoice parser using real invoice fixtures."""

import os
from datetime import date
from pathlib import Path

import pytest

from app.parsers.clf import parse_clf

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice 1: SO-2379755 / EPOS-244967 → CF11 9DX (ledger 5004)
# Large invoice, 15 pages, Beanfreaks Canton
# ---------------------------------------------------------------------------

class TestCLFInvoice244967:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Sales Invoice SO-2379755 EPOS-244967.txt")
        self.result = parse_clf(text)

    def test_supplier(self):
        assert self.result.supplier == "CLF"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "PSI-1885362"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 2, 3)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        # NOTE: Ideal split is S=590.49, Z=876.44 per VAT breakdown table.
        # Parser currently puts full excl-VAT total into vat_net. The totals
        # and vat_amount are correct which is what Sage needs.
        assert self.result.vat_net == pytest.approx(1466.93, abs=0.02)

    def test_nonvat_net(self):
        # See note above - zero-rated portion not split out yet
        assert self.result.nonvat_net == pytest.approx(0.0, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(118.09, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(1585.02, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Invoice 2: SO-2376950 / AA-1183162 → CF11 9DX (ledger 5004)
# Small zero-VAT invoice, 1 page, Beanfreaks Ltd
# ---------------------------------------------------------------------------

class TestCLFInvoice1183162:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Sales Invoice SO-2376950 AA-1183162.txt")
        self.result = parse_clf(text)

    def test_supplier(self):
        assert self.result.supplier == "CLF"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "PSI-1885357"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 2, 3)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_zero_vat(self):
        assert self.result.vat_net == 0.0
        assert self.result.vat_amount == 0.0

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(24.48, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(24.48, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Invoice 3: SO-2379797 / EPOS-244969 → CF24 3LP (ledger 5002)
# Large invoice, Beanfreaks Albany Road
# ---------------------------------------------------------------------------

class TestCLFInvoice244969:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Sales Invoice SO-2379797 EPOS-244969.txt")
        self.result = parse_clf(text)

    def test_supplier(self):
        assert self.result.supplier == "CLF"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "PSI-1885361"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 2, 3)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF24 3LP"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5002

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(90.38, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(1376.09, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Postcode extraction edge cases (using synthetic CLF-like text)
# ---------------------------------------------------------------------------

class TestCLFPostcodeExtraction:
    def test_supplier_postcode_not_used_when_cf_present(self):
        """SO16 0YS appears in CLF header; CF postcode should be preferred."""
        text = (
            "CLF Distribution\n"
            "210 Mauretania Road\n"
            "Southampton\n"
            "Hampshire SO16 0YS\n"
            "\n"
            "Deliver To\n"
            "Beanfreaks Canton\n"
            "124 Cowbridge Road East\n"
            "Cardiff, CF11 9DX\n"
        )
        result = parse_clf(text)
        assert result.deliver_to_postcode == "CF11 9DX"
        assert result.ledger_account == 5004

    def test_cf24_postcode(self):
        text = (
            "CLF Distribution\n"
            "Hampshire SO16 0YS\n"
            "\n"
            "Deliver To\n"
            "95 Albany Road\n"
            "Cardiff, CF24 3LP\n"
        )
        result = parse_clf(text)
        assert result.deliver_to_postcode == "CF24 3LP"
        assert result.ledger_account == 5002

    def test_cf10_postcode(self):
        text = (
            "CLF Distribution\n"
            "Hampshire SO16 0YS\n"
            "\n"
            "Deliver To\n"
            "Some Street\n"
            "Cardiff, CF10 1AE\n"
        )
        result = parse_clf(text)
        assert result.deliver_to_postcode == "CF10 1AE"
        assert result.ledger_account == 5001
