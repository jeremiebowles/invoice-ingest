"""Tests for the Viridian invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.viridian import parse_viridian

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice 455863 → CF10 1AE (ledger 5001), all standard-rated (T1)
# ---------------------------------------------------------------------------

class TestViridianInvoice455863:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("InvoiceCopy - 455863 - Beanfreaks Ltd - 270126_1306.txt")
        self.result = parse_viridian(text)

    def test_supplier(self):
        assert self.result.supplier == "Viridian"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "455863"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 11, 27)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 12, 27)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF10 1AE"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5001

    def test_vat_net(self):
        assert self.result.vat_net == pytest.approx(200.58, abs=0.02)

    def test_nonvat_net(self):
        assert self.result.nonvat_net == pytest.approx(0.0, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(40.11, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(240.69, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []


# ---------------------------------------------------------------------------
# Invoice 396410 → CF10 1AE (ledger 5001), mixed T1 + T0 (zero-rated)
# ---------------------------------------------------------------------------

class TestViridianInvoice396410:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("InvoiceCopy - 396410 - Beanfreaks Ltd - 240425_1312.txt")
        self.result = parse_viridian(text)

    def test_supplier(self):
        assert self.result.supplier == "Viridian"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "396410"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2025, 2, 25)

    def test_due_date(self):
        assert self.result.due_date == date(2025, 3, 27)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF10 1AE"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5001

    def test_vat_net(self):
        """T1 line: 178.09 at 20%."""
        assert self.result.vat_net == pytest.approx(178.09, abs=0.02)

    def test_nonvat_net(self):
        """T0 line: 12.42 at 0%."""
        assert self.result.nonvat_net == pytest.approx(12.42, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(35.62, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(226.13, abs=0.02)

    def test_no_warnings(self):
        assert self.result.warnings == []
