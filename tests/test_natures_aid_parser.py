"""Tests for the Nature's Aid invoice parser using real invoice fixtures."""

from datetime import date
from pathlib import Path

import pytest

from app.parsers.natures_aid import parse_natures_aid

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture {name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# Invoice S-INV-26-000693 → CF11 9DX (ledger 5004), Beanfreaks Canton
# Mixed 20% + zero-rated (Coconut Oil), 2 pages
# ---------------------------------------------------------------------------

class TestNaturesAidInvoice000693:
    @pytest.fixture(autouse=True)
    def parse(self):
        text = _load_fixture("Natures Aid Invoice S-INV-26-000693.txt")
        self.result = parse_natures_aid(text)

    def test_supplier(self):
        assert self.result.supplier == "Natures Aid"

    def test_invoice_number(self):
        assert self.result.supplier_reference == "S-INV-26-000693"

    def test_invoice_date(self):
        assert self.result.invoice_date == date(2026, 1, 19)

    def test_due_date(self):
        assert self.result.due_date == date(2026, 2, 18)

    def test_postcode(self):
        assert self.result.deliver_to_postcode == "CF11 9DX"

    def test_ledger_account(self):
        assert self.result.ledger_account == 5004

    def test_vat_net(self):
        """All 20% items sum to 114.97."""
        assert self.result.vat_net == pytest.approx(114.97, abs=0.02)

    def test_nonvat_net(self):
        """Coconut Oil is zero-rated: 4.59."""
        assert self.result.nonvat_net == pytest.approx(4.59, abs=0.02)

    def test_vat_amount(self):
        assert self.result.vat_amount == pytest.approx(22.99, abs=0.02)

    def test_total(self):
        assert self.result.total == pytest.approx(142.55, abs=0.02)

    def test_not_credit(self):
        assert self.result.is_credit is False

    def test_no_warnings(self):
        assert self.result.warnings == []
