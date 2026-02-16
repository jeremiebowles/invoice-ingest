"""Tests for shared postcode/ledger helpers in parse_utils."""

from app.parse_utils import (
    extract_delivery_postcode,
    normalize_postcode,
    LEDGER_MAP,
    parse_money,
    parse_date,
)


# ---------------------------------------------------------------------------
# normalize_postcode
# ---------------------------------------------------------------------------

def test_normalize_postcode_standard():
    assert normalize_postcode("CF101AE") == "CF10 1AE"


def test_normalize_postcode_already_spaced():
    assert normalize_postcode("CF10 1AE") == "CF10 1AE"


def test_normalize_postcode_lowercase():
    assert normalize_postcode("cf24 3lp") == "CF24 3LP"


def test_normalize_postcode_empty():
    assert normalize_postcode("") is None


def test_normalize_postcode_too_short():
    assert normalize_postcode("CF1") is None


# ---------------------------------------------------------------------------
# extract_delivery_postcode – prefer known store postcodes
# ---------------------------------------------------------------------------

def test_extract_prefers_known_postcode_over_first():
    """Supplier postcode appears first, but known CF postcode should be preferred."""
    text = "Supplier Address SO16 0YS\nDeliver To Cardiff CF10 1AE"
    assert extract_delivery_postcode(text) == "CF10 1AE"


def test_extract_prefers_known_when_multiple_unknown():
    text = "BN1 1AA and SO16 0YS and CF24 3LP somewhere"
    assert extract_delivery_postcode(text) == "CF24 3LP"


def test_extract_falls_back_to_first_when_no_known():
    text = "Warehouse at SO16 0YS then factory at BN1 1AA"
    assert extract_delivery_postcode(text) == "SO16 0YS"


def test_extract_returns_none_when_no_postcodes():
    assert extract_delivery_postcode("no postcodes here") is None


def test_extract_returns_none_for_empty_string():
    assert extract_delivery_postcode("") is None


def test_extract_all_three_stores():
    for pc, ledger in LEDGER_MAP.items():
        text = f"Random address BN1 1AA and delivery {pc}"
        result = extract_delivery_postcode(text)
        assert result == pc, f"Expected {pc}, got {result}"
        assert LEDGER_MAP[result] == ledger


# ---------------------------------------------------------------------------
# LEDGER_MAP sanity
# ---------------------------------------------------------------------------

def test_ledger_map_has_three_entries():
    assert len(LEDGER_MAP) == 3
    assert LEDGER_MAP["CF10 1AE"] == 5001
    assert LEDGER_MAP["CF24 3LP"] == 5002
    assert LEDGER_MAP["CF11 9DX"] == 5004


# ---------------------------------------------------------------------------
# parse_money
# ---------------------------------------------------------------------------

def test_parse_money_basic():
    assert parse_money("1,466.93") == 1466.93


def test_parse_money_with_pound():
    assert parse_money("£118.09") == 118.09


def test_parse_money_none():
    assert parse_money(None) is None


def test_parse_money_empty():
    assert parse_money("") is None


# ---------------------------------------------------------------------------
# parse_date
# ---------------------------------------------------------------------------

def test_parse_date_dd_mm_yyyy():
    d = parse_date("03/02/2026")
    assert d is not None
    assert d.day == 3
    assert d.month == 2
    assert d.year == 2026


def test_parse_date_none():
    assert parse_date(None) is None
