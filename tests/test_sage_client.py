"""Tests for Sage ledger lookup and due date handling in sage_client."""

from datetime import date

import pytest

from app import sage_client
from app.models import InvoiceData


def _invoice(ledger_account=7100):
    return InvoiceData(
        supplier="APAM",
        supplier_reference="May26",
        invoice_date=date(2026, 5, 1),
        due_date=None,
        description="RENT",
        is_credit=False,
        deliver_to_postcode=None,
        ledger_account=ledger_account,
        contact_id="contact-1",
        vat_net=2153.80,
        nonvat_net=0.0,
        vat_amount=430.76,
        total=2584.56,
        warnings=[],
    )


class _Resp:
    def __init__(self, status_code=200, data=None):
        self.status_code = status_code
        self._data = data or {}
        self.text = ""

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.fixture(autouse=True)
def _clear_cache():
    sage_client._LEDGER_ACCOUNT_IDS.clear()
    yield
    sage_client._LEDGER_ACCOUNT_IDS.clear()


# ---------------------------------------------------------------------------
# _lookup_ledger_account_id
# ---------------------------------------------------------------------------

def test_lookup_matches_nominal_code(monkeypatch):
    calls = []

    def fake_get(url, headers, params, timeout):
        calls.append(params)
        return _Resp(data={"$items": [
            {"id": "other", "nominal_code": 7127, "displayed_as": "Repairs (7127)"},
            {"id": "rent-id", "nominal_code": 7100, "displayed_as": "Rent Arcade Cardiff (7100)"},
        ]})

    monkeypatch.setattr(sage_client.requests, "get", fake_get)
    assert sage_client._lookup_ledger_account_id("tok", "biz", 7100) == "rent-id"
    assert len(calls) == 1


def test_lookup_matches_displayed_as_suffix(monkeypatch):
    monkeypatch.setattr(
        sage_client.requests,
        "get",
        lambda url, headers, params, timeout: _Resp(data={"$items": [
            {"id": "rent-id", "displayed_as": "Rent Arcade Cardiff (7100)"},
        ]}),
    )
    assert sage_client._lookup_ledger_account_id("tok", "biz", 7100) == "rent-id"


def test_lookup_falls_back_when_search_rejected(monkeypatch):
    responses = [
        _Resp(status_code=400),
        _Resp(data={"$items": [{"id": "rent-id", "nominal_code": "7100"}]}),
    ]
    monkeypatch.setattr(sage_client.requests, "get", lambda url, headers, params, timeout: responses.pop(0))
    assert sage_client._lookup_ledger_account_id("tok", "biz", 7100) == "rent-id"


def test_lookup_returns_none_when_missing(monkeypatch):
    monkeypatch.setattr(
        sage_client.requests,
        "get",
        lambda url, headers, params, timeout: _Resp(data={"$items": [{"id": "x", "nominal_code": 71000}]}),
    )
    assert sage_client._lookup_ledger_account_id("tok", "biz", 7100) is None


def test_lookup_is_cached(monkeypatch):
    calls = []

    def fake_get(url, headers, params, timeout):
        calls.append(params)
        return _Resp(data={"$items": [{"id": "rent-id", "nominal_code": 7100}]})

    monkeypatch.setattr(sage_client.requests, "get", fake_get)
    sage_client._lookup_ledger_account_id("tok", "biz", 7100)
    sage_client._lookup_ledger_account_id("tok", "biz", 7100)
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# _get_ledger_account_id
# ---------------------------------------------------------------------------

def test_env_mapped_ledger_skips_lookup(monkeypatch):
    monkeypatch.setenv("SAGE_LEDGER_5001_ID", "env-5001")
    monkeypatch.setattr(sage_client, "_lookup_ledger_account_id", lambda *a: pytest.fail("lookup called"))
    assert sage_client._get_ledger_account_id(_invoice(5001), "tok", "biz") == "env-5001"


def test_unmapped_ledger_uses_lookup(monkeypatch):
    monkeypatch.setattr(sage_client, "_lookup_ledger_account_id", lambda tok, biz, code: f"id-{code}")
    assert sage_client._get_ledger_account_id(_invoice(7100), "tok", "biz") == "id-7100"


def test_unmapped_ledger_without_token_returns_none():
    assert sage_client._get_ledger_account_id(_invoice(7100)) is None


# ---------------------------------------------------------------------------
# post_purchase_invoice due date
# ---------------------------------------------------------------------------

def _capture_post(monkeypatch):
    sent = {}
    monkeypatch.setenv("SAGE_BUSINESS_ID", "biz")
    monkeypatch.setattr(sage_client, "_refresh_access_token", lambda: "tok")
    monkeypatch.setattr(sage_client, "_lookup_ledger_account_id", lambda tok, biz, code: "rent-id")
    monkeypatch.setattr(sage_client, "_already_exists", lambda *a, **k: False)

    def fake_post(url, headers, json, timeout):
        sent.update(json)
        return _Resp(data={"id": "new-id"})

    monkeypatch.setattr(sage_client.requests, "post", fake_post)
    monkeypatch.setattr(sage_client.requests, "get", lambda *a, **k: _Resp())
    return sent


def test_post_uses_explicit_due_date(monkeypatch):
    sent = _capture_post(monkeypatch)
    sage_client.post_purchase_invoice(_invoice(), date(2026, 6, 1))
    assert sent["purchase_invoice"]["due_date"] == "2026-06-01"
    assert sent["purchase_invoice"]["invoice_lines"][0]["ledger_account_id"] == "rent-id"


def test_post_defaults_due_date_to_30_days(monkeypatch):
    sent = _capture_post(monkeypatch)
    sage_client.post_purchase_invoice(_invoice())
    assert sent["purchase_invoice"]["due_date"] == "2026-05-31"
