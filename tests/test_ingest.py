"""Ingest tests: pre-checkout validation and idempotent payment ingest."""

from tg_stars_ledger.ingest import pre_checkout, successful_payment
from .conftest import make_payment_update, make_query


def test_pre_checkout_happy_path(ledger):
    query, payload = make_query(ledger)
    ok, err = pre_checkout(ledger, query)
    assert (ok, err) == (True, None)
    assert ledger.get_invoice(payload)["status"] == "prechecked"


def test_pre_checkout_duplicate_query_is_idempotent(ledger):
    query, _ = make_query(ledger, qid="q-dup")
    assert pre_checkout(ledger, query) == (True, None)
    assert pre_checkout(ledger, query) == (True, None)  # replay: same answer


def test_pre_checkout_bad_signature(ledger):
    # A tampered payload never matches a recorded invoice: declined as
    # unknown. (Raw signature tampering is covered at verify_payload level
    # in test_ledger.py.)
    query, payload = make_query(ledger)
    query["invoice_payload"] = payload[:-4] + "ffff"
    ok, err = pre_checkout(ledger, query)
    assert ok is False and "no longer valid" in err


def test_pre_checkout_expired(ledger, clock):
    query, payload = make_query(ledger, ttl=60)
    clock[0] += 61
    ok, err = pre_checkout(ledger, query)
    assert ok is False and "expired" in err


def test_pre_checkout_amount_mismatch(ledger):
    query, _ = make_query(ledger, amount=100)
    query["total_amount"] = 1
    ok, err = pre_checkout(ledger, query)
    assert ok is False and "mismatch" in err


def test_pre_checkout_unknown_invoice(ledger):
    ok, err = pre_checkout(ledger, {
        "id": "q-x", "from": {"id": 1}, "currency": "XTR",
        "total_amount": 5, "invoice_payload": "nope",
    })
    assert ok is False


def test_pre_checkout_wrong_currency(ledger):
    query, _ = make_query(ledger)
    query["currency"] = "USD"
    ok, err = pre_checkout(ledger, query)
    assert ok is False and "currency" in err.lower()


def test_successful_payment_created_then_duplicate(ledger):
    _, payload = make_query(ledger)
    upd = make_payment_update(payload)
    status, payment = successful_payment(ledger, upd)
    assert status == "created"
    assert payment["charge_id"] == "ch-1"
    assert ledger.get_invoice(payload)["status"] == "paid"
    # duplicate delivery of the same update collapses
    status2, payment2 = successful_payment(ledger, upd)
    assert status2 == "duplicate"
    assert payment2["id"] == payment["id"]
    # still exactly one payment row
    assert ledger.get_payment_by_charge("ch-1")["id"] == payment["id"]


def test_successful_payment_wrong_currency_rejected(ledger):
    _, payload = make_query(ledger)
    upd = make_payment_update(payload)
    upd["message"]["successful_payment"]["currency"] = "USD"
    status, _ = successful_payment(ledger, upd)
    assert status == "invalid"
    assert ledger.get_payment_by_charge("ch-1") is None


def test_successful_payment_amount_mismatch_marked_failed(ledger):
    _, payload = make_query(ledger, amount=100)
    upd = make_payment_update(payload, amount=999)
    status, payment = successful_payment(ledger, upd)
    assert status == "invalid"
    assert payment["status"] == "failed"
    assert "mismatch" in payment["note"]
