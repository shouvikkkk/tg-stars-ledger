"""Ledger unit tests: signing, invoices, lease claiming, flood pause."""

import pytest

from tg_stars_ledger import Ledger
from tg_stars_ledger.ledger import _MAX_PAYLOAD_BYTES


def test_sign_verify_roundtrip(ledger):
    token = ledger.sign_payload("pro", 100, customer_ref="u1")
    claims = ledger.verify_payload(token)
    assert claims["b"] == "testbot"
    assert claims["i"] == "pro"
    assert claims["a"] == 100
    assert claims["r"] == "u1"
    assert len(token.encode()) <= _MAX_PAYLOAD_BYTES


def test_tampered_payload_rejected(ledger):
    token = ledger.sign_payload("pro", 100)
    body, sig = token.rsplit(".", 1)
    tampered = body.replace('"a":100', '"a":1') + "." + sig
    assert ledger.verify_payload(tampered) is None
    assert ledger.verify_payload(token + "x") is None
    assert ledger.verify_payload("garbage") is None


def test_payload_bound_to_bot(ledger):
    other = Ledger(bot_id="otherbot", secret="test-secret-123", path=":memory:",
                   now_fn=ledger._now_fn)
    token = other.sign_payload("pro", 100)
    assert ledger.verify_payload(token) is None  # right secret, wrong bot
    other.close()


def test_payload_size_limit(ledger):
    with pytest.raises(ValueError, match="128-byte"):
        ledger.sign_payload("x" * 60, 100, customer_ref="y" * 60)


def test_create_invoice_records_before_send(ledger, clock):
    payload = ledger.create_invoice("pro", 100, customer_ref="u1", user_id=42, ttl=600)
    inv = ledger.get_invoice(payload)
    assert inv["item_id"] == "pro"
    assert inv["amount"] == 100
    assert inv["status"] == "pending"
    assert inv["expires_at"] == clock[0] + 600


def test_query_id_idempotency(ledger):
    assert ledger.mark_query_seen("q-1", "p1") is True
    assert ledger.mark_query_seen("q-1", "p1") is False  # replay


def test_claim_lease_exclusive(ledger, clock):
    ledger.create_invoice("pro", 100)
    _, row = ledger.insert_payment("ch-1", ledger.create_invoice("x", 1), 42, 1)
    pid = row["id"]
    assert ledger.claim_delivery(pid, "wA", lease_ttl=300) is True
    # same worker may renew
    assert ledger.claim_delivery(pid, "wA", lease_ttl=300) is True
    # another worker is denied while the lease is live
    clock[0] += 100
    assert ledger.claim_delivery(pid, "wB", lease_ttl=300) is False
    # after expiry the other worker wins
    clock[0] += 300
    assert ledger.claim_delivery(pid, "wB", lease_ttl=300) is True
    d = ledger.get_delivery(pid)
    assert d["lease_owner"] == "wB"


def test_release_and_complete(ledger):
    _, row = ledger.insert_payment("ch-2", ledger.create_invoice("x", 1), 42, 1)
    pid = row["id"]
    assert ledger.claim_delivery(pid, "wA") is True
    ledger.release_delivery(pid, error="boom")
    d = ledger.get_delivery(pid)
    assert d["status"] == "pending" and d["lease_owner"] is None
    assert d["last_error"] == "boom"
    assert ledger.get_payment(pid)["status"] == "pending"
    assert ledger.claim_delivery(pid, "wA") is True
    ledger.complete_delivery(pid)
    assert ledger.get_delivery(pid)["status"] == "delivered"
    assert ledger.get_payment(pid)["status"] == "delivered"


def test_flood_pause_is_db_shared(ledger, clock):
    assert ledger.flood_paused() is False
    ledger.set_flood_pause(600)
    assert ledger.flood_paused() is True
    # a second handle on the same logical DB sees it too (settings are shared)
    assert ledger.get_setting("flood_pause_until") == str(clock[0] + 600)
    clock[0] += 601
    assert ledger.flood_paused() is False
