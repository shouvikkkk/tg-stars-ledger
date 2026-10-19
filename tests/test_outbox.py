"""Outbox tests: retry budget, backoff, dead letters, flood pause."""

from tg_stars_ledger import outbox
from tg_stars_ledger.transport import ApiError
from .conftest import make_payment_update, make_query
from tg_stars_ledger.ingest import successful_payment


def _refundable(ledger):
    _, payload = make_query(ledger)
    _, payment = successful_payment(ledger, make_payment_update(payload))
    outbox.enqueue_refund(ledger, payment["id"], "ch-1", 42, reason="test")
    return payment


def test_refund_sent_on_first_try(ledger, transport):
    payment = _refundable(ledger)
    stats = outbox.process_due(ledger, transport)
    assert stats["refunds_sent"] == 1
    refund_calls = [c for c in transport.calls if c["method"] == "refundStarPayment"]
    assert refund_calls[0]["telegram_payment_charge_id"] == "ch-1"
    assert refund_calls[0]["user_id"] == 42
    assert ledger.get_payment(payment["id"])["status"] == "refunded"


def test_refund_retry_budget_then_dead_letter(ledger, transport, clock):
    ledger.set_setting("admin_chat_id", "99")
    _, payload = make_query(ledger)
    from tg_stars_ledger.ingest import successful_payment
    _, payment = successful_payment(ledger, make_payment_update(payload))
    outbox.enqueue_refund(ledger, payment["id"], "ch-1", 42,
                          reason="test", max_attempts=3)
    transport.fail_always = ApiError("boom", 500)
    # attempt 1 fails -> queued with backoff; 2 fails; 3 fails -> dead
    for _ in range(3):
        stats = outbox.process_due(ledger, transport)
        clock[0] += 3700  # past the backoff window
    row = ledger._conn.execute("SELECT * FROM refunds").fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == 3
    assert stats["dead"] == 1
    # dead refund pages the admin; the alert goes out once the transport heals
    transport.fail_always = None
    clock[0] += 3700
    stats = outbox.process_due(ledger, transport)
    assert stats["alerts_sent"] == 1
    alert = [c for c in transport.calls
             if c["method"] == "sendMessage" and "text" in c][0]
    assert "DEAD refund" in alert["text"]


def test_backoff_grows(ledger, transport, clock):
    _refundable(ledger)
    transport.fail_always = ApiError("boom", 500)
    outbox.process_due(ledger, transport)
    first_retry = ledger._conn.execute("SELECT next_retry_at FROM refunds").fetchone()[0]
    assert first_retry == clock[0] + 60  # 60 * 2**0
    clock[0] += 61
    outbox.process_due(ledger, transport)
    second_retry = ledger._conn.execute("SELECT next_retry_at FROM refunds").fetchone()[0]
    assert second_retry == clock[0] + 120  # 60 * 2**1


def test_charge_already_refunded_is_success(ledger, transport):
    _refundable(ledger)
    transport.fail_always = ApiError("Bad Request: CHARGE_ALREADY_REFUNDED", 400)
    stats = outbox.process_due(ledger, transport)
    assert stats["refunds_sent"] == 1  # idempotent: not a failure, not a loop
    row = ledger._conn.execute("SELECT status FROM refunds").fetchone()
    assert row["status"] == "sent"


def test_flood_pause_blocks_sending(ledger, transport):
    _refundable(ledger)
    ledger.set_flood_pause(600)
    stats = outbox.process_due(ledger, transport)
    assert stats["paused"] == 1
    assert not [c for c in transport.calls if c["method"] == "refundStarPayment"]
    row = ledger._conn.execute("SELECT status FROM refunds").fetchone()
    assert row["status"] == "queued"


def test_requeue_dead(ledger, transport, clock):
    _, payload = make_query(ledger)
    from tg_stars_ledger.ingest import successful_payment as _sp
    _, payment = _sp(ledger, make_payment_update(payload))
    outbox.enqueue_refund(ledger, payment["id"], "ch-1", 42, max_attempts=3)
    transport.fail_always = ApiError("boom", 500)
    for _ in range(3):
        outbox.process_due(ledger, transport)
        clock[0] += 3700
    row = ledger._conn.execute("SELECT id FROM refunds").fetchone()
    assert outbox.requeue_dead(ledger, "refund", row["id"]) is True
    transport.fail_always = None
    stats = outbox.process_due(ledger, transport)
    assert stats["refunds_sent"] == 1
    assert ledger.get_payment(payment["id"])["status"] == "refunded"


def test_enqueue_refund_idempotent(ledger):
    _, payload = make_query(ledger)
    from tg_stars_ledger.ingest import successful_payment
    _, payment = successful_payment(ledger, make_payment_update(payload))
    assert outbox.enqueue_refund(ledger, payment["id"], "ch-1", 42) is True
    assert outbox.enqueue_refund(ledger, payment["id"], "ch-1", 42) is False
