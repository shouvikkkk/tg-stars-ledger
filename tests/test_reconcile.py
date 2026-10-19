"""Reconciler tests: the automatic healing paths, all against the fake transport."""

from tg_stars_ledger import Reconciler, deliver, outbox
from tg_stars_ledger.ingest import successful_payment
from .conftest import make_payment_update, make_query


def _crashed_payment(ledger, clock, charge="ch-9", amount=100):
    """successful_payment ingested, then worker A claims and 'crashes'."""
    _, payload = make_query(ledger, amount=amount)
    _, payment = successful_payment(ledger, make_payment_update(payload, charge, amount))
    assert deliver.claim_lease(ledger, payment["id"], "worker-A", lease_ttl=300)
    return payment, payload


def test_stuck_settled_payment_is_redriven(ledger, transport, clock):
    payment, _ = _crashed_payment(ledger, clock)
    clock[0] += 400  # lease expired
    transport.add_transaction("ch-9", 100)  # settlement truth
    rec = Reconciler(ledger, transport)
    report = rec.run_once()
    assert report["redriven"] == 1
    assert ledger.get_payment(payment["id"])["status"] == "pending"
    # the normal delivery path now completes it exactly once
    delivered = []
    assert deliver.run(ledger, payment["id"], delivered.append) == "delivered"
    assert len(delivered) == 1


def test_stuck_unsettled_payment_is_flagged(ledger, transport, clock):
    payment, _ = _crashed_payment(ledger, clock)
    clock[0] += 400
    # getStarTransactions knows nothing about this charge
    rec = Reconciler(ledger, transport, admin_chat_id=7)
    report = rec.run_once()
    assert report["flagged"] == 1
    assert ledger.get_payment(payment["id"])["status"] == "failed"
    # the reconciler drains the outbox in the same pass, so the alert was
    # already sent
    sent = [c for c in transport.calls
            if c["method"] == "sendMessage" and "text" in c]
    assert sent and "ch-9" in sent[0]["text"]


def test_lost_successful_payment_is_resurrected(ledger, transport, clock):
    """The update never arrived, but Telegram charged the buyer. The expired
    invoice + getStarTransactions is the only evidence left."""
    _, payload = make_query(ledger, amount=250, ttl=3600)
    clock[0] += 7200  # past expiry + grace, still no payment row
    assert ledger.get_payment_by_charge("ch-lost") is None
    transport.add_transaction("ch-lost", 250, invoice_payload=payload, user_id=42)
    rec = Reconciler(ledger, transport)
    report = rec.run_once()
    assert report["resurrected"] == 1
    payment = ledger.get_payment_by_charge("ch-lost")
    assert payment is not None and payment["amount"] == 250
    assert ledger.get_invoice(payload)["status"] == "paid"
    delivered = []
    assert deliver.run(ledger, payment["id"], delivered.append) == "delivered"
    assert delivered[0]["amount"] == 250


def test_unsettled_expired_invoice_just_expires(ledger, transport, clock):
    _, payload = make_query(ledger, ttl=3600)
    clock[0] += 7200
    rec = Reconciler(ledger, transport)
    report = rec.run_once()
    assert report["invoices_expired"] == 1
    assert report["resurrected"] == 0
    assert ledger.get_invoice(payload)["status"] == "expired"


def test_unfulfillable_delivery_queues_refund(ledger, transport, clock):
    _, payload = make_query(ledger)
    _, payment = successful_payment(ledger, make_payment_update(payload))

    def always_fails(ctx):
        raise RuntimeError("stock gone")

    assert deliver.run(ledger, payment["id"], always_fails, max_attempts=1) == "failed"
    rec = Reconciler(ledger, transport)
    report = rec.run_once()
    assert report["refunds_queued"] == 1
    assert report["outbox"]["refunds_sent"] == 1  # drained in the same pass
    assert ledger.get_payment(payment["id"])["status"] == "refunded"
    refund_calls = [c for c in transport.calls if c["method"] == "refundStarPayment"]
    assert refund_calls and refund_calls[0]["telegram_payment_charge_id"] == "ch-1"


def test_settlement_match_by_payload_not_charge(ledger, transport, clock):
    """Fallback key: TransactionPartnerUser.invoice_payload matches even when
    the charge id in our row is unknown (e.g. row built from a partial update)."""
    payment, payload = _crashed_payment(ledger, clock, charge="ch-partial")
    clock[0] += 400
    # transaction id does NOT match, but the source partner carries our payload
    transport.add_transaction("txn-unrelated-id", 100, invoice_payload=payload)
    rec = Reconciler(ledger, transport)
    report = rec.run_once()
    assert report["redriven"] == 1


def test_transport_failure_does_not_corrupt(ledger, transport, clock):
    from tg_stars_ledger.transport import ApiError
    payment, _ = _crashed_payment(ledger, clock)
    clock[0] += 400
    transport.fail_always = ApiError("network down", None)
    report = Reconciler(ledger, transport).run_once()
    assert report["errors"] and report["redriven"] == 0
    # stuck row untouched, still healable later
    assert ledger.get_payment(payment["id"])["status"] == "processing"
