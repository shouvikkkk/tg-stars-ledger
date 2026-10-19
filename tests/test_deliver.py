"""Delivery tests: duplicates, crashes, contention, error paths."""

import threading

import pytest

from tg_stars_ledger import deliver
from tg_stars_ledger.ingest import successful_payment
from .conftest import make_payment_update, make_query


def _paid(ledger, charge="ch-1", amount=100):
    _, payload = make_query(ledger, amount=amount)
    status, payment = successful_payment(ledger, make_payment_update(payload, charge, amount))
    assert status == "created"
    return payment


def test_duplicate_successful_payment_delivers_once(ledger):
    delivered = []
    payment = _paid(ledger)
    upd = make_payment_update(ledger.get_payment(payment["id"])["payload"])
    # ingest the same update twice (webhook retry)
    for _ in range(2):
        status, _ = successful_payment(ledger, upd)
    assert status == "duplicate"
    assert deliver.run(ledger, payment["id"], delivered.append) == "delivered"
    assert deliver.run(ledger, payment["id"], delivered.append) == "not_claimed"
    assert len(delivered) == 1
    assert delivered[0]["charge_id"] == "ch-1"
    assert delivered[0]["item_id"] == "pro"


def test_crash_mid_delivery_redrives_exactly_once(ledger, clock):
    """SIGKILL during the callback: lease expires, another worker reclaims,
    the goods are delivered exactly once."""
    payment = _paid(ledger)
    # worker A claims and then "crashes" — no release, no complete
    assert deliver.claim_lease(ledger, payment["id"], "worker-A", lease_ttl=300) is True
    assert ledger.get_payment(payment["id"])["status"] == "processing"
    # before the lease expires nobody else may touch it
    clock[0] += 100
    assert deliver.run(ledger, payment["id"], lambda ctx: None, worker_id="worker-B") == "not_claimed"
    # lease expires -> worker B reclaims and delivers
    clock[0] += 300
    delivered = []
    assert deliver.run(ledger, payment["id"], delivered.append, worker_id="worker-B") == "delivered"
    assert len(delivered) == 1
    assert ledger.get_payment(payment["id"])["status"] == "delivered"


def test_two_workers_compete_exactly_one_wins(ledger):
    payment = _paid(ledger)
    delivered = []
    lock = threading.Lock()

    def slow_callback(ctx):
        import time as _t
        _t.sleep(0.2)
        with lock:
            delivered.append(ctx["charge_id"])

    results = {}
    def worker(name):
        results[name] = deliver.run(ledger, payment["id"], slow_callback, worker_id=name)

    t1 = threading.Thread(target=worker, args=("w1",))
    t2 = threading.Thread(target=worker, args=("w2",))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(results.values()) == ["delivered", "not_claimed"]
    assert delivered == ["ch-1"]  # exactly once


def test_callback_exception_releases_lease_for_retry(ledger):
    payment = _paid(ledger)
    calls = []

    def flaky(ctx):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("downstream exploded")

    assert deliver.run(ledger, payment["id"], flaky) == "error_released"
    d = ledger.get_delivery(payment["id"])
    assert d["status"] == "pending" and "downstream exploded" in d["last_error"]
    assert deliver.run(ledger, payment["id"], flaky) == "delivered"
    assert len(calls) == 2


def test_attempts_exhausted_marks_failed(ledger):
    payment = _paid(ledger)

    def always_fails(ctx):
        raise RuntimeError("nope")

    assert deliver.run(ledger, payment["id"], always_fails, max_attempts=1) == "failed"
    assert ledger.get_delivery(payment["id"])["status"] == "failed"
    assert ledger.get_payment(payment["id"])["status"] == "failed"


def test_drain_pending_delivers_rearmed_payments(ledger):
    from tg_stars_ledger import drain_pending
    p1 = _paid(ledger, charge="ch-a")
    p2 = _paid(ledger, charge="ch-b")
    delivered = []
    stats = drain_pending(ledger, delivered.append, worker_id="drainer")
    assert stats == {"delivered": 2, "error_released": 0, "failed": 0,
                     "not_claimed": 0}
    assert sorted(c["charge_id"] for c in delivered) == ["ch-a", "ch-b"]
    # second drain finds nothing pending
    assert drain_pending(ledger, delivered.append)["delivered"] == 0
    assert len(delivered) == 2
    _ = (p1, p2)


def test_async_callback_supported(ledger):
    payment = _paid(ledger)
    seen = []

    async def acb(ctx):
        seen.append(ctx["charge_id"])

    assert deliver.run(ledger, payment["id"], acb) == "delivered"
    assert seen == ["ch-1"]


def test_async_callback_in_running_loop_uses_arun(ledger):
    import asyncio

    from tg_stars_ledger.deliver import AsyncInSyncLoopError
    payment = _paid(ledger)

    async def main():
        async def acb(ctx):
            return ctx["charge_id"]
        # sync run() must refuse inside a running loop and point at arun()
        with pytest.raises(AsyncInSyncLoopError):
            deliver.run(ledger, payment["id"], acb)
        # lease is still held by the same worker, so arun() renews and delivers
        assert await deliver.arun(ledger, payment["id"], acb) == "delivered"

    asyncio.run(main())
    assert ledger.get_payment(payment["id"])["status"] == "delivered"
