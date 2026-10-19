"""Delivery runner with lease-based exactly-once claiming.

The user-supplied callback runs only while this worker holds the lease.
Callback exceptions release the lease WITHOUT marking delivered, so the
payment is retried; a crash (SIGKILL mid-delivery — the postmortem's killer
bug) simply lets the lease expire and another worker reclaims it.

Honest semantics: claiming is exactly-once; the callback itself is
at-least-once. If the callback has non-idempotent side effects AND crashes
after performing them but before returning, a re-drive will repeat them.
Keep delivery callbacks idempotent (keyed on charge_id) — the context dict
hands you everything you need for that.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Callable, Dict, Optional

from .ledger import Ledger

log = logging.getLogger("tg_stars_ledger")

DeliveryCallback = Callable[[Dict[str, Any]], Any]


class AsyncInSyncLoopError(RuntimeError):
    """An async delivery callback was used with run() inside a running loop.

    Use deliver.arun() instead. Raised as-is (not counted as a delivery
    failure) because it is a programming error, not a delivery error.
    """


def claim_lease(
    ledger: Ledger,
    payment_id: int,
    worker_id: str,
    lease_ttl: int = 300,
    now: Optional[int] = None,
) -> bool:
    """Try to claim the delivery lease. True = this worker owns it now."""
    return ledger.claim_delivery(payment_id, worker_id, lease_ttl, now=now)


def build_context(ledger: Ledger, payment_id: int) -> Dict[str, Any]:
    payment = ledger.get_payment(payment_id)
    if payment is None:
        raise KeyError(f"no payment {payment_id}")
    delivery = ledger.get_delivery(payment_id)
    claims = ledger.verify_payload(payment["payload"]) or {}
    return {
        "payment_id": payment["id"],
        "charge_id": payment["charge_id"],
        "user_id": payment["user_id"],
        "amount": payment["amount"],
        "currency": payment["currency"],
        "item_id": claims.get("i", ""),
        "customer_ref": claims.get("r", ""),
        "invoice_payload": payment["payload"],
        "attempt": (delivery["attempts"] if delivery else 0),
    }


def _run_callback(callback: DeliveryCallback, ctx: Dict[str, Any]) -> Any:
    """Run a sync or async callback from synchronous code."""
    result = callback(ctx)
    if inspect.isawaitable(result):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(result)
        if inspect.iscoroutine(result):
            result.close()  # never awaited; avoid "coroutine was never awaited"
        raise AsyncInSyncLoopError(
            "async delivery callback called from a running event loop: "
            "use tg_stars_ledger.deliver.arun instead"
        )
    return result


def run(
    ledger: Ledger,
    payment_id: int,
    callback: DeliveryCallback,
    worker_id: str = "worker",
    lease_ttl: int = 300,
    max_attempts: int = 10,
    now: Optional[int] = None,
) -> str:
    """Deliver one payment. Returns 'delivered' | 'error_released' |
    'failed' | 'not_claimed'."""
    if not claim_lease(ledger, payment_id, worker_id, lease_ttl, now=now):
        return "not_claimed"
    ctx = build_context(ledger, payment_id)
    try:
        _run_callback(callback, ctx)
    except AsyncInSyncLoopError:
        raise  # programming error, not a delivery failure; lease stays held
    except Exception as exc:
        log.exception("delivery callback failed for payment %s", payment_id)
        if ctx["attempt"] >= max_attempts:
            ledger.fail_delivery(payment_id, f"{type(exc).__name__}: {exc}", now=now)
            return "failed"
        ledger.release_delivery(
            payment_id, f"{type(exc).__name__}: {exc}", now=now
        )
        return "error_released"
    ledger.complete_delivery(payment_id, now=now)
    return "delivered"


def drain_pending(
    ledger: Ledger,
    callback: DeliveryCallback,
    worker_id: str = "worker",
    lease_ttl: int = 300,
    max_attempts: int = 10,
    limit: int = 100,
    now: Optional[int] = None,
) -> Dict[str, int]:
    """The delivery worker loop in one call: claim and run every pending
    delivery. Run this on a timer (or in a thread) so payments the
    reconciler re-arms — after a crash or a resurrected lost update —
    actually get delivered without any new Telegram update arriving."""
    stats = {"delivered": 0, "error_released": 0, "failed": 0, "not_claimed": 0}
    for payment_id in ledger.pending_delivery_ids(limit=limit):
        outcome = run(
            ledger, payment_id, callback, worker_id=worker_id,
            lease_ttl=lease_ttl, max_attempts=max_attempts, now=now,
        )
        stats[outcome] = stats.get(outcome, 0) + 1
    return stats


async def arun(
    ledger: Ledger,
    payment_id: int,
    callback: DeliveryCallback,
    worker_id: str = "worker",
    lease_ttl: int = 300,
    max_attempts: int = 10,
    now: Optional[int] = None,
) -> str:
    """Async variant of run(): awaits async callbacks natively."""
    if not claim_lease(ledger, payment_id, worker_id, lease_ttl, now=now):
        return "not_claimed"
    ctx = build_context(ledger, payment_id)
    try:
        result = callback(ctx)
        if inspect.isawaitable(result):
            await result
    except Exception as exc:
        log.exception("delivery callback failed for payment %s", payment_id)
        if ctx["attempt"] >= max_attempts:
            ledger.fail_delivery(payment_id, f"{type(exc).__name__}: {exc}", now=now)
            return "failed"
        ledger.release_delivery(
            payment_id, f"{type(exc).__name__}: {exc}", now=now
        )
        return "error_released"
    ledger.complete_delivery(payment_id, now=now)
    return "delivered"
