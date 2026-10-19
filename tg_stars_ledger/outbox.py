"""Durable outbox for refunds and admin alerts.

Writes go to the DB first; a sender drains them with a retry budget and
exponential backoff. After the budget is exhausted the item lands in the
dead-letter state ('dead') instead of being retried forever — a human can
see it in `audit` and re-queue or handle it manually.

CHARGE_ALREADY_REFUNDED is treated as success: refunding is idempotent, so
a retry that lands after the charge was already refunded must not loop.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from .ledger import Ledger
from .transport import ApiError, BotTransport

log = logging.getLogger("tg_stars_ledger")

BACKOFF_BASE_SECONDS = 60
BACKOFF_CAP_SECONDS = 3600


def _backoff(attempts: int) -> int:
    return min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** max(0, attempts - 1)))


def enqueue_refund(
    ledger: Ledger,
    payment_id: int,
    charge_id: str,
    user_id: int,
    reason: str = "",
    max_attempts: int = 8,
    now: Optional[int] = None,
) -> bool:
    """Queue a refundStarPayment. True = newly queued, False = already queued."""
    ts = ledger.now() if now is None else now
    with ledger._lock, ledger._conn:
        cur = ledger._conn.execute(
            """INSERT OR IGNORE INTO refunds
               (payment_id, charge_id, user_id, reason, max_attempts,
                next_retry_at, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (payment_id, charge_id, user_id, reason, max_attempts, ts, ts),
        )
        return cur.rowcount == 1


def enqueue_alert(
    ledger: Ledger,
    chat_id: int,
    text: str,
    max_attempts: int = 8,
    now: Optional[int] = None,
) -> int:
    ts = ledger.now() if now is None else now
    with ledger._lock, ledger._conn:
        cur = ledger._conn.execute(
            """INSERT INTO alerts
               (chat_id, text, max_attempts, next_retry_at, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (chat_id, text, max_attempts, ts, ts),
        )
        return cur.lastrowid


def requeue_dead(ledger: Ledger, kind: str, item_id: int,
                 now: Optional[int] = None) -> bool:
    """Human escape hatch: put a dead-letter item back in the queue."""
    table = {"refund": "refunds", "alert": "alerts"}.get(kind)
    if table is None:
        raise ValueError("kind must be 'refund' or 'alert'")
    ts = ledger.now() if now is None else now
    with ledger._lock, ledger._conn:
        cur = ledger._conn.execute(
            f"UPDATE {table} SET status='queued', attempts=0, next_retry_at=?,"
            " last_error=NULL WHERE id=? AND status='dead'",
            (ts, item_id),
        )
        return cur.rowcount == 1


def _send_refund(ledger: Ledger, row, transport: BotTransport,
                 now: int) -> bool:
    """Attempt one refund. True = terminal (sent or counted), False = paused."""
    with ledger._lock, ledger._conn:
        ledger._conn.execute(
            "UPDATE refunds SET status='sending' WHERE id=?", (row["id"],)
        )
    try:
        transport.refund_star_payment(row["user_id"], row["charge_id"])
    except ApiError as exc:
        if exc.is_already_refunded():
            log.info("refund %s already refunded; treating as sent", row["id"])
            _terminal(ledger, "refunds", row["id"], "sent", None, now)
            ledger.set_payment_status(row["payment_id"], "refunded", now=now)
            return True
        _count_failure(ledger, "refunds", row, str(exc), now)
        return True
    except Exception as exc:  # network etc.
        _count_failure(ledger, "refunds", row, f"{type(exc).__name__}: {exc}", now)
        return True
    _terminal(ledger, "refunds", row["id"], "sent", None, now)
    ledger.set_payment_status(row["payment_id"], "refunded", now=now)
    log.info("refund sent for charge %s", row["charge_id"])
    return True


def _send_alert(ledger: Ledger, row, transport: BotTransport, now: int) -> bool:
    with ledger._lock, ledger._conn:
        ledger._conn.execute(
            "UPDATE alerts SET status='sending' WHERE id=?", (row["id"],)
        )
    try:
        transport.send_message(row["chat_id"], row["text"])
    except Exception as exc:
        _count_failure(ledger, "alerts", row, f"{type(exc).__name__}: {exc}", now)
        return True
    _terminal(ledger, "alerts", row["id"], "sent", None, now)
    return True


def _terminal(ledger: Ledger, table: str, item_id: int, status: str,
             error: Optional[str], now: int) -> None:
    with ledger._lock, ledger._conn:
        ledger._conn.execute(
            f"UPDATE {table} SET status=?, last_error=? WHERE id=?",
            (status, error, item_id),
        )


def _count_failure(ledger: Ledger, table: str, row, error: str, now: int) -> None:
    attempts = row["attempts"] + 1
    if attempts >= row["max_attempts"]:
        with ledger._lock, ledger._conn:
            ledger._conn.execute(
                f"UPDATE {table} SET status='dead', attempts=?, last_error=?"
                " WHERE id=?",
                (attempts, error, row["id"]),
            )
        log.error("%s %s dead-lettered after %d attempts: %s",
                  table, row["id"], attempts, error)
    else:
        with ledger._lock, ledger._conn:
            ledger._conn.execute(
                f"UPDATE {table} SET status='queued', attempts=?,"
                " next_retry_at=?, last_error=? WHERE id=?",
                (attempts, now + _backoff(attempts), error, row["id"]),
            )
        log.warning("%s %s attempt %d failed, retry in %ds: %s",
                    table, row["id"], attempts, _backoff(attempts), error)


def process_due(
    ledger: Ledger, transport: BotTransport, now: Optional[int] = None
) -> Dict[str, int]:
    """Send every due outbox item. Respects the DB-shared flood pause."""
    ts = ledger.now() if now is None else now
    stats = {"refunds_sent": 0, "alerts_sent": 0, "dead": 0, "paused": 0}
    if ledger.flood_paused(now=ts):
        stats["paused"] = 1
        return stats
    with ledger._lock:
        due_refunds = ledger._conn.execute(
            "SELECT * FROM refunds WHERE status IN ('queued','sending')"
            " AND next_retry_at <= ? ORDER BY next_retry_at",
            (ts,),
        ).fetchall()
        due_alerts = ledger._conn.execute(
            "SELECT * FROM alerts WHERE status IN ('queued','sending')"
            " AND next_retry_at <= ? ORDER BY next_retry_at",
            (ts,),
        ).fetchall()
    for row in due_refunds:
        _send_refund(ledger, row, transport, ts)
        after = ledger._conn.execute(
            "SELECT status FROM refunds WHERE id=?", (row["id"],)
        ).fetchone()["status"]
        if after == "sent":
            stats["refunds_sent"] += 1
        elif after == "dead":
            stats["dead"] += 1
            # A dead refund is money the buyer is owed: page a human once.
            admin = ledger.get_setting("admin_chat_id")
            if admin:
                enqueue_alert(
                    ledger,
                    int(admin),
                    f"DEAD refund #{row['id']} charge {row['charge_id']} "
                    f"({row['attempts']+1} attempts): {row['last_error']}. "
                    "Re-queue with outbox.requeue_dead or refund manually.",
                    max_attempts=3,
                    now=ts,
                )
    for row in due_alerts:
        _send_alert(ledger, row, transport, ts)
        after = ledger._conn.execute(
            "SELECT status FROM alerts WHERE id=?", (row["id"],)
        ).fetchone()["status"]
        if after == "sent":
            stats["alerts_sent"] += 1
        elif after == "dead":
            stats["dead"] += 1
    return stats
