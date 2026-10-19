"""tg_stars_ledger: crash-safe Telegram Stars payment ledger.

Framework-agnostic building blocks for accepting Telegram Stars (currency XTR)
without losing payments to duplicate updates, crashed workers, or expired
leases. All state lives in SQLite; a fake Bot API transport makes the whole
thing testable offline.
"""

from .ledger import Ledger
from .ingest import pre_checkout, successful_payment
from .deliver import (
    run as deliver_run,
    arun as deliver_arun,
    claim_lease,
    drain_pending,
    AsyncInSyncLoopError,
)
from .outbox import enqueue_refund, enqueue_alert, process_due
from .reconcile import Reconciler
from .transport import BotTransport, RealBotTransport, FakeBotTransport, ApiError

__all__ = [
    "Ledger",
    "pre_checkout",
    "successful_payment",
    "deliver_run",
    "deliver_arun",
    "claim_lease",
    "drain_pending",
    "AsyncInSyncLoopError",
    "enqueue_refund",
    "enqueue_alert",
    "process_due",
    "Reconciler",
    "BotTransport",
    "RealBotTransport",
    "FakeBotTransport",
    "ApiError",
]

__version__ = "0.1.0"
