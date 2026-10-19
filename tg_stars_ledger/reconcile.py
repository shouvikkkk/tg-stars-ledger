"""Automatic reconciler daemon.

Runs on a loop (or --once) and heals the three stuck states, no human SQL
required:

1. CRASHED DELIVERIES — payments stuck in 'processing' with an expired
   lease (SIGKILL mid-delivery, the postmortem's killer bug). Settlement is
   verified against getStarTransactions; settled payments are re-driven,
   unconfirmed ones are flagged for a human.
2. LOST successful_payment UPDATES — invoices that expired unpaid but whose
   money actually moved (Telegram charged the buyer, the update never
   reached us). Matched via the source partner's invoice_payload in
   getStarTransactions; the payment row is rebuilt idempotently and
   delivery is driven. This fixes the "money taken, goods never delivered,
   no record" case.
3. UNFULFILLABLE DELIVERIES — retries exhausted. Enqueues a refund through
   the durable outbox (refundStarPayment with retry budget + dead letter).

Settlement matching uses two keys, both from the official Bot API docs:
  * StarTransaction.id coincides with telegram_payment_charge_id for
    successful incoming payments — the primary key.
  * TransactionPartnerUser.invoice_payload carries our signed payload —
    the fallback key (also covers payments whose charge id we never saw).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Set

from . import outbox
from .ledger import Ledger
from .transport import BotTransport

log = logging.getLogger("tg_stars_ledger")


def _settlement_index(
    transactions: List[Dict[str, Any]],
) -> Dict[str, Set[str]]:
    """Index transactions by charge id and by source invoice_payload."""
    by_charge: Set[str] = set()
    by_payload: Set[str] = set()
    for txn in transactions:
        tid = txn.get("id")
        if tid:
            by_charge.add(str(tid))
        source = txn.get("source") or {}
        # Only incoming (positive-amount) transactions settle a payment.
        try:
            amount = int(txn.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0
        if amount > 0:
            payload = source.get("invoice_payload")
            if payload:
                by_payload.add(str(payload))
    return {"by_charge": by_charge, "by_payload": by_payload}


class Reconciler:
    def __init__(
        self,
        ledger: Ledger,
        transport: BotTransport,
        admin_chat_id: Optional[int] = None,
        lease_ttl: int = 300,
        invoice_grace: int = 1800,
        max_delivery_attempts: int = 10,
    ) -> None:
        self.ledger = ledger
        self.transport = transport
        self.admin_chat_id = admin_chat_id
        self.lease_ttl = lease_ttl
        self.invoice_grace = invoice_grace
        self.max_delivery_attempts = max_delivery_attempts
        if admin_chat_id is not None:
            ledger.set_setting("admin_chat_id", str(admin_chat_id))

    # -- public -------------------------------------------------------------
    def run_once(self, now: Optional[int] = None) -> Dict[str, Any]:
        """One reconciliation pass. Returns a report dict of what happened."""
        ts = self.ledger.now() if now is None else now
        report: Dict[str, Any] = {
            "redriven": 0,
            "resurrected": 0,
            "refunds_queued": 0,
            "invoices_expired": 0,
            "flagged": 0,
            "outbox": {},
            "errors": [],
        }
        try:
            transactions = self.transport.get_star_transactions()
        except Exception as exc:
            report["errors"].append(f"getStarTransactions failed: {exc}")
            log.error("reconciler: getStarTransactions failed: %s", exc)
            return report
        settled = _settlement_index(transactions)

        # 1. Crashed deliveries: lease expired while 'processing'.
        for row in self.ledger.stuck_payments(now=ts):
            charge = row["charge_id"]
            payload = row["payload"]
            if charge in settled["by_charge"] or payload in settled["by_payload"]:
                # Money moved; re-arm the delivery so the next worker picks it up.
                self.ledger.release_delivery(row["id"], error=None, now=ts)
                report["redriven"] += 1
                log.info("reconciler: re-driving stuck payment %s", charge)
            else:
                # 'processing' means a successful_payment arrived, so money
                # almost certainly moved — but we cannot prove it from the
                # transaction window (pagination/API gap). Flag, don't guess.
                self.ledger.set_payment_status(
                    row["id"], "failed",
                    note="stuck with expired lease; not found in getStarTransactions window",
                    now=ts,
                )
                self._alert(
                    f"Payment {charge} was stuck mid-delivery and is NOT in the "
                    f"Star transaction window. Marked failed — check manually.",
                    ts,
                )
                report["flagged"] += 1

        # 2. Lost successful_payment updates: expired invoices that settled.
        for inv in self.ledger.expired_unpaid_invoices(
            grace=self.invoice_grace, now=ts
        ):
            payload = inv["payload"]
            if payload in settled["by_payload"]:
                matched = [
                    t for t in transactions
                    if (t.get("source") or {}).get("invoice_payload") == payload
                ]
                txn = matched[0]
                created, payment = self.ledger.insert_payment(
                    charge_id=str(txn["id"]),
                    payload=payload,
                    user_id=inv["user_id"] if inv["user_id"] is not None else -1,
                    amount=inv["amount"],
                    now=ts,
                )
                self.ledger.set_invoice_status(
                    payload, "paid", charge_id=str(txn["id"])
                )
                if created:
                    report["resurrected"] += 1
                    log.info(
                        "reconciler: resurrected lost payment for invoice %s "
                        "(charge %s)",
                        payload[:24], txn["id"],
                    )
            else:
                self.ledger.set_invoice_status(payload, "expired")
                report["invoices_expired"] += 1

        # 3. Unfulfillable deliveries -> durable refund.
        for row in self.ledger.unfulfillable_payments():
            reason = (
                f"delivery failed after {row['delivery_attempts']} attempts: "
                f"{row['last_error']}"
            )
            if outbox.enqueue_refund(
                self.ledger, row["id"], row["charge_id"], row["user_id"],
                reason=reason, now=ts,
            ):
                self.ledger.set_payment_status(row["id"], "refund_queued", now=ts)
                report["refunds_queued"] += 1
                log.info("reconciler: queued refund for charge %s", row["charge_id"])

        # 4. Drain the outbox (honors the DB-shared flood pause).
        report["outbox"] = outbox.process_due(self.ledger, self.transport, now=ts)
        return report

    def loop(self, interval: int = 60) -> None:
        """Run forever. Ctrl-C stops cleanly."""
        log.info("reconciler loop started (interval=%ds)", interval)
        try:
            while True:
                report = self.run_once()
                interesting = {k: v for k, v in report.items()
                               if v and k not in ("outbox", "errors")}
                if interesting or report["errors"]:
                    log.info("reconciler pass: %s", report)
                time.sleep(interval)
        except KeyboardInterrupt:
            log.info("reconciler loop stopped")

    # -- internals -------------------------------------------------------------
    def _alert(self, text: str, now: int) -> None:
        if self.admin_chat_id is not None:
            outbox.enqueue_alert(self.ledger, self.admin_chat_id, text, now=now)
        else:
            log.warning("no admin_chat_id; alert dropped: %s", text)
