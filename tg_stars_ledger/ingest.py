"""Ingest paths: pre_checkout_query and successful_payment updates.

Framework-agnostic: each takes the raw update dict (as Telegram sends it —
aiogram's ``model_dump()``, PTB's ``to_dict()``, or a parsed webhook body)
and returns a plain decision. Idempotency keys on every path:

* pre-checkout: the query ``id`` is recorded once; a retried update for the
  same query is answered identically without re-running anything.
* successful_payment: ``telegram_payment_charge_id`` has a UNIQUE index;
  duplicate deliveries collapse into one payment row, never a double credit.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Tuple

from .ledger import Ledger

log = logging.getLogger("tg_stars_ledger")


def pre_checkout(
    ledger: Ledger, query: Dict, now: Optional[int] = None
) -> Tuple[bool, Optional[str]]:
    """Validate a PreCheckoutQuery. Returns (ok, error_message).

    Runs only O(1) local checks — signature, expiry, amount, currency,
    replay — so the adapter can answer inside Telegram's 10-second
    deadline. No user database round-trips in this path.
    """
    ts = ledger.now() if now is None else now
    qid = query.get("id")
    if not qid:
        return False, "Missing pre-checkout query id."
    payload = query.get("invoice_payload")
    if not payload:
        return False, "Missing invoice payload."

    invoice = ledger.get_invoice(payload)
    if invoice is None:
        log.warning("pre-checkout for unknown invoice payload")
        return False, "This invoice is no longer valid."

    claims = ledger.verify_payload(payload)
    if claims is None:
        log.warning("pre-checkout with bad signature (query %s)", qid)
        return False, "Invalid invoice signature."

    if claims.get("e", 0) < ts or invoice["status"] == "expired":
        return False, "This invoice has expired."

    if query.get("currency") != "XTR":
        return False, "Unsupported currency."

    if query.get("total_amount") != invoice["amount"]:
        log.warning(
            "pre-checkout amount mismatch: query=%s invoice=%s",
            query.get("total_amount"),
            invoice["amount"],
        )
        return False, "Amount mismatch."

    # Idempotency: a retried update for the same query id is a no-op that
    # answers the same way.
    first_seen = ledger.mark_query_seen(qid, payload, now=ts)
    if first_seen and invoice["status"] == "pending":
        ledger.set_invoice_status(payload, "prechecked")
    return True, None


def successful_payment(
    ledger: Ledger, update: Dict, now: Optional[int] = None
) -> Tuple[str, Optional[Dict]]:
    """Ingest a successful_payment update.

    Returns (status, payment_row) where status is one of:
      "created"         — new payment recorded, delivery pipeline armed
      "duplicate"       — same telegram_payment_charge_id seen before; no-op
      "invalid"         — not a payment update, wrong currency, or tampered
                          payload; nothing credited
    """
    ts = ledger.now() if now is None else now
    message = update.get("message") or {}
    sp = message.get("successful_payment")
    if not isinstance(sp, dict):
        return "invalid", None

    charge_id = sp.get("telegram_payment_charge_id")
    if not charge_id:
        log.warning("successful_payment without telegram_payment_charge_id")
        return "invalid", None

    if sp.get("currency") != "XTR":
        log.warning("successful_payment with non-XTR currency: %r", sp.get("currency"))
        return "invalid", None

    payload = sp.get("invoice_payload") or ""
    user = message.get("from") or {}
    user_id = user.get("id")

    created, row = ledger.insert_payment(
        charge_id=charge_id,
        payload=payload,
        user_id=user_id if isinstance(user_id, int) else -1,
        amount=int(sp.get("total_amount") or 0),
        currency="XTR",
        now=ts,
    )
    if not created:
        # Duplicate update (webhook retry / bot restart): collapse silently.
        return "duplicate", dict(row)

    # Sanity-check the payment against our own signed invoice. The money has
    # moved at this point; a mismatch is an admin-alert situation, not a
    # silent drop.
    invoice = ledger.get_invoice(payload)
    claims = ledger.verify_payload(payload)
    if invoice is None or claims is None:
        ledger.set_payment_status(
            row["id"], "failed", note="no matching signed invoice", now=ts
        )
        log.error("payment %s has no matching signed invoice", charge_id)
        return "invalid", dict(ledger.get_payment(row["id"]))
    if int(sp.get("total_amount") or 0) != invoice["amount"]:
        ledger.set_payment_status(
            row["id"],
            "failed",
            note=f"amount mismatch: paid {sp.get('total_amount')} != invoiced {invoice['amount']}",
            now=ts,
        )
        log.error("payment %s amount mismatch", charge_id)
        return "invalid", dict(ledger.get_payment(row["id"]))

    ledger.set_invoice_status(payload, "paid", charge_id=charge_id)
    return "created", dict(ledger.get_payment(row["id"]))
