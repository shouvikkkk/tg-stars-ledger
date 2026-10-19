"""Thin adapters: aiogram, python-telegram-bot, raw webhooks.

Each is a few lines that translate framework updates into the
framework-agnostic ingest/deliver calls. Framework imports happen inside
the register functions, so importing this module never requires aiogram
or python-telegram-bot to be installed.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from . import deliver as _deliver
from .ingest import pre_checkout, successful_payment
from .ledger import Ledger
from .transport import BotTransport

DeliveryCallback = Callable[[Dict[str, Any]], Any]


def register_aiogram(
    dp,
    ledger: Ledger,
    on_deliver: DeliveryCallback,
    worker_id: str = "aiogram",
    lease_ttl: int = 300,
) -> None:
    """Register pre-checkout + successful_payment handlers on an aiogram 3 Dispatcher."""
    try:
        from aiogram import F, Router
    except ImportError as exc:
        raise ImportError("register_aiogram needs aiogram>=3 installed") from exc

    router = Router(name="tg-stars-ledger")

    @router.pre_checkout_query()
    async def _pre_checkout(query) -> None:
        ok, error = pre_checkout(ledger, query.model_dump())
        await query.answer(ok=ok, error_message=error)

    @router.message(F.successful_payment)
    async def _successful_payment(message) -> None:
        status, payment = successful_payment(
            ledger, {"message": message.model_dump()}
        )
        if status == "created" and payment is not None:
            await _deliver.arun(
                ledger, payment["id"], on_deliver,
                worker_id=worker_id, lease_ttl=lease_ttl,
            )

    dp.include_router(router)


def register_ptb(
    application,
    ledger: Ledger,
    on_deliver: DeliveryCallback,
    worker_id: str = "ptb",
    lease_ttl: int = 300,
) -> None:
    """Register handlers on a python-telegram-bot v20+ Application."""
    try:
        from telegram.ext import MessageHandler, PreCheckoutQueryHandler, filters
    except ImportError as exc:
        raise ImportError(
            "register_ptb needs python-telegram-bot>=20 installed"
        ) from exc

    async def _pre_checkout(update, context) -> None:
        ok, error = pre_checkout(ledger, update.pre_checkout_query.to_dict())
        await update.pre_checkout_query.answer(ok=ok, error_message=error)

    async def _successful_payment(update, context) -> None:
        status, payment = successful_payment(ledger, update.to_dict())
        if status == "created" and payment is not None:
            await _deliver.arun(
                ledger, payment["id"], on_deliver,
                worker_id=worker_id, lease_ttl=lease_ttl,
            )

    application.add_handler(PreCheckoutQueryHandler(_pre_checkout))
    application.add_handler(
        MessageHandler(filters.SUCCESSFUL_PAYMENT, _successful_payment)
    )


def handle_raw_webhook(
    update: Dict[str, Any],
    ledger: Ledger,
    on_deliver: DeliveryCallback,
    transport: Optional[BotTransport] = None,
    worker_id: str = "webhook",
    lease_ttl: int = 300,
) -> Dict[str, Any]:
    """Handle one raw update dict (parsed webhook body). Framework-free.

    Returns what the caller must do next: for a pre_checkout_query, the
    answer payload to POST to answerPreCheckoutQuery (or pass a transport
    and it is sent immediately); for a payment, the ingest outcome.
    """
    if "pre_checkout_query" in update:
        query = update["pre_checkout_query"]
        ok, error = pre_checkout(ledger, query)
        answer = {
            "pre_checkout_query_id": query.get("id"),
            "ok": ok,
            "error_message": error,
        }
        if transport is not None:
            transport.answer_pre_checkout_query(query.get("id"), ok, error)
            return {"answered": True, "ok": ok}
        return {"answer_pre_checkout_query": answer}

    message = update.get("message") or {}
    if "successful_payment" in message:
        status, payment = successful_payment(ledger, update)
        if status == "created" and payment is not None:
            outcome = _deliver.run(
                ledger, payment["id"], on_deliver,
                worker_id=worker_id, lease_ttl=lease_ttl,
            )
            return {"payment": status, "delivery": outcome,
                    "charge_id": payment["charge_id"]}
        charge = (payment or {}).get("charge_id")
        return {"payment": status, "charge_id": charge}

    return {"ignored": True}
