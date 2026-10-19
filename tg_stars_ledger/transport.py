"""Bot API transport layer.

Every call this library makes to Telegram goes through a ``BotTransport``.
``RealBotTransport`` speaks HTTPS to api.telegram.org; ``FakeBotTransport``
lets the ledger, ingest, delivery, outbox and reconciler be tested fully
offline — the same pattern the aiogram-stars postmortem used to reproduce
its payment-loss bugs.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict, List, Optional, Protocol


class ApiError(Exception):
    """A non-ok response (or transport failure) from the Bot API."""

    def __init__(self, description: str, code: Optional[int] = None) -> None:
        super().__init__(description)
        self.description = description
        self.code = code

    def is_already_refunded(self) -> bool:
        return "CHARGE_ALREADY_REFUNDED" in self.description


class BotTransport(Protocol):
    """Minimal Bot API surface the library needs."""

    def answer_pre_checkout_query(
        self, query_id: str, ok: bool, error_message: Optional[str] = None
    ) -> None: ...

    def refund_star_payment(self, user_id: int, telegram_payment_charge_id: str) -> bool: ...

    def get_star_transactions(self) -> List[Dict[str, Any]]: ...

    def send_message(self, chat_id: int, text: str) -> None: ...


class RealBotTransport:
    """HTTPS transport for api.telegram.org. Needs a real bot token."""

    def __init__(self, token: str, timeout: float = 15.0) -> None:
        if not token or ":" not in token:
            raise ValueError("RealBotTransport needs a real bot token")
        self._base = f"https://api.telegram.org/bot{token}"
        self._timeout = timeout

    def _call(self, method: str, payload: Dict[str, Any]) -> Any:
        req = urllib.request.Request(
            f"{self._base}/{method}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = json.loads(resp.read().decode())
        except Exception as exc:  # network failure, DNS, TLS, timeout
            raise ApiError(f"transport failure calling {method}: {exc}") from exc
        if not body.get("ok"):
            desc = body.get("description", "unknown error")
            raise ApiError(desc, body.get("error_code"))
        return body.get("result")

    def answer_pre_checkout_query(
        self, query_id: str, ok: bool, error_message: Optional[str] = None
    ) -> None:
        payload: Dict[str, Any] = {"pre_checkout_query_id": query_id, "ok": ok}
        if error_message:
            payload["error_message"] = error_message
        self._call("answerPreCheckoutQuery", payload)

    def refund_star_payment(self, user_id: int, telegram_payment_charge_id: str) -> bool:
        return bool(
            self._call(
                "refundStarPayment",
                {
                    "user_id": user_id,
                    "telegram_payment_charge_id": telegram_payment_charge_id,
                },
            )
        )

    def get_star_transactions(self) -> List[Dict[str, Any]]:
        result = self._call("getStarTransactions", {})
        return result.get("transactions", []) if isinstance(result, dict) else []

    def send_message(self, chat_id: int, text: str) -> None:
        self._call("sendMessage", {"chat_id": chat_id, "text": text})


class FakeBotTransport:
    """In-memory Bot API double for tests and local development.

    Record every call in ``calls``; script failures with ``fail_next`` /
    ``fail_always``; seed settlement truth with ``add_transaction``.
    """

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.transactions: List[Dict[str, Any]] = []
        self.fail_next: List[Exception] = []
        self.fail_always: Optional[Exception] = None

    # -- scripting helpers -------------------------------------------------
    def add_transaction(
        self,
        txn_id: str,
        amount: int,
        invoice_payload: Optional[str] = None,
        date: int = 0,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Seed one incoming StarTransaction, as getStarTransactions returns it.

        For a successful incoming payment the transaction ``id`` coincides
        with the ``telegram_payment_charge_id`` from ``successful_payment``;
        the source partner carries the bot-specified ``invoice_payload``.
        """
        source: Dict[str, Any] = {"type": "transaction_partner_user"}
        if user_id is not None:
            source["user"] = {"id": user_id}
        if invoice_payload is not None:
            source["invoice_payload"] = invoice_payload
        txn = {
            "id": txn_id,
            "amount": amount,
            "date": date,
            "source": source,
            "receiver": {"type": "transaction_partner_telegram_api", "request_count": 1},
        }
        self.transactions.append(txn)
        return txn

    def _maybe_fail(self, method: str) -> None:
        self.calls.append({"method": method})
        if self.fail_always is not None:
            raise self.fail_always
        if self.fail_next:
            raise self.fail_next.pop(0)

    # -- BotTransport -------------------------------------------------------
    def answer_pre_checkout_query(
        self, query_id: str, ok: bool, error_message: Optional[str] = None
    ) -> None:
        self._maybe_fail("answerPreCheckoutQuery")
        self.calls[-1].update(
            {"query_id": query_id, "ok": ok, "error_message": error_message}
        )

    def refund_star_payment(self, user_id: int, telegram_payment_charge_id: str) -> bool:
        self._maybe_fail("refundStarPayment")
        self.calls[-1].update(
            {
                "user_id": user_id,
                "telegram_payment_charge_id": telegram_payment_charge_id,
            }
        )
        return True

    def get_star_transactions(self) -> List[Dict[str, Any]]:
        self._maybe_fail("getStarTransactions")
        return list(self.transactions)

    def send_message(self, chat_id: int, text: str) -> None:
        self._maybe_fail("sendMessage")
        self.calls[-1].update({"chat_id": chat_id, "text": text})
