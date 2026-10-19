"""Shared fixtures: in-memory ledger on a mutable fake clock + fake transport."""

import pytest

from tg_stars_ledger import FakeBotTransport, Ledger


@pytest.fixture()
def clock():
    return [1_000_000]


@pytest.fixture()
def ledger(clock):
    led = Ledger(
        bot_id="testbot",
        secret="test-secret-123",
        path=":memory:",
        now_fn=lambda: clock[0],
    )
    yield led
    led.close()


@pytest.fixture()
def transport():
    return FakeBotTransport()


def make_query(ledger, item="pro", amount=100, ref="u1", ttl=3600, qid="q-1"):
    payload = ledger.create_invoice(item, amount, customer_ref=ref, ttl=ttl)
    return {
        "id": qid,
        "from": {"id": 42},
        "currency": "XTR",
        "total_amount": amount,
        "invoice_payload": payload,
        "order_info": {},
    }, payload


def make_payment_update(payload, charge_id="ch-1", amount=100, user_id=42):
    return {
        "update_id": 7,
        "message": {
            "message_id": 11,
            "from": {"id": user_id},
            "chat": {"id": user_id, "type": "private"},
            "successful_payment": {
                "currency": "XTR",
                "total_amount": amount,
                "invoice_payload": payload,
                "telegram_payment_charge_id": charge_id,
                "provider_payment_charge_id": "",
            },
        },
    }
