"""Adapter + CLI tests (frameworks not installed: raw webhook path + guards)."""

import os

import pytest

from tg_stars_ledger import adapters
from tg_stars_ledger.cli import main as cli_main
from tg_stars_ledger.ingest import successful_payment
from .conftest import make_payment_update, make_query


def test_adapters_import_without_frameworks():
    # importing adapters must never require aiogram / ptb
    assert hasattr(adapters, "handle_raw_webhook")
    with pytest.raises(ImportError, match="aiogram"):
        adapters.register_aiogram(None, None, lambda ctx: None)
    with pytest.raises(ImportError, match="python-telegram-bot"):
        adapters.register_ptb(None, None, lambda ctx: None)


def test_raw_webhook_pre_checkout(ledger, transport):
    query, _ = make_query(ledger)
    res = adapters.handle_raw_webhook(
        {"update_id": 1, "pre_checkout_query": query}, ledger,
        lambda ctx: None, transport=transport,
    )
    assert res == {"answered": True, "ok": True}
    call = transport.calls[-1]
    assert call["method"] == "answerPreCheckoutQuery"
    assert call["query_id"] == "q-1" and call["ok"] is True


def test_raw_webhook_pre_checkout_without_transport_returns_answer(ledger):
    query, _ = make_query(ledger, qid="q-2")
    query["total_amount"] = 1  # tamper with the amount
    res = adapters.handle_raw_webhook(
        {"update_id": 2, "pre_checkout_query": query}, ledger, lambda ctx: None
    )
    answer = res["answer_pre_checkout_query"]
    assert answer["ok"] is False and answer["pre_checkout_query_id"] == "q-2"


def test_raw_webhook_successful_payment_delivers(ledger):
    _, payload = make_query(ledger)
    delivered = []
    res = adapters.handle_raw_webhook(
        make_payment_update(payload), ledger, delivered.append
    )
    assert res["payment"] == "created" and res["delivery"] == "delivered"
    assert delivered[0]["charge_id"] == "ch-1"


def test_raw_webhook_duplicate_payment_no_double_delivery(ledger):
    _, payload = make_query(ledger)
    delivered = []
    upd = make_payment_update(payload)
    r1 = adapters.handle_raw_webhook(upd, ledger, delivered.append)
    r2 = adapters.handle_raw_webhook(upd, ledger, delivered.append)
    assert r1["payment"] == "created" and r2["payment"] == "duplicate"
    assert len(delivered) == 1


def test_raw_webhook_ignores_other_updates(ledger):
    assert adapters.handle_raw_webhook(
        {"update_id": 3, "message": {"text": "hi"}}, ledger, lambda ctx: None
    ) == {"ignored": True}


def test_cli_init_db_and_audit(tmp_path, monkeypatch):
    db = str(tmp_path / "ledger.db")
    monkeypatch.setenv("TG_STARS_DB", db)
    monkeypatch.setenv("TG_STARS_BOT", "testbot")
    monkeypatch.setenv("TG_STARS_SECRET", "s3cret")
    assert cli_main(["init-db"]) == 0
    assert os.path.exists(db)
    assert cli_main(["audit"]) == 0


def test_cli_reconcile_needs_token(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_STARS_DB", str(tmp_path / "l.db"))
    monkeypatch.setenv("TG_STARS_BOT", "testbot")
    monkeypatch.setenv("TG_STARS_SECRET", "s3cret")
    monkeypatch.delenv("TG_BOT_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        cli_main(["reconcile", "--once"])
