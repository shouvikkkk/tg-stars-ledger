"""tg-stars-ledger CLI.

Configuration via environment:
  TG_STARS_DB      path to the SQLite ledger (default ~/.tg-stars-ledger/ledger.db)
  TG_STARS_BOT     bot username/id the payloads are bound to (required)
  TG_STARS_SECRET  HMAC secret for invoice payloads (required)
  TG_BOT_TOKEN     Telegram bot token (required for reconcile)
  TG_ADMIN_CHAT    chat id for admin alerts (optional)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from .ledger import Ledger
from .reconcile import Reconciler
from .transport import RealBotTransport

log = logging.getLogger("tg-stars-ledger")


def _db_path() -> str:
    return os.environ.get(
        "TG_STARS_DB", str(Path.home() / ".tg-stars-ledger" / "ledger.db")
    )


def _ledger() -> Ledger:
    bot = os.environ.get("TG_STARS_BOT")
    secret = os.environ.get("TG_STARS_SECRET")
    if not bot or not secret:
        sys.exit("set TG_STARS_BOT and TG_STARS_SECRET first")
    path = _db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return Ledger(bot_id=bot, secret=secret, path=path)


def _reconciler(ledger: Ledger) -> Reconciler:
    token = os.environ.get("TG_BOT_TOKEN")
    if not token:
        sys.exit("set TG_BOT_TOKEN first (reconcile calls getStarTransactions)")
    admin = os.environ.get("TG_ADMIN_CHAT")
    return Reconciler(
        ledger,
        RealBotTransport(token),
        admin_chat_id=int(admin) if admin else None,
    )


def cmd_init_db(_args: argparse.Namespace) -> int:
    ledger = _ledger()
    print(f"ledger ready at {_db_path()}")
    ledger.close()
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    ledger = _ledger()
    rec = _reconciler(ledger)
    if args.loop:
        rec.loop(interval=args.interval)
    else:
        report = rec.run_once()
        print(json.dumps(report, indent=2, default=str))
    ledger.close()
    return 0


def cmd_audit(_args: argparse.Namespace) -> int:
    ledger = _ledger()
    print(json.dumps(ledger.audit(), indent=2, default=str))
    ledger.close()
    return 0


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    parser = argparse.ArgumentParser(
        prog="tg-stars-ledger",
        description="Crash-safe Telegram Stars payment ledger.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create the ledger database").set_defaults(
        func=cmd_init_db
    )

    rec = sub.add_parser("reconcile", help="run the settlement reconciler")
    rec.add_argument("--once", action="store_true",
                     help="single pass and exit (default)")
    rec.add_argument("--loop", action="store_true", help="run forever")
    rec.add_argument("--interval", type=int, default=60,
                     help="seconds between passes in --loop mode")
    rec.set_defaults(func=cmd_reconcile)

    sub.add_parser("audit", help="show ledger counts and stuck rows").set_defaults(
        func=cmd_audit
    )

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
