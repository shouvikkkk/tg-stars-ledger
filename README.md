# tg-stars-ledger

A crash-safe Telegram Stars payment ledger with **automatic** stuck-payment reconciliation. Framework-agnostic: works with aiogram, python-telegram-bot, or a raw webhook — with one SQLite file as the single source of truth.

## The problem

Accepting Telegram Stars looks like six easy steps (`sendInvoice` → `pre_checkout_query` → `answerPreCheckoutQuery` → `successful_payment` → deliver). In production, payments get lost in the gaps between those steps. The author of the `aiogram-stars` library documented **nine real payment-loss bugs** found the day before its 0.1.0 release, in [this postmortem](https://dev.to/sinnercode228/the-stars-were-charged-and-onpaid-never-ran-where-telegram-stars-payments-get-lost-in-aiogram-5m2) (2026-10-18):

> "A buyer pays 100 Stars, Telegram takes them, and the bot replies to the payment message with 'Sorry, I don't know this command'. The `on_paid` handler that should hand over the goods never runs, and there is no row for the payment anywhere."

> "As far as Telegram is concerned, the update is handled, and it will not be sent again." — on a deploy SIGKILL landing in the middle of `on_paid`: the Stars are gone, the goods are never delivered, and the payment sits in `processing` forever.

And the duplicates problem, from [glacierphonk's integration guide](https://glacierphonk.com/log/2026-02-25-telegram-stars-payment-integration/):

> "Telegram may deliver the `successful_payment` update more than once. Network issues, webhook retries, bot restarts — duplicates happen."

The `aiogram-stars` author fixed what they could, but left an explicit unsolved list: *"It is still manual work"*, *"the library never calls `stuck_payments()` on its own"* — healing a stuck payment needs a human with SQL, the flood-control pause is per-process instead of shared, and the whole thing only works inside aiogram.

**tg-stars-ledger is the answer to that unsolved list**: a framework-agnostic ledger with an *automatic* reconciler daemon that heals stuck payments by verifying settlement against `getStarTransactions` — no human SQL required.

## Quickstart

```bash
pip install tg-stars-ledger
export TG_STARS_BOT=mybot TG_STARS_SECRET="$(openssl rand -hex 32)"
export TG_BOT_TOKEN=1234:ABC TG_ADMIN_CHAT=987654321
tg-stars-ledger init-db
```

```python
from tg_stars_ledger import Ledger
from tg_stars_ledger import adapters
from tg_stars_ledger.transport import RealBotTransport

ledger = Ledger(bot_id="mybot", secret=os.environ["TG_STARS_SECRET"],
                path="payments.db")

# 1. Mint a signed invoice payload BEFORE calling sendInvoice, so you never
#    accept a payment you can't look up.
payload = ledger.create_invoice("pro-month", 250, customer_ref="user-123")
await bot.send_invoice(chat_id, title="Pro", description="...",
                       payload=payload, currency="XTR",
                       prices=[{"label": "Pro", "amount": 250}])
#    (provider_token must be OMITTED for XTR — Telegram rejects it otherwise.)

# 2. Deliver goods under a crash-safe lease.
def on_deliver(ctx):                      # sync or async
    grant_pro(ctx["user_id"], ctx["item_id"])   # keep it idempotent on charge_id

adapters.register_aiogram(dp, ledger, on_deliver)   # or register_ptb / handle_raw_webhook

# 3. Run the reconciler (systemd unit, second process, whatever you like):
#    tg-stars-ledger reconcile --loop --interval 60
#    And a tiny delivery worker loop so re-armed payments actually go out
#    even when no new Telegram update arrives (e.g. after a crash):
#    import time
#    from tg_stars_ledger import drain_pending
#    while True:
#        drain_pending(ledger, on_deliver, worker_id="worker-1")
#        time.sleep(5)
```

## Architecture

Every payment moves through one state machine, persisted in SQLite (WAL mode):

```
invoice:   pending ──pre_checkout──▶ prechecked ──successful_payment──▶ paid
              │                                                        ▲
              │ (expired + settled in getStarTransactions)              │
              └──────────── reconciler resurrects ─────────────────────┘
              │ (expired + not settled) → expired

payment:   pending ──lease claimed──▶ processing ──callback ok──▶ delivered
              ▲                          │  │
              │        crash/SIGKILL      │  │ callback raised
              │        (lease expires)    │  └─▶ pending (lease released, retried)
              │                          │
              │   reconciler verifies    └─▶ failed (attempts exhausted)
              │   settlement, re-drives       │
              └──────────────────────────┘   └─▶ refund_queued ──outbox──▶ refunded
                                                  (refundStarPayment, retry budget)
```

The hard parts, and how they're solved:

- **Signed invoice payloads** (`ledger.py`): the `invoice_payload` carries an HMAC-SHA256 over bot id, item, amount, customer ref, nonce and expiry. A payload minted elsewhere, or paid for a different amount, is declined *before* any money logic runs. The token stays within Telegram's 128-byte payload limit (enforced at mint time).
- **Idempotent ingest** (`ingest.py`): `pre_checkout()` records each `PreCheckoutQuery.id` once (retried updates answer identically) and runs only O(1) local checks, so adapters answer inside the 10-second `answerPreCheckoutQuery` deadline. `successful_payment()` inserts on the `telegram_payment_charge_id` UNIQUE key — duplicate deliveries collapse into one row, never a double credit — then sanity-checks amount/signature against the signed invoice.
- **Lease-based exactly-once claiming** (`deliver.py`): one atomic conditional `UPDATE` claims a delivery only if it is pending, its lease expired (crashed worker), or the same worker renews. Concurrent workers race on that statement; exactly one wins. Callback exceptions release the lease *without* marking delivered; a crash just lets the lease expire. Claiming is exactly-once; the callback is at-least-once, so keep delivery callbacks idempotent on `charge_id` (the context dict gives you everything for that).
- **Automatic reconciler** (`reconcile.py`): a daemon that heals the three stuck states with zero human SQL:
  1. *Crashed deliveries* — `processing` payments with expired leases are re-driven after settlement is verified against `getStarTransactions` (`StarTransaction.id` coincides with `telegram_payment_charge_id` for incoming payments; `TransactionPartnerUser.invoice_payload` is the fallback key).
  2. *Lost `successful_payment` updates* — expired invoices with no payment row are matched against settled transactions by invoice payload; the payment row is rebuilt idempotently and delivery re-armed. This is the "money taken, goods never delivered, no record" bug, fixed automatically.
  3. *Unfulfillable deliveries* — retries exhausted → refund enqueued through the durable outbox.
- **Durable outbox** (`outbox.py`): refunds and admin alerts are written to the DB first, then sent with exponential backoff (60s → 1h cap) and a retry budget; exhausted items become dead letters (`audit` shows them; `requeue_dead()` revives them). `CHARGE_ALREADY_REFUNDED` is treated as success — refunds are idempotent, never an infinite loop.
- **DB-shared flood pause** (`ledger.set_flood_pause`): one setting in the DB that *every* process honors before touching the Bot API — unlike per-process pauses that other workers ignore.

## Differentiation vs aiogram-stars

| | aiogram-stars (0.1.0) | tg-stars-ledger |
|---|---|---|
| Framework | aiogram only | aiogram, python-telegram-bot, raw webhooks |
| Stuck payments | `stuck_payments()` exists but *"the library never calls it on its own"* — human with SQL | reconciler daemon runs it automatically, verifies settlement via `getStarTransactions`, re-drives or refunds |
| Lost `successful_payment` update | no record anywhere | expired invoice + transaction match → payment row resurrected |
| Flood control | per-process pause | DB-shared pause honored by all processes |
| Delivery guarantee | lease in `INSERT … ON CONFLICT DO NOTHING` | atomic conditional-UPDATE lease claiming, tested under thread contention |
| Refund queue | background task, manual retry | durable outbox: retry budget, backoff, dead letters, idempotent on `CHARGE_ALREADY_REFUNDED` |
| Dependencies | aiogram, storage backend | stdlib only |

## Honest limitations

- **Not verified against real Telegram.** Everything is tested against a `FakeBotTransport` (the same pattern the aiogram-stars postmortem used). The field names (`successful_payment.telegram_payment_charge_id`, `answerPreCheckoutQuery(pre_checkout_query_id, ok, error_message)`, `refundStarPayment(user_id, telegram_payment_charge_id)`, `getStarTransactions` → `StarTransaction{id, amount, source}`) were read from the [official Bot API docs](https://core.telegram.org/bots/payments-stars), but run it against the Telegram **test environment** (free test Stars, identical flow) before touching real money.
- **`getStarTransactions` returns a window, not a query.** The Bot API exposes no charge-id filter, so settlement verification scans the returned transaction list in memory. Very old stuck payments can fall outside the window — those are flagged for a human, never auto-marked settled.
- **At-least-once callbacks.** If your delivery callback performs a non-idempotent side effect and then crashes before returning, the re-drive will repeat it. Key side effects on `charge_id`.
- **aiogram/PTB adapters** are thin and importable without the frameworks installed, but were not executed against real aiogram/PTB (no network here). The raw-webhook path is fully tested.
- **One DB file per bot.** Payloads are bound to `bot_id`; don't share a ledger across bots.

## Usage per adapter

**aiogram 3:**
```python
from tg_stars_ledger import Ledger, adapters
adapters.register_aiogram(dp, ledger, on_deliver)
```

**python-telegram-bot v20+:**
```python
from tg_stars_ledger import adapters
adapters.register_ptb(application, ledger, on_deliver)
```

**Raw webhook / any framework:**
```python
from tg_stars_ledger import adapters
from tg_stars_ledger.transport import RealBotTransport

result = adapters.handle_raw_webhook(update_dict, ledger, on_deliver,
                                     transport=RealBotTransport(token))
# {"answered": True, "ok": ...}                       — pre_checkout_query
# {"payment": "created"|"duplicate", "delivery": ...} — successful_payment
# {"ignored": True}                                   — anything else
```

## CLI

```bash
tg-stars-ledger init-db                 # create the ledger
tg-stars-ledger reconcile --once       # single healing pass (cron-friendly)
tg-stars-ledger reconcile --loop --interval 60
tg-stars-ledger audit                  # counts, stuck rows, dead letters
```

## Development

```bash
pip install pytest
pytest tests/   # 48 tests: duplicates, crash/re-drive, contention,
                # replay attacks, settlement matching, refund dead letters
```

## License

MIT — Copyright (c) 2026 Shouvik Maji
