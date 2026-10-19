"""SQLite payment ledger.

Tables: invoices, used_queries (pre-checkout idempotency), payments,
deliveries (lease-based exactly-once claiming), refunds + alerts (durable
outbox), settings (DB-shared flood pause).

Invoice payloads are HMAC-SHA256 signed so a payload minted anywhere else —
or paid for a different amount — is rejected before any money logic runs.
The payload format keeps the signed token within Telegram's 128-byte limit.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS invoices (
    payload      TEXT PRIMARY KEY,   -- signed invoice_payload (the invoice id)
    bot_id       TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    amount       INTEGER NOT NULL,   -- Stars, integer (XTR has no decimals)
    customer_ref TEXT,
    user_id      INTEGER,
    created_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending|prechecked|paid|failed|expired
    charge_id    TEXT,
    nonce        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS used_queries (
    query_id   TEXT PRIMARY KEY,  -- PreCheckoutQuery.id, seen exactly once
    payload    TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_id TEXT UNIQUE NOT NULL,  -- telegram_payment_charge_id: idempotency key
    payload   TEXT NOT NULL,         -- signed invoice_payload
    user_id   INTEGER NOT NULL,
    amount    INTEGER NOT NULL,
    currency  TEXT NOT NULL DEFAULT 'XTR',
    status    TEXT NOT NULL DEFAULT 'pending',  -- pending|processing|delivered|failed|refund_queued|refunded
    note      TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);
CREATE INDEX IF NOT EXISTS idx_payments_payload ON payments(payload);

CREATE TABLE IF NOT EXISTS deliveries (
    payment_id    INTEGER PRIMARY KEY,  -- one delivery pipeline per payment
    status        TEXT NOT NULL DEFAULT 'pending',  -- pending|claimed|delivered|failed
    lease_owner   TEXT,
    lease_expires INTEGER,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    delivered_at  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);

CREATE TABLE IF NOT EXISTS refunds (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    payment_id    INTEGER NOT NULL UNIQUE,  -- one refund pipeline per payment
    charge_id     TEXT NOT NULL,
    user_id       INTEGER NOT NULL,
    reason        TEXT,
    status        TEXT NOT NULL DEFAULT 'queued',  -- queued|sending|sent|dead
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 8,
    next_retry_at INTEGER NOT NULL,
    last_error    TEXT,
    created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_refunds_due ON refunds(status, next_retry_at);

CREATE TABLE IF NOT EXISTS alerts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id       INTEGER NOT NULL,
    text          TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'queued',  -- queued|sent|dead
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 8,
    next_retry_at INTEGER NOT NULL,
    last_error    TEXT,
    created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_due ON alerts(status, next_retry_at);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# Signature covers the compact body only; 16 hex chars (64-bit tag) keep the
# whole token comfortably inside Telegram's 128-byte invoice payload limit.
_SIG_BYTES = 8
_MAX_PAYLOAD_BYTES = 128


class Ledger:
    """Owns the SQLite connection and every read/write the library performs."""

    def __init__(
        self,
        bot_id: str,
        secret: str,
        path: str = ":memory:",
        now_fn: Optional[Callable[[], float]] = None,
    ) -> None:
        if not bot_id:
            raise ValueError("bot_id is required: payloads are bound to the bot identity")
        if not secret:
            raise ValueError("secret is required: invoice payloads are HMAC-signed")
        self.bot_id = bot_id
        self._secret = secret.encode()
        self._now_fn = now_fn or time.time
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)

    # -- time --------------------------------------------------------------
    def now(self) -> int:
        return int(self._now_fn())

    # -- payload signing ----------------------------------------------------
    def _sign(self, body: str) -> str:
        mac = hmac.new(self._secret, body.encode(), hashlib.sha256).digest()
        return mac[:_SIG_BYTES].hex()

    def sign_payload(
        self,
        item_id: str,
        amount: int,
        customer_ref: str = "",
        ttl: int = 3600,
        nonce: Optional[str] = None,
        now: Optional[int] = None,
    ) -> str:
        """Build an HMAC-signed invoice payload token.

        Claims bind the bot identity, item, amount, customer ref, a nonce
        (uniqueness + replay scope) and an expiry. Anything tampered with —
        or minted for a different bot — fails verification.
        """
        ts = self.now() if now is None else now
        claims = {
            "v": 1,
            "b": self.bot_id,
            "i": item_id,
            "a": int(amount),
            "r": customer_ref,
            "n": nonce or secrets.token_hex(6),
            "e": ts + int(ttl),
        }
        body = json.dumps(claims, separators=(",", ":"))
        token = f"{body}.{self._sign(body)}"
        if len(token.encode()) > _MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"signed payload is {len(token.encode())} bytes, over Telegram's "
                f"{_MAX_PAYLOAD_BYTES}-byte limit: shorten item_id/customer_ref"
            )
        return token

    def verify_payload(self, token: str) -> Optional[Dict[str, Any]]:
        """Return the claims dict if the signature is valid and bound to us."""
        try:
            body, sig = token.rsplit(".", 1)
            claims = json.loads(body)
        except (ValueError, json.JSONDecodeError):
            return None
        if not hmac.compare_digest(self._sign(body), sig):
            return None
        if not isinstance(claims, dict) or claims.get("v") != 1:
            return None
        if claims.get("b") != self.bot_id:  # bound to a different bot
            return None
        return claims

    # -- invoices -------------------------------------------------------------
    def create_invoice(
        self,
        item_id: str,
        amount: int,
        customer_ref: str = "",
        user_id: Optional[int] = None,
        ttl: int = 3600,
        now: Optional[int] = None,
    ) -> str:
        """Mint a signed invoice payload and record it. Returns the payload.

        Record the invoice BEFORE calling sendInvoice so we never accept a
        payment we cannot look up.
        """
        ts = self.now() if now is None else now
        payload = self.sign_payload(
            item_id, amount, customer_ref=customer_ref, ttl=ttl, now=ts
        )
        claims = self.verify_payload(payload)
        assert claims is not None
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT OR IGNORE INTO invoices
                   (payload, bot_id, item_id, amount, customer_ref, user_id,
                    created_at, expires_at, nonce)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload,
                    self.bot_id,
                    item_id,
                    int(amount),
                    customer_ref,
                    user_id,
                    ts,
                    claims["e"],
                    claims["n"],
                ),
            )
        return payload

    def get_invoice(self, payload: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM invoices WHERE payload = ?", (payload,)
            ).fetchone()

    def set_invoice_status(
        self, payload: str, status: str, charge_id: Optional[str] = None
    ) -> None:
        with self._lock, self._conn:
            if charge_id is not None:
                self._conn.execute(
                    "UPDATE invoices SET status=?, charge_id=? WHERE payload=?",
                    (status, charge_id, payload),
                )
            else:
                self._conn.execute(
                    "UPDATE invoices SET status=? WHERE payload=?", (status, payload)
                )

    # -- pre-checkout idempotency ---------------------------------------------
    def mark_query_seen(self, query_id: str, payload: str, now: Optional[int] = None) -> bool:
        """Record a PreCheckoutQuery.id. True = first time seen, False = replay."""
        ts = self.now() if now is None else now
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO used_queries (query_id, payload, created_at)"
                " VALUES (?, ?, ?)",
                (query_id, payload, ts),
            )
            return cur.rowcount == 1

    # -- payments ---------------------------------------------------------------
    def insert_payment(
        self,
        charge_id: str,
        payload: str,
        user_id: int,
        amount: int,
        currency: str = "XTR",
        now: Optional[int] = None,
    ) -> Tuple[bool, sqlite3.Row]:
        """Insert a payment idempotently on telegram_payment_charge_id.

        Returns (created, row): created=False means this is a duplicate
        delivery of the same successful_payment update.
        """
        ts = self.now() if now is None else now
        with self._lock, self._conn:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO payments
                   (charge_id, payload, user_id, amount, currency, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (charge_id, payload, user_id, int(amount), currency, ts, ts),
            )
            row = self._conn.execute(
                "SELECT * FROM payments WHERE charge_id = ?", (charge_id,)
            ).fetchone()
            if cur.rowcount == 1:
                self._conn.execute(
                    "INSERT OR IGNORE INTO deliveries (payment_id) VALUES (?)",
                    (row["id"],),
                )
            return cur.rowcount == 1, row

    def get_payment(self, payment_id: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM payments WHERE id = ?", (payment_id,)
            ).fetchone()

    def get_payment_by_charge(self, charge_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM payments WHERE charge_id = ?", (charge_id,)
            ).fetchone()

    def set_payment_status(
        self, payment_id: int, status: str, note: Optional[str] = None,
        now: Optional[int] = None,
    ) -> None:
        ts = self.now() if now is None else now
        with self._lock, self._conn:
            if note is not None:
                self._conn.execute(
                    "UPDATE payments SET status=?, note=?, updated_at=? WHERE id=?",
                    (status, note, ts, payment_id),
                )
            else:
                self._conn.execute(
                    "UPDATE payments SET status=?, updated_at=? WHERE id=?",
                    (status, ts, payment_id),
                )

    # -- deliveries: lease-based exactly-once claiming --------------------------
    def claim_delivery(
        self,
        payment_id: int,
        worker_id: str,
        lease_ttl: int = 300,
        now: Optional[int] = None,
    ) -> bool:
        """Atomically claim a delivery lease. True = this worker owns it.

        One conditional UPDATE: the row is claimed only if it is pending, or
        a previous claim's lease has expired (crashed worker), or this same
        worker already holds it (lease renewal). Concurrent workers race on
        this statement; exactly one wins.
        """
        ts = self.now() if now is None else now
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE deliveries
                   SET status='claimed', lease_owner=?, lease_expires=?,
                       attempts=attempts+1, last_error=NULL
                   WHERE payment_id=?
                     AND (status='pending'
                          OR (status='claimed' AND lease_expires IS NOT NULL
                              AND lease_expires <= ?)
                          OR (status='claimed' AND lease_owner=?))""",
                (worker_id, ts + lease_ttl, payment_id, ts, worker_id),
            )
            if cur.rowcount == 1:
                self._conn.execute(
                    "UPDATE payments SET status='processing', updated_at=? WHERE id=?",
                    (ts, payment_id),
                )
                return True
            return False

    def release_delivery(
        self,
        payment_id: int,
        error: Optional[str] = None,
        now: Optional[int] = None,
    ) -> None:
        """Release a claimed lease back to pending (callback raised)."""
        ts = self.now() if now is None else now
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE deliveries
                   SET status='pending', lease_owner=NULL, lease_expires=NULL,
                       last_error=?
                   WHERE payment_id=? AND status='claimed'""",
                (error, payment_id),
            )
            self._conn.execute(
                "UPDATE payments SET status='pending', updated_at=? WHERE id=?",
                (ts, payment_id),
            )

    def complete_delivery(
        self, payment_id: int, now: Optional[int] = None
    ) -> None:
        ts = self.now() if now is None else now
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE deliveries
                   SET status='delivered', lease_owner=NULL, lease_expires=NULL,
                       delivered_at=?
                   WHERE payment_id=?""",
                (ts, payment_id),
            )
            self._conn.execute(
                "UPDATE payments SET status='delivered', updated_at=? WHERE id=?",
                (ts, payment_id),
            )

    def fail_delivery(
        self, payment_id: int, error: str, now: Optional[int] = None
    ) -> None:
        """Attempts exhausted: the delivery is unfulfillable, refund policy applies."""
        ts = self.now() if now is None else now
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE deliveries
                   SET status='failed', lease_owner=NULL, lease_expires=NULL,
                       last_error=?
                   WHERE payment_id=?""",
                (error, payment_id),
            )
            self._conn.execute(
                "UPDATE payments SET status='failed', updated_at=? WHERE id=?",
                (ts, payment_id),
            )

    def get_delivery(self, payment_id: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM deliveries WHERE payment_id = ?", (payment_id,)
            ).fetchone()

    def pending_delivery_ids(self, limit: int = 100) -> List[int]:
        """Payment ids whose delivery pipeline is armed but not running.

        Feeds the delivery worker loop: claim each id and run the callback.
        """
        with self._lock:
            return [
                r["payment_id"]
                for r in self._conn.execute(
                    "SELECT payment_id FROM deliveries WHERE status='pending'"
                    " ORDER BY payment_id LIMIT ?",
                    (limit,),
                ).fetchall()
            ]

    # -- settlement helpers for the reconciler ------------------------------------
    def stuck_payments(self, now: Optional[int] = None) -> List[sqlite3.Row]:
        """Payments mid-delivery whose lease has expired: crash victims."""
        ts = self.now() if now is None else now
        with self._lock:
            return self._conn.execute(
                """SELECT p.*, d.lease_owner, d.lease_expires, d.attempts AS delivery_attempts
                   FROM payments p JOIN deliveries d ON d.payment_id = p.id
                   WHERE p.status='processing'
                     AND d.status='claimed'
                     AND d.lease_expires IS NOT NULL
                     AND d.lease_expires <= ?""",
                (ts,),
            ).fetchall()

    def expired_unpaid_invoices(
        self, grace: int = 1800, now: Optional[int] = None
    ) -> List[sqlite3.Row]:
        """Invoices past expiry (plus grace) that never produced a payment.

        A successful_payment update can be lost on the wire; the invoice row
        is the only record left. The reconciler checks these against
        getStarTransactions.
        """
        ts = self.now() if now is None else now
        with self._lock:
            return self._conn.execute(
                """SELECT * FROM invoices
                   WHERE status IN ('pending','prechecked')
                     AND expires_at <= ?
                     AND created_at <= ?""",
                (ts, ts - grace),
            ).fetchall()

    def unfulfillable_payments(self) -> List[sqlite3.Row]:
        """Delivered-never, retries exhausted: refund policy applies."""
        with self._lock:
            return self._conn.execute(
                """SELECT p.*, d.attempts AS delivery_attempts, d.last_error
                   FROM payments p JOIN deliveries d ON d.payment_id = p.id
                   WHERE d.status='failed' AND p.status NOT IN ('refund_queued','refunded')"""
            ).fetchall()

    # -- settings / DB-shared flood pause -------------------------------------------
    def set_setting(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def get_setting(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM settings WHERE key=?", (key,)
            ).fetchone()
            return row["value"] if row else None

    def set_flood_pause(self, seconds: int, now: Optional[int] = None) -> None:
        """Pause ALL processes' Bot API calls until now+seconds.

        Stored in the DB (not per-process) so every worker and the
        reconciler honors it — the aiogram-stars postmortem notes flood
        control was per-process and didn't.
        """
        ts = self.now() if now is None else now
        self.set_setting("flood_pause_until", str(ts + int(seconds)))

    def flood_paused(self, now: Optional[int] = None) -> bool:
        ts = self.now() if now is None else now
        raw = self.get_setting("flood_pause_until")
        return raw is not None and int(raw) > ts

    # -- audit -----------------------------------------------------------------------
    def audit(self) -> Dict[str, Any]:
        """Counts and the currently interesting rows, for humans and the CLI."""
        with self._lock:
            out: Dict[str, Any] = {}
            for table in ("invoices", "payments", "deliveries", "refunds", "alerts"):
                rows = self._conn.execute(
                    f"SELECT status, COUNT(*) AS n FROM {table} GROUP BY status"
                ).fetchall()
                out[table] = {r["status"]: r["n"] for r in rows}
            out["stuck_processing"] = len(self.stuck_payments())
            out["flood_paused"] = self.flood_paused()
            out["failed_deliveries"] = [
                dict(r) for r in self.unfulfillable_payments()
            ]
            return out

    def close(self) -> None:
        with self._lock:
            self._conn.close()
