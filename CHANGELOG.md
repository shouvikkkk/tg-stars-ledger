# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-10-19

### Added
- Crash-safe Telegram Stars payment ledger backed by SQLite
- HMAC-signed invoice payloads and idempotent payment ingest
- Automatic reconciliation of stuck payments against getStarTransactions
- Durable refund outbox with retry budget and dead letters
