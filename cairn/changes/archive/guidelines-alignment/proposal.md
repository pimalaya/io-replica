---
cairn: change
id: guidelines-alignment
status: landed
created: 2026-08-02
---

# Align io-replica with the Pimalaya guidelines

## Why

The crate was renamed from io-offline to io-replica on 2026-07-28, but the rename left stale branding behind (README and CONTRIBUTING titles, `Offline <VERB> failed` error prefixes, doc prose damaged by the mechanical `Replica` prefix pass). A line-by-line pass over .github/GUIDELINES.md also surfaced genuine rule violations.

## What

- readme-004 / contributing-002: retitle README and CONTRIBUTING to I/O replica, add the missing cairn/ reading-order item to CONTRIBUTING.
- naming-012 fallout: error message prefixes become `Replica <VERB> failed`, matching io-imap's `IMAP <VERB> failed` shape.
- inline-002 fallout: repair doc prose damaged by the mechanical rename (`ReplicaFlags merge element-wise`, `Local, ReplicaBase and ReplicaRemote`, and similar).
- inline-003: remove the blank lines between `ReplicaYield` variants.
- inline-004: tag the untagged production comment blocks with NOTE (test narration keeps the io-imap-style untagged scenario comments).
- logging-002: drop the top-of-resume `trace!` in every verb (state changes are already logged at the end of match arms) and the now-unused `State` Display impls.
- crate-004: client.rs imports move from std to core and alloc, and the error trait becomes `core::error::Error` (stable since 1.81, rust-version is 1.87).
- crate-003: once client.rs uses only core and alloc, the `client` feature pulls no crates at all, so the golden rule removes the gate: the blocking driver ships unconditionally and the crate carries no features. The same rule was already applied to the hub (see cairn/spec/hub.md).
- changelog-001/002: backfill the empty Unreleased section with the net changes landed since 0.1.0 (the cairn log holds the history), plus this change.
- `ReplicaClientError` variants `ReplicaStorage`/`ReplicaRemote` (rename fallout inside an already-prefixed type) become `Storage`/`Remote`.
