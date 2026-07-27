---
cairn: change
id: rename-to-io-replica
status: landed
created: 2026-07-28
---

# Rename io-offline to io-replica

## Why
After the "managed replica + policies" reframe, offline-cache is only one of the
engine's faces (backup, mirror, migrate, 2-way, watch do not read as "offline").
The invariant center every face shares is the **replica** — the local reconciled
state with per-source bases. `io-replica` names that asset; `io-offline` names a
single use case, and risks the "offline = mobile-only" misread. DELTA_PLAN
already recorded `io-replica` as the best rename candidate. Nothing is published
yet (the two Android clients and neverest are all unreleased), so the cost is at
its lowest; it only grows as consumers and the on-disk index format calcify.

## What
Rename the crate `io-offline` → `io-replica`, the module path `io_offline` →
`io_replica`, and the public type prefix `Offline*` → `Replica*` (e.g.
`OfflineClient` → `ReplicaClient`, `OfflineSyncOptions` → `ReplicaSyncOptions`).
The word "offline" stays where it describes the *property* (the replica is still
usable fully offline); only the crate name and the type prefix move. Update the
consumers (neverest, cardamum-android, himalaya-android-m3) and the local
directory. No behaviour changes. Past log/archive entries keep their original
names (immutable history).
