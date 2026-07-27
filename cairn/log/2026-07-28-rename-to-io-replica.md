---
cairn: log
change: rename-to-io-replica
landed: 2026-07-28
---

# Rename io-offline to io-replica

Renamed the crate `io-offline` → `io-replica`, the module path `io_offline` →
`io_replica`, the public type prefix `Offline*` → `Replica*` (e.g.
`OfflineClient` → `ReplicaClient`, `OfflineSyncOptions` → `ReplicaSyncOptions`,
`OfflineHash` → `ReplicaHash`), and the local directory `io-offline/` →
`io-replica/`. The word "offline" is kept wherever it names the *property* (the
replica is still usable fully offline); only the crate name and type prefix
moved. No behaviour change.

Reason: after the "managed replica + policies" reframe, offline-cache is only one
of the engine's faces; the replica is the invariant center every face shares.
Nothing is published yet, so the cost was at its lowest.

Consumers updated: neverest (dep + `../io-replica` patch + `src/offline/` +
docs, builds + 10 tests green), cardamum-android and himalaya-android-m3 (Rust
`io_replica`/`Replica*` + `../../io-replica` path dep + the new
`ReplicaSyncOptions.rights` default). The two Android crates' full builds run
through their cargo-ndk/gradle toolchains and are pending there; the Rust rename
is mechanical and identical to the (green) io-replica and neverest passes.

Spec updated: `sync` — type names refreshed to `Replica*` (no requirement
added/modified/removed; a pure rename). Past log and archive entries keep their
original `Offline*` names as immutable history.
