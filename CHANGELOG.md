# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-08-06

### Added

- Added the multi-source hub (the `hub` module): one shared item with a per-source base, projected as per-source placements and absorbed back from sync writes, so multi-source propagation (adds, flags, deletes, staged removes and moves) falls out of the ordinary per-source merge with no cross-merge.

  Cross-source content conflicts resolve by the `ReplicaHubConflict` policy (manual, prefer incoming, prefer existing); a client-staged tombstone marks the item deleted while keeping the source binding so the remove still pushes.

- Added per-item sync events: `ReplicaSyncReport` now carries the ordered `ReplicaEvent` list (added, flags changed, content changed, vanished, conflicted, created) for hooks and richer reporting.
- Added granular push rights: `ReplicaSyncOptions::rights` refines the master `push` switch per kind (flags, content, add, remove), each forbidden kind kept pending instead of pushed.
- Added the headless conflict policy: `ReplicaSyncOptions::conflict` resolves a content conflict unattended (prefer local, prefer remote, keep both) instead of the interactive manual default.
- Added the local `Add` mutation: a brand-new, locally-authored item (compose, import) staged as a pending create the next sync appends, guarded against live link id collisions.
- Added bounded-memory body transfer: a `Full` fetch reports `ReplicaFetchedBody::Inline` bytes for the engine to store, or `Persisted` for an object the consumer already streamed into its blob store, and `ReplicaWriteOp::StoreObject` carries its bytes optionally.
- Documented soft-delete retention at the storage seam: `DropPlacement` is the retention decision point, and a storage hiding soft-deleted rows from `load` never has them re-derived.
- Guaranteed that a `WantsFetch` batch is order-independent, so a consumer may service it across a connection pool.

### Changed

- Renamed the crate from io-offline to io-replica.
- Removed the `client` feature: the blocking driver performs no I/O of its own, so it now builds from core and alloc only and ships unconditionally; the crate carries no cargo features.
- Renamed the `ReplicaClientError` variants `ReplicaStorage` and `ReplicaRemote` to `Storage` and `Remote`, and reworded the error messages from the `Offline` prefix to `Replica` (`Replica SYNC failed: …`).
- Aligned logging with the library rules: coroutines no longer trace their state at the top of every resume; state changes keep logging at the end of match arms.

### Fixed

- Fixed a fetch overwriting an already-resolved link id: a fetch establishes the link only for a not-yet-linked item, so two tiers disagreeing on the link can no longer strand and duplicate it.

## [0.1.0] - 2026-07-16

### Added

- Added the five I/O-free coroutines (open, upgrade, mutate, sync, rekey): no_std state machines over the two-axis model of content-addressed Objects and per-collection Placements, emitting Wants for both storage and remote effects.
- Added the three-way merge against a stored base, with element-wise flag merging (flags never conflict, divergent sets fold into their union of changes) and content conflicts kept both, carrying the observed remote revision for the consumer to merge and resolve with an edit.
- Added edit-beats-delete in both directions: a remote update resurrects a local tombstone, and a local staged edit survives a remote delete as a pending create re-uploading the edited body.
- Added optimistic-concurrency content pushes gated on the last-synced revision (if_match), following the confirm-before-rewrite discipline: no local state is rewritten until the remote accepts the push.
- Added the rekey verb, rebuilding a collection after a handle-space change (an IMAP UIDVALIDITY bump) and carrying the cache and pending local state over to the new handles by link id.
- Added the std client behind the client feature: a blocking ReplicaClient servicing every yield through the consumer-implemented Storage and Remote traits.
- Documented the at-least-once push contract (an add's link_id dedups a retry, a remove of an already-missing member reads as accepted) and the pointer-derived object refcounting the consumer maintains by diffing placement upserts and drops.

[unreleased]: https://github.com/pimalaya/io-replica/compare/v0.2.0..HEAD
[0.2.0]: https://github.com/pimalaya/io-replica/compare/v0.1.0..v0.2.0
[0.1.0]: https://github.com/pimalaya/io-replica/compare/root..v0.1.0
