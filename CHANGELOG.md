# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1] - 2026-08-25

### Fixed

- **A remote content change never reached a hub-backed store.** `pull_content` drops the stale body and lowers the placement so an upgrade refetches it, but `ReplicaHub::absorb` merged the level as a maximum, so the item kept the `Full` it had reached while it still held a body, and `ReplicaUpgrade` skips whatever already reads as `Full`. The item was left claiming a body it no longer had, carrying the summary of the revision before the change, with no fetch ever derived for it: it stayed stale for good while the consumer re-downloaded it on every run without the write landing.

  `ReplicaHubItem::stored_level` states what the two fields were disagreeing about: `Full` means a stored body, so an item holding none reads one rung down. `absorb` records it and `project` reports it. Recording keeps the storage from persisting the false claim; projecting is what heals a store already written that way, since an upgrade reads what `load` projects rather than the stored row, so no migration and no resync are needed. The maximum stays for every other case, along with its reason: a source that has only probed an item holds no opinion about its detail, and adopting that opinion would un-know what another source read.

  Only mutable content reaches it. Mail bodies are immutable and carry no revision, so nothing ever refreshes; a CardDAV side is where it showed.

## [0.4.0] - 2026-08-24

### Added

- **A flag set can be unknown**, distinct from a known-empty one. `ReplicaFlags` is now `Unknown | Known(set)`, so the state the reference storage records as a `NULL` flags column (pimdir SPEC §13) has somewhere to live: before it, a placement nobody had read the markers of claimed to carry none.

  Empty is an opinion. Element-wise it says every flag the other side holds was removed here, so a source that enumerates without markers (a CardDAV `sync-collection` REPORT returns hrefs and ETags and nothing else) looked exactly like a source that had cleared them. The merge now treats an unknown side as no opinion: the result is whatever the other side reports, two unknown sides stay unknown, and an unknown base is the same fact as no base on the flag axis. In the hub an unknown set **never erases a known one**, the rule the sort key already had.

  `contains` reports `false` for an unknown set, `is_unknown` tells the two absences apart, and `known` hands out the set when there is one.

- **A placement carries a presentation sort key**, `ReplicaSortKey`, beside its summary: the item's position in its collection's natural order, newest first for mail and calendars, A to Z for contacts. The engine ferries it and never parses it, exactly as it does the summary, so what it means stays the kind's business and how it is stored stays the storage's.

  Without it the only orderings a store can serve are by link id or allocation order, neither of which means anything to a reader, so every consumer had to scan a whole collection into memory to render a list.

  Empty means unknown and is the default, so an item is orderable from the moment it exists.

  A fetch refreshes the key at **both** tiers, unlike the link id, which is kept once resolved: the key is a projection of content rather than an identity, so the better-informed derivation should win. A `Full` body carries the real date where an envelope may have carried none.

  In the hub, an unknown key **never erases a known one**, mirroring the rule for an absent summary: a second source that has only probed an item must not un-sort what another source already placed. A rekey carries each key over, falling back to the old placement's when the meta fetch resolved none, so a handle-space change does not un-sort a collection.

### Changed

- **Breaking.** `ReplicaFlags` is an enum rather than a newtype over `BTreeSet<String>`, so its set is reached through `known()` instead of `.0`. `Default` is still known-empty and `FromIterator` still builds a known set, so an ordinary construction is unchanged; unknown is stated outright.

- **Breaking.** `ReplicaPlacement`, `ReplicaFetchedItem` and `ReplicaHubItem` gained a `sort_key` field; `ReplicaMutation::Add` gained one and `ReplicaMutation::Edit` an optional one, on the same terms as its optional `meta` (absent keeps the stored key).

## [0.3.0] - 2026-08-07

### Fixed

- **A per-source content conflict now survives a round trip through the hub.** `ReplicaHub::absorb` dropped an upserted placement's `Conflict` status and its `conflict_revision` (every binding was built as `{ handle, base }`), and `project` derived its status purely from the base comparison with a hardcoded `conflict_revision: None`, so a storage built on the hub read a conflicted placement back as `Dirty`.

  That defeated the merge's own rule that an unresolved conflict is left alone: the push the remote had already rejected was re-derived, re-rejected and re-marked conflicted on **every run**, never converging, while a consumer reading the storage could not tell which items needed resolving. Invisible to immutable-content backends, since mail bodies never conflict.

  `ReplicaSourceBinding` now carries `conflicted` and `conflict_revision`, recorded by `absorb` from a `Conflict` upsert and cleared by an upsert of any other status (so a consumer's resolving edit needs no dedicated call), and projected back ahead of the Clean/Dirty decision. The state lives on the **binding**, not the item: "this source vs its own remote" is a different fact from `ReplicaHubItem::conflicted`, which is "source vs source", and the two stay independent. Purely additive: `reconcile_content` and `ReplicaHubItem` are untouched.

  Persisting the two fields is io-pimdir's half, and it landed alongside this release (`bindings.conflicted` / `bindings.conflict_revision`, folded into the draft schema), so a pimdir-backed store now keeps a conflict across a restart instead of re-deriving the rejected push on every run.

### Changed

- **BREAKING**: `ReplicaSourceBinding` gained the two public fields above, so its struct literals need updating.

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

[unreleased]: https://github.com/pimalaya/io-replica/compare/v0.4.1..HEAD
[0.4.1]: https://github.com/pimalaya/io-replica/compare/v0.4.0..v0.4.1
[0.4.0]: https://github.com/pimalaya/io-replica/compare/v0.3.0..v0.4.0
[0.3.0]: https://github.com/pimalaya/io-replica/compare/v0.2.0..v0.3.0
[0.2.0]: https://github.com/pimalaya/io-replica/compare/v0.1.0..v0.2.0
[0.1.0]: https://github.com/pimalaya/io-replica/compare/root..v0.1.0
