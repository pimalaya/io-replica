# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`ReplicaStatus::Ambiguous`, plus `ambiguous_handles` on `ReplicaPlacement` and `ReplicaSourceBinding`**: an identity one collection holds twice, which the engine refuses to resolve and refuses to act on. A status variant rather than a flag, so every rule that matches the enum has to say what it does with an identity that cannot be resolved.

- `ReplicaLoadScope`, carried by `ReplicaYield::WantsLoad` and by `ReplicaStorage::load`: a mutation now reads the one placement it edits (an `Add`, the rows holding its link id) and an upgrade the handles it raises, instead of the whole collection. The scope is a floor rather than a ceiling, so a storage that ignores it stays correct.
- `ReplicaDropReason`, carried by `ReplicaWriteOp::DropPlacement`: whether the item itself is gone, or only this row of it.
- `ReplicaPlacement::staged_edit`, the single reading of "there is a local content edit here", replacing six hand-rolled predicates that disagreed about the status guard and about what a missing base means.

- `coroutine::ReplicaArgError`, the one error a driver that breaks the coroutine contract gets, replacing the four per-verb enums that said it identically.
- `ReplicaSync::PUSH_CHUNK` and `ReplicaSync::WRITE_CHUNK`, the number of changes one push chunk holds and the number of writes one batch holds.

### Changed

- `ReplicaChange::Remove` carries the `link_id` its destination would receive, so a consumer relocates a moved member only while the destination does not already hold it.
- `ReplicaSyncReport::pushed` counts the changes a run derived and the remote accepted, rather than the results the consumer reported: a result naming an unknown handle, or one twice, no longer inflates it.

- **`ReplicaChange` carries an idempotency key.** It is now the `ReplicaChangeKind` it used to be (the four verbs, unchanged) plus the `ReplicaChangeKey` naming it, derived from the collection, the handle, the kind and the target state the change makes true. A consumer that records the keys it applied recognises a replay of any kind, where only an add could be recognised before, through its `link_id`. `ReplicaChange::new` is the only way to make one, so a change cannot exist without its key, and `ReplicaChange::handle` reaches the member any kind acts on.

- **A sync pushes and records in chunks**, yielding a `WantsPush` and the `WantsWrite` recording it per chunk of `ReplicaSync::PUSH_CHUNK` changes, instead of one of each per run. A crash between a serviced push and its recording write used to replay every push the run derived; it now replays only the chunk whose write never landed, and a chunk that was never reached was never pushed. A driver that assumed one push and one write per run has to loop; `ReplicaClient` already does.
- The checkpoint lands in the last write of a run rather than in the middle of the batch, and stays the pre-push one: an intermediate chunk's write must not carry a cursor claiming its unrecorded pushes were seen.

- **The merge joins the two sides instead of copying them.** It used to build a set union of the local and remote key spaces, cloning every handle, every remote item and every placement to produce a walk over two sides that were both ordered already; it now walks them together and takes each placement, copying one only where a write takes ownership. On 100k members that pass alone cost 83 ms before the merge looked at anything, against the 225 ms the whole merge takes.
- **A merge hands its writes over in bounded batches** of `ReplicaSync::WRITE_CHUNK`, instead of holding one write per member until the last candidate is resolved. A batch is cut between candidates and never inside one, since the writes of a single candidate (a keep-both resolution stages a member beside the placement it forked from) are consistent only together. An interrupted merge now leaves its prefix applied where it used to leave nothing, which the unmoved checkpoint makes safe to resume.
- `ReplicaRemoteSnapshot::items` is expected sorted by handle, each handle listed once. A snapshot that arrives unsorted is sorted and a repeated handle collapsed, so a consumer that gets it wrong pays a pass rather than correctness.

- `ReplicaLoadScope::Link` becomes `Links`, taking several: the reads that ask about an identity rather than a location have to see every row claiming it.
- **No coroutine resumes once it has completed**, where only `ReplicaSync` refused before. The four others handed back a default output, which is exactly what a run that genuinely did nothing returns, so a driver with a loop bug was told it had succeeded.
- `ReplicaMutateError` keeps its three real variants and composes the shared `ReplicaArgError` as `Arg`.
- The hub projects its three placements (bound, tombstone, create) from one `ReplicaHubItem::project`, each settling only what the source binding decides. A field added to `ReplicaPlacement` is one edit rather than three, where forgetting one was a silently wrong projection rather than a compile error.

### Removed

- `ReplicaCollection`, referenced nowhere. Its `enumerated` flag stated an invariant the engine models nowhere: spine completeness comes off the consumer's snapshot on every run.
- `ReplicaOpenError`, `ReplicaUpgradeError`, `ReplicaRekeyError` and `ReplicaSyncError`, four byte-identical enums differing only in the verb their message named. `coroutine::ReplicaArgError` replaces them: a driver breaking the coroutine contract is one bug, and none of those four verbs can fail on its own terms.

### Fixed

- **A collection holding one identity twice lost mail on a side nobody touched.** A placement is identified by its collection and link id and a source binds it with one handle, so a second copy of one `Message-ID` (a double delivery, a retried `APPEND`, a restore, a migration) had nowhere to live: the fetch that resolved it silently repointed the first binding, and the evidence was gone at that write. Deleting the bound copy then propagated a delete that removed the only copy on another source, while the source that reported it still held the message.

  Such an identity is now frozen rather than guessed: the losing handle is recorded on the placement that holds it, which reads as `Ambiguous`, and the engine derives nothing for it in either direction, including reading its absence from a complete snapshot as a delete. The record is what makes the freeze survive: the twin appears in exactly one enumeration, and an incremental one never mentions it again. An enumeration reporting the identity once again clears it and syncing resumes.

- **A move delivered the item to the target twice.** Both halves of a move can deliver on their own, the target's create by copying from its origin and the source's tombstone by relocating the member, so syncing the target first left the server holding the copy *and* the relocation. Both now recognise what the other did through the link id, and an item whose link id is not resolved yet stages the source half alone, since it has no such key. Neither half could simply be dropped: the create is what makes a move work through a hub, whose bindings carry no origin, and the relocation is what keeps a never-fetched item from being deleted before its copy can run.

- **A read-only source's local delete was lost for good under a delta enumerate.** The delete was applied locally on the promise that the next enumerate would re-add the member, which holds only for a complete snapshot: an incremental one never lists an untouched member again, and the merge only revisits local rows that are not clean, which a dropped row is not. The tombstone is reverted instead, keeping whatever the placement had cached.

- **A housekeeping drop propagated as a delete to every other source.** Reconciling a provisional placeholder to its server-assigned handle, or rebuilding a spine after a UIDVALIDITY bump, drops rows without the item going anywhere, but a storage sharing one item across sources read every drop as a delete and pushed a `Remove` to sources nobody had touched. Only `ReplicaDropReason::Deleted` propagates now, which also ends rekey's reliance on the write batch being applied in list order.

- **A placement recorded at `Full` holding no body was skipped for ever**, since nothing revisits what already reads as reached. The level is a claim and the payload is the fact, so an upgrade revisits either rung whose payload is missing. 0.4.1 fixed this for hub-backed stores only; the plain path needed a resync.

- **A keep-both duplicate could not be identified.** It carried no link id and a constant handle suffix, so a retried add had no idempotency key, a storage sharing items by link could not hold it, and a second resolution overwrote the first. Both are derived from the forked body now: the duplicate is a new item, not another copy of the one it forked from.

- **A derived content push suppressed the flag merge for that item.** It self-healed only because the recorded checkpoint is the pre-push one, so a concurrent remote flag change stayed invisible, and emitted no event, until some later run happened to list the item again. The flag axis always runs now and withholds only its own push, so one handle still yields at most one change.

## [0.4.2] - 2026-08-25

### Fixed

- **A body linked from the object store read as a local edit, on every sync, for good.** `ReplicaUpgrade` resolves a link id against the store before fetching, so an item present in two collections is downloaded once; that branch set the placement's body and level but left its base behind, while the fetch branch a few lines down moves the base with the body. The placement then held a body its base did not, which is exactly the shape of a staged local edit: a storage projected it dirty, the consumer reported a change it never made, and nothing converged. Seen on plain mail, where the same message sits in two folders (an inbox copy and a trashed one).

  The dedup branch now rebases too. **A store already holding such a placement is not repaired by this**: the item is at `Full` with a body, so no upgrade revisits it. Dropping and resyncing the replica clears it, and it cannot recur.

- **A mutable body is no longer linked from another collection.** A link id says two copies are the same item, not that they hold the same bytes, and where a source rewrites bodies in place (CardDAV, CalDAV) the difference is observable through the revision each copy carries. Such a placement is now fetched rather than linked, so no copy's bytes are recorded under another copy's revision. Immutable content (mail) keeps the dedup, which is what it was written for; the object store still deduplicates the bytes either way, so the cost is one fetch, not one body.

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

[unreleased]: https://github.com/pimalaya/io-replica/compare/v0.4.2..HEAD
[0.4.2]: https://github.com/pimalaya/io-replica/compare/v0.4.1..v0.4.2
[0.4.1]: https://github.com/pimalaya/io-replica/compare/v0.4.0..v0.4.1
[0.4.0]: https://github.com/pimalaya/io-replica/compare/v0.3.0..v0.4.0
[0.3.0]: https://github.com/pimalaya/io-replica/compare/v0.2.0..v0.3.0
[0.2.0]: https://github.com/pimalaya/io-replica/compare/v0.1.0..v0.2.0
[0.1.0]: https://github.com/pimalaya/io-replica/compare/root..v0.1.0
