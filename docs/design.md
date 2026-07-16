# io-offline design

The deep design of io-offline: the model, the merge semantics and the operational details that outgrow the src/lib.rs header. The header is the architecture overview and the entry point; read it first, this document assumes it. Where a statement here conflicts with the code, the code wins; please flag it.

## Where io-offline fits

io-offline is an **offline-first replica engine library**: it maintains a local replica of remote collections of items (mail first, contacts and calendar next), usable fully offline, and reconciles that replica with the remote through a three-way merge against a stored base. Sync is a consequence of offline editing, not the primary goal. The full design lives in the [pimdir SPEC](https://github.com/pimalaya/pimdir/blob/master/SPEC.md).

It is deliberately backend-agnostic and protocol-agnostic: it owns no wire protocol and no on-disk format. It speaks two seams, storage and remote, both expressed as coroutine yields, so a consumer wires it to whatever it likes (sqlite plus a blob dir on desktop, io-imap over JNI plus an Android sqlite store on mobile). The crate has two of the three standard layers; there is no CLI:

1. **I/O-free coroutines** (`no_std` core, always present): the whole replica logic, five verbs, emitting `Wants` for both storage and remote effects.
2. **Std client** (`client` feature): a blocking driver, `OfflineClient`, that services those `Wants` through an `OfflineStorage` and an `OfflineRemote` trait the consumer implements.

## The core model: two identity axes, never collapsed

The engine separates *what an item is* from *where it sits*, and never folds the two together. This split is what makes dedup, the unified across-collections view, and a safe partial cache all fall out for free.

- An `OfflineObject` (object.rs) is the content-addressed body: an `OfflineHash` (a collision-resistant content hash) plus a byte size. Stored once, the bytes live out of band at blobdir/<hash>, refcounted so copy, move and undelete are reference edits. Where content is immutable the object never changes; where it is mutable an edit is a new object (new hash). References derive from placement pointers (`OfflinePlacement::object` and `OfflineBase::object`): the consumer maintains the counts by diffing each `UpsertPlacement` against the row it replaces and releasing both pointers of a `DropPlacement`, and may garbage-collect an object no placement points at. `StoreObject` stores bytes and takes no reference of its own.
- An `OfflinePlacement` (placement.rs) is one item's presence in one collection. It pins a logical item to a single collection through the protocol `OfflineHandle` (an IMAP UID, a WebDAV href, a JMAP id; always a string so non-integer ids are a non-issue), and carries the per-location mutable state.

Many placements may point at one object: this is the dedup and unified-view mechanism. The cross-collection identity is the `OfflineLinkId` (placement.rs): a stable content id (the Message-ID header, a vCard or iCalendar UID), never derived from a per-copy value a provider may rewrite. A body present under one collection is linked into another by `OfflineLinkId`, so a copied or shared message is fetched and stored once.

### The level ladder

Each placement sits at one rung of a strict ladder (`OfflineLevel`), where each rung includes the one below:

- `Probed`: the handle is known and nothing else. The spine is kept *complete* per collection, so a missing item means deleted only when the base says so, never inferred from a missing body.
- `Meta`: a minimal summary is cached (a list row, enough to resolve the `OfflineLinkId`), held in the placement's `meta` field as an opaque blob.
- `Full`: linked to a stored object body.

The completeness of the `Probed` spine plus the per-placement base, not the presence or absence of a cached body, is what tells deleted from not-cached. That invariant is what keeps a partial body cache safe to sync.

### OfflineStatus and base

`OfflineStatus` records how a placement relates to its sync base: `Clean` (in sync), `Dirty` (locally changed, a push pending), `Tombstone` (locally deleted, a remove pending), `Conflict` (content diverged on both sides, awaiting keep-both resolution; flags never conflict, they merge element-wise), `Created` (locally created under a provisional handle, an add pending; the create path below).

`OfflineBase` is the last-synced state the three-way merge reconciles against: the last-synced `flags`, an optional `revision` (the last-synced content revision of mutable-content backends: a WebDAV etag, an MS Graph changeKey), and an optional `object` pinning the last-synced body so a content merge keeps its base bytes after a local edit repoints the placement. The base's existence is the membership base: a based placement was a member as of the last sync, and `base = None` means never reconciled. Immutable-content backends (IMAP) report no revision, which the merge reads as unchanged, never as unknown.

An `OfflineCollection` (collection.rs) is a mailbox, address book or calendar: an `OfflineCollectionId`, a name, an opaque per-collection `OfflineCheckpoint` (a QRESYNC pack, a JMAP state string, a WebDAV sync-token), and an `enumerated` flag. The engine never inspects the checkpoint; it only round-trips it between storage and remote. The `OfflineCollection` struct itself is defined but not yet wired: no verb produces or consumes it (`OfflineLoaded` carries placements plus checkpoint directly), so treat it as a reserved carrier for the collection list.

## The coroutine contract

Every verb is an I/O-free state machine (coroutine.rs). It implements `OfflineCoroutine::resume(arg) -> OfflineCoroutineState`, where the state is `Yielded(OfflineYield)` or `Complete(Result<Output, Error>)`. The driver passes `None` on the first call, then feeds back the matching `OfflineArg` on each resume. The engine performs no I/O of any kind: storage is not a trait injected into the engine (that would break the I/O-free contract), it is `Wants` variants like everything else.

`OfflineYield` gathers every effect, the remote seam first, the storage seam second:

- remote: `WantsEnumerate { collection, cursor }`, `WantsFetch { collection, handles, tier }`, `WantsPush { collection, changes }`;
- storage: `WantsLoad`, `WantsLookupObject(link_ids)`, `WantsWrite(ops)`.

Each yield is paired with the `OfflineArg` the driver feeds back: `Enumerate(OfflineRemoteSnapshot)`, `Fetch(Vec<OfflineFetchedItem>)`, `Push(Vec<OfflinePushResult>)`, `Load(OfflineLoaded)`, `LookupObject(BTreeMap<OfflineLinkId, OfflineHash>)`, `Write`. Outbound effects travel as two payload types (change.rs): `OfflineChange` is what to push to the remote (`Add`, `Remove`, `SetFlags`, `Update`), and `OfflineWriteOp` is what to persist locally (`UpsertPlacement`, `DropPlacement`, `StoreObject`, `SetCheckpoint`). The consumer applies an `OfflineWriteOp` batch atomically. An `OfflineChange::Add` carries an optional `OfflineOrigin` (the source collection and handle, for a server-side copy that avoids re-uploading the body), an optional object `OfflineHash` (the stored body to upload for a genuine append), the `OfflineFlags` to create the member with (an IMAP APPEND flag list; a copy may ignore it, the skew reconciles on the next sync), and an optional `OfflineLinkId` (the idempotency key for a retried add); an `OfflineChange::Remove` carries an optional `to` collection (a move, a server-side UID MOVE), or `None` for a plain delete the consumer routes to trash; an `OfflineChange::Update` replaces a member's content in place, gated on the last-synced revision (`if_match`).

Pushes are at-least-once: a crash between a serviced push and the storage write that records it replays the change on the next sync. Flag and content pushes re-apply harmlessly; the consumer keeps the other two harmless by treating a remove of an already-missing member as `Accepted` and by using an add's `link_id` to detect that it already landed instead of duplicating it.

The remote payloads (remote.rs) are the seam's vocabulary: an `OfflineRemoteItem` is one enumerate row (handle and flags, deliberately no `OfflineLinkId` and no body, because an IMAP SEARCH returns just UIDs and the link id is resolved later at the `Meta` fetch); an `OfflineRemoteSnapshot` carries the observed items, the `vanished` handles, a `complete` flag, and the new checkpoint; an `OfflineFetchedItem` carries the resolved `OfflineLinkId`, the `OfflineMeta`, and the body at `OfflineTier::Full`; an `OfflinePushResult` carries the per-change `OfflinePushOutcome` (`Accepted` or `Rejected`), plus an `assigned` handle the engine rekeys a confirmed create to.

## The five verbs

- `open` (open.rs): a single storage `WantsLoad`, handing the placements and checkpoint straight back. No network is ever touched; this is the fully-offline collection open.
- `upgrade` (upgrade.rs): a pure pull to a higher level, never a merge. For `OfflineTier::Full` it first resolves the targeted placements' link ids against the object store (`WantsLookupObject`): a body already stored under another collection is linked without any round-trip (the dedup path), and only the misses are fetched. This stores one body for an item that appears in several collections at once.
- `mutate` (mutate.rs): a local edit with no network. It loads the source placement and stages the resulting writes. A `SetFlags` rewrites the source `Dirty` (a `Created` or `Conflict` placement keeps its status; the flag change rides along), a `Remove` rewrites it `Tombstone` (the base left untouched so the next sync derives the push), an `Edit` stores the new body and repoints the placement at it dirty (editing a conflicted placement is its resolution: the base adopts the remote revision observed at conflict time), a `Copy` leaves the source untouched and stages an `OfflineStatus::Created` placement in the target collection, carrying its `OfflineOrigin`, under a provisional placeholder handle, and a `Move` tombstones the source carrying its destination in `origin` so the sync pushes one atomic server-side move.
- `sync` (sync.rs): the load-bearing verb, below.
- `rekey` (rekey.rs): the recovery verb for a handle-space change (an IMAP UIDVALIDITY bump renumbers every UID). It enumerates the new spine in full, resolves the new link ids at the meta tier, and carries every old placement over to the new handle of the same logical item: the cache (level, summary, body) survives without a refetch, a pending flag delta re-derives against the new base, a tombstone keeps its pending remove and move destination, a staged edit keeps its body (its first push is last-writer-wins: the old revision chain died with the old handles). Pending state whose link id was never resolved cannot be matched; it is dropped and counted in the report. Pending creates are local staging, not spine, and are left untouched.

## The three-way merge

`sync` loads local state, enumerates the remote (full or delta), then runs a three-way merge of Local, OfflineBase and OfflineRemote per placement, keyed on the handle. The merge compares per-placement identities (the flag set, and for mutable-content backends the content `revision`), never raw bytes.

`reconcile` builds the candidate handle set, then merges each:

- `full_candidates` (complete snapshot): the union of local and remote handles, where a local handle absent from the remote reads as removed upstream.
- `delta_candidates` (incremental snapshot): the changed handles, the vanished handles, plus every locally non-clean handle (`Dirty`, `Tombstone`, `Conflict` or `Created`) whose pending push the delta would otherwise never revisit. An unlisted non-clean handle is unchanged upstream, so its remote state is synthesized from its own base.

`merge` dispatches on `(local_present, based, remote_present)`:

- local tombstone, was based, still remote: push a `Remove` (held, see below), carrying the move destination from `origin` when it is a move rather than a delete; unless the remote revision moved past the base, in which case the remote edit beats the local delete and the placement is re-pulled fresh;
- local tombstone, already gone remote: just drop it, no push;
- based and present locally, vanished remote: remote delete, drop it and count a pull; unless the placement carries a staged content edit (dirty or conflicted), in which case the edit beats the delete and the placement converts to a pending create that re-uploads the edited body (held, see below; a conflict is moot once the remote side is gone);
- not local, not based, present remote: remote add, pull a fresh `Probed` placement;
- present on both: reconcile content, then flags;
- present locally, never based, not upstream: if `OfflineStatus::Created`, push an `OfflineChange::Add` carrying its origin or its stored body (held, see below); any other base-less placement is left untouched.

`reconcile_content` runs first for a placement present on both sides, reading positive signals only: the local side changed when a dirty placement points at a body its base does not hold, the remote side when the enumerate reported a revision differing from the base. A local-only edit pushes an `Update` gated on the base revision (held); a remote-only edit drops the stale body for an on-demand refetch (the level falls back to `Probed`, keeping the stale summary as a display fallback) and rebases the revision; a divergence marks the placement `OfflineStatus::Conflict` carrying the observed remote revision, so the consumer merges the content itself and resolves with an edit. An unresolved conflict is left alone, only its observed remote revision tracks the latest remote.

`reconcile_flags` is an element-wise three-way merge against `base.flags` (`OfflineFlags::merge`): each flag is independent, the side that changed a flag's presence from the base wins for that flag, and both sides changing always agree, so flags never conflict. The merged set is pushed when the local side won any flag and pulled when the remote side did (both can happen in one sync: divergent sets fold into their union of changes and both sides converge on it). A placement present on both but never based converges on the remote. Flag pulls and rebases on a content-conflicted placement keep its `Conflict` status: only an edit resolves a conflict.

`OfflineSyncReport` counts `pulled`, `pushed` (accepted pushes), `conflicts`, `rejected` and `refreshed`.

## Push-outcome discipline

A push is confirmed before the local state is rewritten. This is the subtle correctness point of the whole engine, so it is worth stating plainly.

When the merge derives a push it does not immediately rewrite the placement. A flag push stashes the placement in `pending_flag_pushes`; a content push stashes it in `pending_content_pushes` (kept apart so an accepted flag push on a conflicted placement is never misread as a resolved content push); a tombstone delete stashes the handle in `pending_drops`; a create stashes the placeholder placement in `pending_creates`. The engine then yields `WantsPush` and waits for the `OfflinePushResult` outcomes:

- `Accepted`: the rebase is applied (a flag push rebases the placement clean onto its new base; a content push pins the pushed body and reported revision as the new base, keeping a riding flag edit dirty), the drop is applied (the confirmed delete drops the placement), or the create is rekeyed (the placeholder is dropped and the placement re-inserted clean and based under the `assigned` server handle).
- `Rejected`: nothing is rewritten. The placement stays `Dirty`, stays a `Tombstone`, or keeps its `Created` placeholder, so the next sync retries the push.

The reason a rejected delete must not drop the placement is specific to incremental sync: dropping a message locally while it still exists on the server creates a permanent desync, because QRESYNC `CHANGEDSINCE` will never re-list an unchanged message, so the replica would never see it again. Holding the drop until the server confirms the move-to-trash closes that hole. Any handle the push never reported on is treated like a rejection (left dirty) for the same reason.

## OfflineCheckpoint and sync depths

The checkpoint is an opaque token the engine round-trips. An incremental sync passes the stored checkpoint as the enumerate cursor; the consumer turns that into a QRESYNC `CHANGEDSINCE` plus a new-UID search and returns a delta snapshot (`complete = false`) with explicit `vanished` handles. A full sync (`OfflineSyncOptions.full`) ignores the stored checkpoint, so the enumerate is asked for the whole remote and the merge reconciles the complete spine: it re-adds any locally-missing message and drops any local phantom. This is the recovery path for a replica that has drifted out of sync.

`OfflineSyncOptions.push` is the second depth: when false the source is treated read-only, local changes are kept dirty and never pushed (permission gating), while remote-won changes are still pulled.

In the Android app these depths surface as three user actions: Refresh (an incremental sync), Resync (a full sync, `full = true`), and Download-all (an `upgrade` of a collection's spine to `OfflineTier::Full`). The checkpoint itself, on IMAP, is a serialized sync state of UIDVALIDITY plus HIGHESTMODSEQ plus the highest UID.

## Driving the engine from an app

The std client (client.rs) is the reference driver: `OfflineClient<S, R>` wraps a consumer `OfflineStorage` and a consumer `OfflineRemote`, and its generic `run` loop services each yield by calling the matching trait method and resuming with the reply. The five verbs are exposed as `open`, `upgrade`, `mutate`, `sync` and `rekey`. A desktop or Neverest consumer backs `OfflineRemote` with io-email's blocking clients and `OfflineStorage` with sqlite plus a blob dir.

A consumer may also drive the coroutines directly without `OfflineClient`: the Himalaya Android app does, servicing each yield over two Kotlin transports reached by JNI (io-imap as the remote seam, a SQLite index plus a blob dir as the storage seam). That wiring, the JSON contract over JNI, and the IMAP enumerate are app concerns and live in that repo; see the himalaya-android-m3 ARCHITECTURE. Two engine-level facts matter here: an `OfflineHandle` is any string (so a consumer's storage keys stay protocol-neutral), and a consumer that does not yet build creates reports `OfflineChange::Add` as `Rejected` so the engine keeps the placeholder rather than dropping a member.

## The create path

Offline copy and move are wired end to end (the Himalaya app drives them); offline append (a genuinely new message) is the one piece not yet built. All three are the membership counterpart of the flag and delete pushes, reusing the same confirm-before-rewrite discipline.

Copy:

- `mutate`'s `Copy` stages an `OfflineStatus::Created` placement in the target collection under a provisional placeholder handle, carrying its `OfflineOrigin` (the source `OfflineCollectionId` plus `OfflineHandle`), and leaves the source untouched.
- `sync`'s merge turns that `Created` placement into an `OfflineChange::Add { handle, link_id, flags, origin, object }`: `origin` set so the push reuses a server-side copy (no body re-upload), `object` carrying the stored body's hash for an append, `flags` the flag set to create with, `link_id` as the retry idempotency key. The placeholder is stashed in `pending_creates`.
- On `Accepted`: if the push reports an `assigned` handle (a UIDPLUS-capable driver), `rekey_create` re-inserts the placement clean and based under it; if not, the placeholder is simply dropped and the real handle is re-added by the target's next enumerate, deduping the body by link id. On `Rejected` the placeholder stays for the next sync.

Move:

- `mutate`'s `Move` tombstones the source carrying its destination in `origin` (no target placement), so the merge pushes one `OfflineChange::Remove { handle, to: Some(target) }`: the consumer issues a single atomic server-side UID MOVE, never a copy-then-delete with a window where the message is on neither side. The target picks it up on its own next enumerate. (A plain delete is the same `Remove` with `to = None`, routed to trash.)

Append (`origin = None`, `object` naming the stored body to upload) is pushed by the sync in one case today: a staged local edit whose remote member was deleted upstream resurrects as a pending create. A `mutate` entry for a genuinely new local item (an offline compose or draft) is not built yet; the engine shape is ready, only that entry and the driver's `APPEND` are missing.

## Module layout

```
src/
  lib.rs          crate root: no_std, module + client-feature gates
  coroutine.rs    OfflineCoroutine / OfflineCoroutineState / OfflineYield / OfflineArg
  object.rs       OfflineObject, OfflineHash (content-addressed body)
  placement.rs    OfflinePlacement, OfflineHandle, OfflineLinkId, OfflineMeta, OfflineFlags, OfflineLevel, OfflineStatus, OfflineBase, OfflineOrigin
  collection.rs   OfflineCollection, OfflineCollectionId, OfflineCheckpoint
  change.rs       OfflineChange (push) + OfflineWriteOp (persist)
  remote.rs       OfflineRemoteItem, OfflineRemoteSnapshot, OfflineFetchedItem, OfflinePushResult, OfflineTier
  storage.rs      OfflineLoaded (the load reply)
  open.rs         OfflineOpen          load-only, fully offline
  upgrade.rs      OfflineUpgrade       pull a level, dedup before fetch
  mutate.rs       OfflineMutate        local flag/remove, mark dirty, no network
  sync.rs         OfflineSync          enumerate + three-way merge + push + checkpoint
  rekey.rs        OfflineRekey         rebuild after a handle-space change, carry by link id
  client.rs       (client) OfflineClient: OfflineStorage + OfflineRemote traits, blocking run loop
```

Each verb follows the standard coroutine template: one `new`, a private `State` enum with a `fmt::Display`, a `resume` matching on `(state, arg)`, paired `debug!`/`trace!` logging, and the canonical test layout exercising each transition plus the missing-arg and unexpected-arg error arms.

## Notes for the reader

One thing is defined ahead of its first use, so do not mistake it for dead code: the `OfflineCollection` struct (with its `enumerated` flag) is defined but no verb produces or consumes it (`OfflineLoaded` carries placements plus checkpoint directly); treat it as a reserved carrier for the collection list. A handle-space change (an IMAP UIDVALIDITY bump renumbers every handle) is `rekey`'s job: a consumer that detects the change runs it instead of a full sync, which would drop the whole spine and every pending change with it. The residual limitation is that rekey matches by link id, so pending state on placements that never resolved one (a probed-only spine) is dropped, and said so in the report.
