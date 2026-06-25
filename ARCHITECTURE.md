# io-offline architecture

Read the [Pimalaya ARCHITECTURE](https://github.com/pimalaya/.github/blob/master/ARCHITECTURE.md) first: it describes the conventions every Pimalaya repository shares (the sans-I/O coroutine approach, `no_std`, module and error rules, code style, licensing). This document only covers what is specific to io-offline, and assumes you know that shared context.

If a statement here conflicts with the code, the code wins; please flag it.

## Where io-offline fits

io-offline is an **offline-first replica engine library**: it maintains a local replica of remote collections of items (mail first, contacts and calendar next), usable fully offline, and reconciles that replica with the remote through a three-way merge against a stored base. Sync is a consequence of offline editing, not the primary goal. The full design lives in the [pimdir SPEC](https://github.com/pimalaya/pimdir/blob/master/SPEC.md).

It is deliberately backend-agnostic and protocol-agnostic: it owns no wire protocol and no on-disk format. It speaks two seams, storage and remote, both expressed as coroutine yields, so a consumer wires it to whatever it likes (sqlite plus a blob dir on desktop, io-imap over JNI plus an Android sqlite store on mobile). The crate has two of the three standard layers; there is no CLI:

1. **I/O-free coroutines** (`no_std` core, always present): the whole replica logic, four verbs, emitting `Wants` for both storage and remote effects.
2. **Std client** (`client` feature): a blocking driver, `OfflineClient`, that services those `Wants` through a `Storage` and a `Remote` trait the consumer implements.

## The core model: two identity axes, never collapsed

The engine separates *what an item is* from *where it sits*, and never folds the two together. This split is what makes dedup, the unified across-collections view, and a safe partial cache all fall out for free.

- An `Object` (object.rs) is the content-addressed body: a `Hash` (a collision-resistant content hash) plus a byte size. Stored once, the bytes live out of band at blobdir/<hash>, refcounted so copy, move and undelete are reference edits. Where content is immutable the object never changes; where it is mutable an edit is a new object (new hash). The engine only ever adds: it emits `StoreObject`, never a deref op, so dropping the old hash and reclaiming bytes is the consumer's job today (there is no GC `WriteOp` yet).
- A `Placement` (placement.rs) is one item's presence in one collection. It pins a logical item to a single collection through the protocol `Handle` (an IMAP UID, a WebDAV href, a JMAP id; always a string so non-integer ids are a non-issue), and carries the per-location mutable state.

Many placements may point at one object: this is the dedup and unified-view mechanism. The cross-collection identity is the `LinkId` (placement.rs): a stable content id (the Message-ID header, a vCard or iCalendar UID), never derived from a per-copy value a provider may rewrite. A body present under one collection is linked into another by `LinkId`, so a copied or shared message is fetched and stored once.

### The level ladder

Each placement sits at one rung of a strict ladder (`Level`), where each rung includes the one below:

- `Probed`: the handle is known and nothing else. The spine is kept *complete* per collection, so a missing item means deleted only when the base says so, never inferred from a missing body.
- `Meta`: a minimal summary is cached (a list row, enough to resolve the `LinkId`), held in the placement's `meta` field as an opaque blob.
- `Full`: linked to a stored object body.

The completeness of the `Probed` spine plus the per-placement base, not the presence or absence of a cached body, is what tells deleted from not-cached. That invariant is what keeps a partial body cache safe to sync.

### Status and base

`Status` records how a placement relates to its sync base: `Clean` (in sync), `Dirty` (locally changed, a push pending), `Tombstone` (locally deleted, a remove pending), `Conflict` (both sides diverged, awaiting keep-both resolution), `Created` (locally created under a provisional handle, an add pending; the create path below).

`Base` is the last-synced state the three-way merge reconciles against: the last-synced `flags`, the last-synced `present` membership, and an optional `etag` reserved for the last-synced content identity of mutable-content backends. Today the merge diffs flags only: `etag` is not yet compared, and `RemoteItem` carries no content token to diff it against, so an in-place content edit (a CalDAV event change) is not yet detected. Mail is immutable, so this is moot for it. A placement carries `base = None` until first reconciled.

A `Collection` (collection.rs) is a mailbox, address book or calendar: a `CollectionId`, a name, an opaque per-collection `Checkpoint` (a QRESYNC pack, a JMAP state string, a WebDAV sync-token), and an `enumerated` flag. The engine never inspects the checkpoint; it only round-trips it between storage and remote. The `Collection` struct itself is defined but not yet wired: no verb produces or consumes it (`Loaded` carries placements plus checkpoint directly), so treat it as a reserved carrier for the collection list.

## The coroutine contract

Every verb is an I/O-free state machine (coroutine.rs). It implements `OfflineCoroutine::resume(arg) -> OfflineCoroutineState`, where the state is `Yielded(OfflineYield)` or `Complete(Result<Output, Error>)`. The driver passes `None` on the first call, then feeds back the matching `OfflineArg` on each resume. The engine performs no I/O of any kind: storage is not a trait injected into the engine (that would break the I/O-free contract), it is `Wants` variants like everything else.

`OfflineYield` gathers every effect, the remote seam first, the storage seam second:

- remote: `WantsCount`, `WantsEnumerate { collection, cursor }`, `WantsFetch { collection, handles, tier }`, `WantsPush { collection, changes }`;
- storage: `WantsLoad`, `WantsLookupObject(link_ids)`, `WantsWrite(ops)`.

Each yield is paired with the `OfflineArg` the driver feeds back: `Count`, `Enumerate(RemoteSnapshot)`, `Fetch(Vec<FetchedItem>)`, `Push(Vec<PushResult>)`, `Load(Loaded)`, `LookupObject(BTreeMap<LinkId, Hash>)`, `Write`. Outbound effects travel as two payload types (change.rs): `Change` is what to push to the remote (`Add`, `Remove`, `SetFlags`), and `WriteOp` is what to persist locally (`UpsertPlacement`, `DropPlacement`, `StoreObject`, `SetBase`, `SetCheckpoint`). The consumer applies a `WriteOp` batch atomically. A `Change::Add` carries an optional `Origin` (the source collection and handle, for a server-side copy that avoids re-uploading the body) and an optional `Object` (the bytes to upload for a genuine append); a `Change::Remove` carries an optional `to` collection (a move, a server-side UID MOVE), or `None` for a plain delete the consumer routes to trash.

The remote payloads (remote.rs) are the seam's vocabulary: a `RemoteItem` is one enumerate row (handle and flags, deliberately no `LinkId` and no body, because an IMAP SEARCH returns just UIDs and the link id is resolved later at the `Meta` fetch); a `RemoteSnapshot` carries the observed items, the `vanished` handles, a `complete` flag, and the new checkpoint; a `FetchedItem` carries the resolved `LinkId`, the `Meta`, and the body at `Tier::Full`; a `PushResult` carries the per-change `PushOutcome` (`Accepted` or `Rejected`), plus an `assigned` handle the engine rekeys a confirmed create to.

## The four verbs

- `open` (open.rs): a single storage `WantsLoad`, handing the placements and checkpoint straight back. No network is ever touched; this is the fully-offline collection open.
- `upgrade` (upgrade.rs): a pure pull to a higher level, never a merge. For `Tier::Full` it first resolves the targeted placements' link ids against the object store (`WantsLookupObject`): a body already stored under another collection is linked without any round-trip (the dedup path), and only the misses are fetched. This stores one body for an item that appears in several collections at once.
- `mutate` (mutate.rs): a local edit with no network. It loads the source placement and stages the resulting writes. A `SetFlags` rewrites the source `Dirty`, a `Remove` rewrites it `Tombstone` (the base left untouched so the next sync derives the push), and a `Copy` leaves the source untouched and stages a `Status::Created` placement in the target collection, carrying its `Origin`, under a provisional placeholder handle. Move and append reuse this create staging and are not built yet.
- `sync` (sync.rs): the load-bearing verb, below.

## The three-way merge

`sync` loads local state, enumerates the remote (full or delta), then runs a three-way merge of Local, Base and Remote per placement, keyed on the handle. The merge compares per-placement identities (today the flag set; a content token for mutable backends is the reserved `etag` slot, not yet wired), never raw bytes.

`reconcile` builds the candidate handle set, then merges each:

- `full_candidates` (complete snapshot): the union of local and remote handles, where a local handle absent from the remote reads as removed upstream.
- `delta_candidates` (incremental snapshot): the changed handles, the vanished handles, plus every locally non-clean handle (`Dirty`, `Tombstone`, `Conflict` or `Created`) whose pending push the delta would otherwise never revisit. An unlisted non-clean handle is unchanged upstream, so its remote state is synthesized from its own base.

`merge` dispatches on `(local_present, base_present, remote_present)`:

- local tombstone, was based, still remote: push a `Remove` (held, see below), carrying the move destination from `origin` when it is a move rather than a delete;
- local tombstone, already gone remote: just drop it, no push;
- based and present locally, vanished remote: remote delete, drop it and count a pull;
- not local, not based, present remote: remote add, pull a fresh `Probed` placement;
- present on both: reconcile flags;
- present locally, never based, not upstream: if `Status::Created`, push a `Change::Add` carrying its origin (held, see below); any other base-less placement is left untouched.

`reconcile_flags` is the flag-level three-way merge against `base.flags`: local-only change pushes a `SetFlags` (held), remote-only change pulls, an identical convergence on both sides rebases clean with no push, and a divergence is written back as `Status::Conflict` keeping the local flags and the original base so it can be re-resolved later rather than silently losing a side. A placement present on both but never based converges on the remote.

`OfflineSyncReport` counts `pulled`, `pushed`, `conflicts` and `rejected`.

## Push-outcome discipline

A push is confirmed before the local state is rewritten. This is the subtle correctness point of the whole engine, so it is worth stating plainly.

When the merge derives a push it does not immediately rewrite the placement. A flag push stashes the placement in `pending_rebases`; a tombstone delete stashes the handle in `pending_drops`; a create stashes the placeholder placement in `pending_creates`. The engine then yields `WantsPush` and waits for the `PushResult` outcomes:

- `Accepted`: the rebase is applied (the flag push rebases the placement clean onto its new base), the drop is applied (the confirmed delete drops the placement), or the create is rekeyed (the placeholder is dropped and the placement re-inserted clean and based under the `assigned` server handle).
- `Rejected`: nothing is rewritten. The placement stays `Dirty`, stays a `Tombstone`, or keeps its `Created` placeholder, so the next sync retries the push.

The reason a rejected delete must not drop the placement is specific to incremental sync: dropping a message locally while it still exists on the server creates a permanent desync, because QRESYNC `CHANGEDSINCE` will never re-list an unchanged message, so the replica would never see it again. Holding the drop until the server confirms the move-to-trash closes that hole. Any handle the push never reported on is treated like a rejection (left dirty) for the same reason.

## Checkpoint and sync depths

The checkpoint is an opaque token the engine round-trips. An incremental sync passes the stored checkpoint as the enumerate cursor; the consumer turns that into a QRESYNC `CHANGEDSINCE` plus a new-UID search and returns a delta snapshot (`complete = false`) with explicit `vanished` handles. A full sync (`OfflineSyncOptions.full`) ignores the stored checkpoint, so the enumerate is asked for the whole remote and the merge reconciles the complete spine: it re-adds any locally-missing message and drops any local phantom. This is the recovery path for a replica that has drifted out of sync.

`OfflineSyncOptions.push` is the second depth: when false the source is treated read-only, local changes are kept dirty and never pushed (permission gating), while remote-won changes are still pulled.

In the Android app these depths surface as three user actions: Refresh (an incremental sync), Resync (a full sync, `full = true`), and Download-all (an `upgrade` of a collection's spine to `Tier::Full`). The checkpoint itself, on IMAP, is a serialized sync state of UIDVALIDITY plus HIGHESTMODSEQ plus the highest UID.

## Driving the engine from an app

The std client (client.rs) is the reference driver: `OfflineClient<S, R>` wraps a consumer `Storage` and a consumer `Remote`, and its generic `run` loop services each yield by calling the matching trait method and resuming with the reply. The four verbs are exposed as `open`, `upgrade`, `mutate` and `sync`. A desktop or Neverest consumer backs `Remote` with io-email's blocking clients and `Storage` with sqlite plus a blob dir.

A consumer may also drive the coroutines directly without `OfflineClient`: the Himalaya Android app does, servicing each yield over two Kotlin transports reached by JNI (io-imap as the remote seam, a SQLite index plus a blob dir as the storage seam). That wiring, the JSON contract over JNI, and the IMAP enumerate are app concerns and live in that repo; see the himalaya-android-m3 ARCHITECTURE. Two engine-level facts matter here: a `Handle` is any string (so a consumer's storage keys stay protocol-neutral), and a consumer that does not yet build creates reports `Change::Add` as `Rejected` so the engine keeps the placeholder rather than dropping a member.

## The create path

Offline copy and move are wired end to end (the Himalaya app drives them); offline append (a genuinely new message) is the one piece not yet built. All three are the membership counterpart of the flag and delete pushes, reusing the same confirm-before-rewrite discipline.

Copy:

- `mutate`'s `Copy` stages a `Status::Created` placement in the target collection under a provisional placeholder handle, carrying its `Origin` (the source `CollectionId` plus `Handle`), and leaves the source untouched.
- `sync`'s merge turns that `Created` placement into a `Change::Add { handle, origin, object }`: `origin` set so the push reuses a server-side copy (no body re-upload), `object` reserved for an append. The placeholder is stashed in `pending_creates`.
- On `Accepted`: if the push reports an `assigned` handle (a UIDPLUS-capable driver), `rekey_create` re-inserts the placement clean and based under it; if not, the placeholder is simply dropped and the real handle is re-added by the target's next enumerate, deduping the body by link id. On `Rejected` the placeholder stays for the next sync.

Move:

- `mutate`'s `Move` tombstones the source carrying its destination in `origin` (no target placement), so the merge pushes one `Change::Remove { handle, to: Some(target) }`: the consumer issues a single atomic server-side UID MOVE, never a copy-then-delete with a window where the message is on neither side. The target picks it up on its own next enumerate. (A plain delete is the same `Remove` with `to = None`, routed to trash.)

Not built: append (`origin = None`, pushing the stored object's bytes via the reserved `object` field) for a genuinely new local message (an offline compose or draft). The engine shape is ready; only the `mutate` entry and the driver's `APPEND` are missing.

## Module layout

```
src/
  lib.rs          crate root: no_std, module + client-feature gates
  coroutine.rs    OfflineCoroutine / OfflineCoroutineState / OfflineYield / OfflineArg
  object.rs       Object, Hash (content-addressed body)
  placement.rs    Placement, Handle, LinkId, Meta, Flags, Level, Status, Base, Origin
  collection.rs   Collection, CollectionId, Checkpoint
  change.rs       Change (push) + WriteOp (persist)
  remote.rs       RemoteItem, RemoteSnapshot, FetchedItem, PushResult, Tier
  storage.rs      Loaded (the load reply)
  open.rs         OfflineOpen          load-only, fully offline
  upgrade.rs      OfflineUpgrade       pull a level, dedup before fetch
  mutate.rs       OfflineMutate        local flag/remove, mark dirty, no network
  sync.rs         OfflineSync          enumerate + three-way merge + push + checkpoint
  client.rs       (client) OfflineClient: Storage + Remote traits, blocking run loop
```

Each verb follows the standard coroutine template: one `new`, a private `State` enum with a `fmt::Display`, a `resume` matching on `(state, arg)`, paired `debug!`/`trace!` logging, and the canonical test layout exercising each transition plus the missing-arg and unexpected-arg error arms.

## Notes for the reader

A few things are defined ahead of their first use, so do not mistake them for dead code: `WantsCount` and the `Remote::count` capability exist for protocols that expose a cheap member count, but no verb emits `WantsCount` yet (the Android driver stubs it to zero). The `WriteOp::SetBase` variant exists for a base-only rewrite, but the sync coroutine sets the base inline through `UpsertPlacement` rather than emitting `SetBase`. The `Collection` struct (with its `enumerated` flag) is defined but no verb produces or consumes it. The create path (the `Created` status, `Change::Add` with its origin, and the rekey on accept) is built and tested in the engine but not yet driven by any consumer; see the create-path section above.
