---
cairn: change
id: object-bytes-by-reference
status: landed
created: 2026-07-31
---

# Object bytes by reference (bounded-memory body transfer)

## Why

Transferring one large message materialises its whole body in memory, twice.
A Full-tier fetch returns `ReplicaFetchedItem.body: Option<(ReplicaHash, Vec<u8>)>`
— the entire body as a heap `Vec` — which the engine then hands back as
`ReplicaWriteOp::StoreObject { body: Vec<u8> }` for the storage to write; later a
cross-source append reads the whole blob back into another `Vec`. Peak memory is
therefore `O(largest message)` (transiently ~2×). A 60 MB attachment is a 60 MB
allocation on the wire and again on append; a pathological message can dominate a
sync's footprint or OOM the process — fatal on the constrained devices Pimalaya
targets (a mobile app will be killed for the spike).

The protocol layer is already streaming-ready: io-imap exposes
`fetch_body_stream(sink: impl Write)` and `append_stream(source: impl Read, len)`,
both documented as never holding the body whole. The blob store is content-
addressed on disk regardless. The **only** thing forcing full materialisation is
that io-replica carries object *bytes inline* through the fetch result and the
write op. `ReplicaObject` already carries `hash` **and** `size` explicitly, so the
engine needs the bytes for nothing but forwarding them to the store.

## What

Let object bytes be delivered **by reference** so a body never has to sit in
memory whole:

- The Full-tier fetch result MAY report an object the consumer has **already
  persisted** into the blob store, as `(hash, size)` with no inline bytes.
- `StoreObject` carries **either** the object's bytes **or** a reference to an
  already-persisted object; the engine indexes the object from its `(hash, size)`
  either way, and writes bytes only when they are present.
- `lookup_objects` dedup (skipping a fetch whose object already exists) is
  unchanged — it is exactly what makes a persist-during-fetch safe to re-run.

This is a shape change to two types (`ReplicaFetchedItem.body`,
`ReplicaWriteOp::StoreObject.body`), not a behaviour change to the merge. A
consumer that keeps delivering inline bytes is semantically identical to today.

## Scope / non-goals

- **No concurrency here.** Parallel, size-ordered hydration is a separate change
  ([`concurrent-size-ordered-fetch`]) that *depends on this one* — streaming must
  land first so concurrency does not multiply memory (N workers × full body).
- **The replica still keeps the body.** neverest is a replica engine: the object
  lands in the local blob store (offline copy, dedup, resumable restore). This
  change bounds *memory*, not disk; the transfer is fetch→disk→append, not a
  pure fetch-chunk→append-chunk relay. A no-local-copy relay mode is out of scope.
- **Content-addressing is preserved.** The hash is computed in-stream (the FNV
  digest is incremental), so no extra pass over the bytes.

## Blast radius

`ReplicaRemote` / `StoreObject` are implemented/consumed by himalaya-android-m3
and cardamum-android (outside this repo's Cairn). The change is a mechanical
shape update for them — keep the inline/`Some(bytes)` variant and nothing else
moves. io-pimdir and neverest are the repos that gain the streaming path.
