---
cairn: tasks
change: object-bytes-by-reference
---

# Tasks

## io-replica (this repo)

- [ ] Make the fetched body a two-variant payload: inline bytes
      (`{ hash, bytes }`) or an already-persisted reference (`{ hash, size }`).
      Replace `ReplicaFetchedItem.body: Option<(ReplicaHash, Vec<u8>)>`.
- [ ] Make `ReplicaWriteOp::StoreObject.body` optional (`None` = the object is
      already on disk; index it and refcount it, write no bytes).
- [ ] In the upgrade/write path, derive the object's `(hash, size)` from the
      report (not from `Vec::len`), and emit a byteless `StoreObject` when the
      body was persisted during fetch.
- [ ] Unit tests: a persisted-reference fetch indexes the object without bytes;
      an inline fetch behaves exactly as before; `lookup_objects` still short-
      circuits a Full fetch whose object exists.
- [ ] Fold spec: `storage` (two ADDED requirements).
- [ ] Log entry.

## Coordinated downstream Cairn changes

Each carries the same id in its own repo:

- [ ] **io-pimdir** `object-bytes-by-reference`: streaming blob write/read, and a
      byteless `StoreObject` path.
- [ ] **neverest** `object-bytes-by-reference`: streaming remote fetch/append
      over io-imap, header-sniff link id, incremental hash.
- [ ] himalaya-android-m3, cardamum-android (not yet Cairn repos): adopt the new
      body shape — keep the inline variant, no behaviour change.
