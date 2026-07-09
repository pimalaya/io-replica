# I/O offline [![Documentation](https://img.shields.io/docsrs/io-offline?style=flat&logo=docs.rs&logoColor=white)](https://docs.rs/io-offline/latest/io_offline) [![Matrix](https://img.shields.io/badge/chat-%23pimalaya-blue?style=flat&logo=matrix&logoColor=white)](https://matrix.to/#/#pimalaya:matrix.org) [![Mastodon](https://img.shields.io/badge/news-%40pimalaya-blue?style=flat&logo=mastodon&logoColor=white)](https://fosstodon.org/@pimalaya)

Offline-first replica engine library, written in Rust.

This library maintains a local replica of remote collections of items (mail first, contacts and calendar next), usable fully offline, that reconciles with the remote through a three-way merge against a stored base. Sync is a consequence of offline editing, not the primary goal. The full design lives in the [SPEC](https://github.com/pimalaya/pimdir/blob/master/SPEC.md).

This library is composed of 2 feature-gated layers:

- Low-level **I/O-free** coroutines: these `no_std`-compatible state machines hold the whole replica logic and emit `Wants` for both storage and remote effects, which a consumer services however it likes.
- Mid-level **std client**: a blocking driver that services those `Wants` through a `Storage` and a `Remote` trait the consumer implements (sqlite plus a blob dir on desktop, io-imap over JNI plus sqlite on Android).

## Core model

Two identity axes, never collapsed. An `Object` is a content-hashed body stored once; a `Placement` is one item's presence in one collection (handle, flags, membership, detail level, sync base). Many placements point at one object: this is the dedup and unified-view mechanism (the same item present in several collections is fetched and stored once).

Each placement sits at one rung of a level ladder, where each rung includes the one below:

- `Probed`: handle known, kept complete per collection so a missing item means deleted only when the base says so, never inferred from a missing body.
- `Meta`: minimal summary cached (a list row, enough to resolve the link id).
- `Full`: linked to a stored object.

## Verbs

Five coroutines: `open` (load a collection, fully offline), `upgrade` (pull a level, no merge, dedup before fetching a body), `mutate` (write flags, content or membership locally, mark dirty, no network), `sync` (derive local changes, pull the remote delta, three-way merge against the base, write the new base and checkpoint), and `rekey` (rebuild a collection after a handle-space change such as an IMAP UIDVALIDITY bump, carrying the cache and pending local state over by link id).

Flags merge element-wise and never conflict: each flag is independent, so divergent sets fold into their union of changes and both sides converge on it. An edit beats a delete in both directions: a remote update resurrects a local tombstone, and a local staged edit survives a remote delete as a pending create.

For mutable-content backends (CardDAV, CalDAV), content changes ride a per-item revision (a WebDAV etag): a local edit pushes an in-place update gated on the last-synced revision, a remote edit drops the stale body for an on-demand refetch, and a divergence marks the placement conflicted, carrying the observed remote revision so the consumer can merge the content itself (vcard-rs for contacts) and resolve with an edit. Immutable-content backends (IMAP) report no revision and stage no edit, so their merge stays flags and membership only.

## AI disclosure

This library was written with the help of an AI assistant.

## License

This project is dual-licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) and the [MIT License](https://opensource.org/license/mit/).
