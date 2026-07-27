# TODO

The continuation punch list toward Neverest v2, frozen 2026-07-09 at the end of the engine hardening sessions. The design source of truth for the pipeline items is pimalaya/DELTA_PLAN.md (the crate keeps the io-replica name; only the delta pipeline concept survives from the abandoned rename). Pick up from D1.

## The delta pipeline (D1 to D5, in dependency order)

- [ ] D1. Signal and event vocabulary, the gate on everything below. Inbound: the change-signal types a source hands the engine (a bare tick, a set of touched handles, a vanished set, a new checkpoint hint), with per-protocol mapping notes for IMAP IDLE, JMAP StateChange and WebDAV sync-token. Outbound: per-item delta events emitted by the sync coroutine (added, flags changed, content changed, vanished, conflicted, per handle); today the reconcile surfaces only WriteOps and report counters, and hooks need events. Events are coroutine-yielded data; no I/O enters the core. Open question to settle first: events as the source of truth with the report derived from them (leaning yes), or side by side.
- [ ] D2. Hook seam: a Hook trait next to ReplicaStorage and ReplicaRemote at the std client layer, driven by the D1 events, with filters (on pull, on push, on conflict). Implementations (notification, log, exec) live in Neverest, not here.
- [ ] D3. Watch configuration: no new verb. Document and test end to end: sync with push disabled over a metadata-only replica, an upgrade of the changed handles to Meta for enrichment, hooks on the events. Reporting is at-least-once by contract (a crash between hook and checkpoint write re-fires), same as pushes.
- [ ] D4. Connectors behind feature gates, one feature each: sqlite plus blob dir first, then vdir, maildir, m2dir. This is also where locking lands: each connector provides the single-writer-per-collection guarantee its environment supports (sqlite transactions, a lock file); the core stays lock-free and only documents the contract (see ReplicaStorage::write).
- [ ] D5. Neverest v2 planning doc (CLI, domain-language config, sync and watch subcommands, mirador migration story) once D1 to D3 are stable enough to build on.

## Engine follow-ups

- [ ] Consumer feedback from cardamum-android to fold in (flagged in its docs/io-replica-migration.md): the WantsLookupObject dedup assumes a link id names immutable bytes, but contact replicas sharing a UID diverge legitimately (cardamum answers the lookup empty on purpose); and the m:n membership adapters (JMAP, Google) map Add-with-ReplicaOrigin to a membership patch, a contract worth naming in the ReplicaChange docs.
- [ ] Copy placeholder whose origin died: when the body is cached locally the create could degrade to an append instead of staying pending forever (the consumer-side fallback is documented on ReplicaChange::Add; an engine-side degrade needs a signal that the rejection is permanent).
- [ ] Streaming bodies: ReplicaFetchedItem carries Vec<u8>, fine for PIM, a blocker for large items; revisit only if files ever enter scope (they are out of scope per DELTA_PLAN).
- [ ] Publish 0.1.0 to crates.io (user decision; reserves the name, unblocks dropping the git patches in consumers).

## Property suite, optional round 4

Only worth revisiting if a consumer hits something the suite missed. Candidate shapes: copies and moves in the full-vs-delta differential property (currently flags, edits and deletes only); an intent ledger for the two-active-replicas model (concurrent intents legitimately suppress each other, so the accounting needs happens-before reasoning); rekey inside the two-replica model.

## Consumers

- [ ] cardamum-android: wire the rekey verb (candidate for the CardDAV collection-URL-migration story); on-device shakeout of the API-bump build is pending.
- [ ] himalaya-android: the cache refactor (repo docs/cache-refactor-plan.md) targets this engine; refresh the plan against the current API (Add carries flags and link_id, rekey exists for UIDVALIDITY, report.pushed counts accepted only) before implementation starts.
- [ ] calendula-android: repin io-webdav to git on its next update (the D-prefix namespace fix landed there).
- [ ] mirador: retires into neverest watch once D2 and D3 exist; untouched until then.
