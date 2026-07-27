//! Property-based safety net over the sync engine.
//!
//! Byte-fuzzing does not fit this crate (there is no parser); the input
//! space is operation sequences. proptest generates random interleavings
//! of local mutations, server-side mutations and syncs, then asserts the
//! invariants that make the engine trustworthy: no panic on any protocol
//! misuse, no user intent silently lost by the flag merge, convergence to
//! the server state once quiescent, and idempotence of a quiescent sync.
//! Shrinking turns any violation into a minimal counterexample sequence.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    rc::Rc,
};

use io_replica::{
    change::{ReplicaChange, ReplicaWriteOp},
    client::{ReplicaClient, ReplicaRemote, ReplicaStorage},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    coroutine::{ReplicaArg, ReplicaCoroutine, ReplicaCoroutineState},
    mutate::{ReplicaMutate, ReplicaMutation},
    object::{ReplicaHash, ReplicaObject},
    open::ReplicaOpen,
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaPlacement, ReplicaStatus},
    remote::{ReplicaFetchedItem, ReplicaPushResult, ReplicaRemoteSnapshot, ReplicaTier},
    storage::ReplicaLoaded,
    sync::{ReplicaSync, ReplicaSyncOptions, ReplicaSyncReport},
    upgrade::ReplicaUpgrade,
};
use proptest::{prelude::*, test_runner::TestCaseError};

use crate::common::{MemRemote, MemStorage, hash};

// ---- ReplicaFlags::merge: element-wise merge loses no intent ------------------

/// A small flag universe keeps the sets overlapping, which is where the
/// merge actually has work to do.
fn arb_flags() -> impl Strategy<Value = ReplicaFlags> {
    proptest::collection::btree_set(
        prop_oneof![
            Just("seen"),
            Just("flagged"),
            Just("draft"),
            Just("answered")
        ],
        0..4,
    )
    .prop_map(ReplicaFlags::from_iter)
}

proptest! {
    /// Every change a side made against the base survives the merge:
    /// a side's addition is present, a side's removal is absent. This is
    /// the no-silent-loss property of the flag axis.
    #[test]
    fn flags_merge_loses_no_intent(
        base in arb_flags(),
        local in arb_flags(),
        remote in arb_flags(),
    ) {
        let merged = ReplicaFlags::merge(&base, &local, &remote);

        // NOTE: a removal always wins: the other side cannot concurrently
        // re-add a flag it already held in the shared base.
        for side in [&local, &remote] {
            for added in side.0.difference(&base.0) {
                prop_assert!(merged.contains(added), "{added} added by one side");
            }
            for removed in base.0.difference(&side.0) {
                prop_assert!(!merged.contains(removed), "{removed} removed by one side");
            }
        }
    }

    /// The merge is symmetric in its sides and keeps what nobody touched.
    #[test]
    fn flags_merge_is_symmetric_and_stable(
        base in arb_flags(),
        local in arb_flags(),
        remote in arb_flags(),
    ) {
        let ab = ReplicaFlags::merge(&base, &local, &remote);
        let ba = ReplicaFlags::merge(&base, &remote, &local);
        prop_assert_eq!(&ab, &ba);

        let stable = ReplicaFlags::merge(&base, &base, &base);
        prop_assert_eq!(&stable, &base);
    }
}

// ---- coroutine protocol: any arg sequence, never a panic ---------------

/// Any coroutine arg, mostly empty payloads: the point is protocol
/// misuse (wrong variant, missing arg), not payload realism.
fn arb_arg() -> impl Strategy<Value = Option<ReplicaArg>> {
    prop_oneof![
        Just(None),
        Just(Some(ReplicaArg::Write)),
        Just(Some(ReplicaArg::Push(vec![]))),
        Just(Some(ReplicaArg::Fetch(vec![]))),
        Just(Some(ReplicaArg::LookupObject(Default::default()))),
        Just(Some(ReplicaArg::Load(ReplicaLoaded::default()))),
        Just(Some(ReplicaArg::Enumerate(ReplicaRemoteSnapshot {
            items: vec![],
            vanished: vec![],
            complete: true,
            checkpoint: Default::default(),
        }))),
    ]
}

/// Feeds the sequence until the coroutine completes; the property is that
/// it always returns (never panics, never runs past its state machine).
fn feed<C: ReplicaCoroutine>(mut coroutine: C, args: Vec<Option<ReplicaArg>>) {
    for arg in args {
        if let ReplicaCoroutineState::Complete(_) = coroutine.resume(arg) {
            return;
        }
    }
}

proptest! {
    #[test]
    fn coroutines_survive_any_arg_sequence(args in proptest::collection::vec(arb_arg(), 1..8)) {
        feed(ReplicaOpen::new("inbox"), args.clone());
        feed(
            ReplicaMutate::new("inbox", ReplicaMutation::Remove(ReplicaHandle::from("1"))),
            args.clone(),
        );
        feed(
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full),
            args.clone(),
        );
        feed(ReplicaSync::new("inbox", ReplicaSyncOptions::default()), args);
    }
}

// ---- model: random op interleavings converge without loss --------------

/// One step of the random scenario. ReplicaHandle picks are indices resolved
/// modulo the live set at execution time, so every generated op is valid
/// by construction and shrinking stays meaningful.
#[derive(Clone, Debug)]
enum Op {
    /// Replace the flags of the i-th local placement.
    LocalSetFlags(usize, ReplicaFlags),
    /// Delete the i-th local placement offline.
    LocalRemove(usize),
    /// Replace the flags of the i-th server item.
    ServerSetFlags(usize, ReplicaFlags),
    /// Delete the i-th server item.
    ServerRemove(usize),
    /// A new message arrives server-side.
    ServerAdd(u8),
    /// Reconcile.
    Sync,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (any::<usize>(), arb_flags()).prop_map(|(i, f)| Op::LocalSetFlags(i, f)),
        any::<usize>().prop_map(Op::LocalRemove),
        (any::<usize>(), arb_flags()).prop_map(|(i, f)| Op::ServerSetFlags(i, f)),
        any::<usize>().prop_map(Op::ServerRemove),
        any::<u8>().prop_map(Op::ServerAdd),
        Just(Op::Sync),
    ]
}

fn nth(handles: &BTreeSet<ReplicaHandle>, i: usize) -> Option<ReplicaHandle> {
    if handles.is_empty() {
        return None;
    }
    handles.iter().nth(i % handles.len()).cloned()
}

proptest! {
    /// Whatever the interleaving of local edits, server edits and syncs,
    /// two quiescent syncs converge the replica onto the server (same
    /// members, same flags, everything clean) and a third sync is a
    /// no-op. The fake remote accepts every push and its content is
    /// immutable, so no conflict can remain to excuse a divergence.
    #[test]
    fn random_interleavings_converge(ops in proptest::collection::vec(arb_op(), 0..25)) {
        let mut client = ReplicaClient::new(MemStorage::default(), MemRemote::default());
        client.remote_mut().seed("inbox", "m1", "l1", &[], b"one");
        client.remote_mut().seed("inbox", "m2", "l2", &["seen"], b"two");
        let opts = ReplicaSyncOptions::default();
        client.sync("inbox", opts).unwrap();

        for op in ops {
            match op {
                Op::LocalSetFlags(i, flags) => {
                    let local: BTreeSet<ReplicaHandle> = client
                        .open("inbox").unwrap().placements
                        .into_iter()
                        .filter(|p| p.status != ReplicaStatus::Tombstone)
                        .map(|p| p.handle)
                        .collect();
                    if let Some(handle) = nth(&local, i) {
                        client
                            .mutate("inbox", ReplicaMutation::SetFlags { handle, flags })
                            .unwrap();
                    }
                }
                Op::LocalRemove(i) => {
                    let local: BTreeSet<ReplicaHandle> = client
                        .open("inbox").unwrap().placements
                        .into_iter()
                        .filter(|p| p.status != ReplicaStatus::Tombstone)
                        .map(|p| p.handle)
                        .collect();
                    if let Some(handle) = nth(&local, i) {
                        client.mutate("inbox", ReplicaMutation::Remove(handle)).unwrap();
                    }
                }
                Op::ServerSetFlags(i, flags) => {
                    let handles: BTreeSet<ReplicaHandle> = server_handles(&client);
                    if let Some(handle) = nth(&handles, i) {
                        let flags: Vec<&str> = flags.0.iter().map(|f| f.as_str()).collect();
                        client.remote_mut().set_flags("inbox", handle.as_str(), &flags);
                    }
                }
                Op::ServerRemove(i) => {
                    let handles: BTreeSet<ReplicaHandle> = server_handles(&client);
                    if let Some(handle) = nth(&handles, i) {
                        client.remote_mut().remove("inbox", handle.as_str());
                    }
                }
                Op::ServerAdd(n) => {
                    let handle = format!("srv-{n}");
                    let link = format!("lnk-{n}");
                    client.remote_mut().seed("inbox", &handle, &link, &[], b"new");
                }
                Op::Sync => {
                    client.sync("inbox", opts).unwrap();
                }
            }
        }

        // quiesce: local pushes land, then their server echo reconciles
        client.sync("inbox", opts).unwrap();
        client.sync("inbox", opts).unwrap();

        let placements = client.open("inbox").unwrap().placements;
        let local: BTreeSet<ReplicaHandle> = placements.iter().map(|p| p.handle.clone()).collect();
        let server = server_handles(&client);
        prop_assert_eq!(&local, &server, "replica mirrors the server members");

        for placement in &placements {
            prop_assert_eq!(
                placement.status,
                ReplicaStatus::Clean,
                "nothing left dirty after quiescence: {:?}",
                placement,
            );
            let server_flags = client
                .remote()
                .flags_of("inbox", placement.handle.as_str());
            prop_assert_eq!(&placement.flags, server_flags, "flags converged");
        }

        // idempotence: a quiescent sync changes nothing
        let report = client.sync("inbox", opts).unwrap();
        prop_assert_eq!(report, ReplicaSyncReport::default());
    }
}

fn server_handles(client: &ReplicaClient<MemStorage, MemRemote>) -> BTreeSet<ReplicaHandle> {
    client
        .remote()
        .items
        .get(&"inbox".into())
        .map(|c| c.keys().cloned().collect())
        .unwrap_or_default()
}

// ---- model v2: mutable content, revision races, crash injection --------

/// A storage that drops exactly one write batch (simulating a crash after
/// the pushes were serviced but before the write landed), then recovers.
struct CrashyStorage {
    inner: MemStorage,
    /// Write batches left before the crash; `None` = never crash (or
    /// already crashed).
    remaining: Option<usize>,
}

impl ReplicaStorage for CrashyStorage {
    type Error = &'static str;

    fn load(&self, collection: &ReplicaCollectionId) -> Result<ReplicaLoaded, Self::Error> {
        Ok(self.inner.load(collection).unwrap())
    }

    fn lookup_objects(
        &self,
        links: &[ReplicaLinkId],
    ) -> Result<BTreeMap<ReplicaLinkId, ReplicaHash>, Self::Error> {
        Ok(self.inner.lookup_objects(links).unwrap())
    }

    fn write(&mut self, ops: Vec<ReplicaWriteOp>) -> Result<(), Self::Error> {
        match &mut self.remaining {
            Some(0) => {
                self.remaining = None;
                Err("crashed before the write landed")
            }
            Some(left) => {
                *left -= 1;
                self.inner.write(ops).unwrap();
                Ok(())
            }
            None => {
                self.inner.write(ops).unwrap();
                Ok(())
            }
        }
    }
}

/// One step of the mutable-content scenario; indices resolve modulo the
/// live set at execution time. Local ops target the inbox; a copy or move
/// targets the archive; server ops touch the inbox only, so the archive
/// changes exclusively through engine pushes.
#[derive(Clone, Debug)]
enum MutOp {
    LocalSetFlags(usize, ReplicaFlags),
    LocalRemove(usize),
    /// Stage a local content edit on the i-th placement (a fresh body
    /// derived from the byte).
    LocalEdit(usize, u8),
    /// Copy the i-th live inbox placement into the archive.
    LocalCopy(usize),
    /// Move the i-th live inbox placement into the archive.
    LocalMove(usize),
    ServerSetFlags(usize, ReplicaFlags),
    ServerRemove(usize),
    /// A server-side content edit: the revision advances.
    ServerEdit(usize, u8),
    /// A new message arrives server-side, always under a fresh handle (a
    /// real server never reuses one within a uidvalidity).
    ServerAdd(u8),
    /// Raise the i-th live inbox placement to full detail (resolves its
    /// link id, caches its body).
    Upgrade(usize),
    /// A handle-space change (a UIDVALIDITY bump): the server renumbers
    /// every member and the replica runs the rekey verb.
    Bump,
    Sync,
    SyncArchive,
}

fn arb_mut_op() -> impl Strategy<Value = MutOp> {
    prop_oneof![
        (any::<usize>(), arb_flags()).prop_map(|(i, f)| MutOp::LocalSetFlags(i, f)),
        any::<usize>().prop_map(MutOp::LocalRemove),
        (any::<usize>(), any::<u8>()).prop_map(|(i, n)| MutOp::LocalEdit(i, n)),
        any::<usize>().prop_map(MutOp::LocalCopy),
        any::<usize>().prop_map(MutOp::LocalMove),
        (any::<usize>(), arb_flags()).prop_map(|(i, f)| MutOp::ServerSetFlags(i, f)),
        any::<usize>().prop_map(MutOp::ServerRemove),
        (any::<usize>(), any::<u8>()).prop_map(|(i, n)| MutOp::ServerEdit(i, n)),
        any::<u8>().prop_map(MutOp::ServerAdd),
        any::<usize>().prop_map(MutOp::Upgrade),
        Just(MutOp::Bump),
        Just(MutOp::Sync),
        Just(MutOp::SyncArchive),
    ]
}

/// What the user asked for, to be accounted for at the end: every intent
/// must land, stay visibly pending, or be superseded by a strictly later
/// action on the same item; nothing may just evaporate. Entries are
/// removed exactly when a later op legitimately overrides them.
#[derive(Default)]
struct Ledger {
    /// Last staged edit per inbox handle: the body's hash.
    edits: BTreeMap<ReplicaHandle, ReplicaHash>,
    /// Last staged flag change per inbox handle, as the per-element delta
    /// against the base the replica held: (added, removed). Only changed
    /// elements carry an obligation; setting a flag the base already has
    /// claims nothing (the element-wise merge owes nothing for it).
    flags: BTreeMap<ReplicaHandle, (BTreeSet<String>, BTreeSet<String>)>,
    /// Staged copies: the placeholder and the source's server link.
    copies: Vec<(ReplicaHandle, Option<ReplicaLinkId>)>,
    /// Staged moves: the source handle, its server link, and whether a
    /// later server action on the source voided the move.
    moves: Vec<(ReplicaHandle, Option<ReplicaLinkId>, bool)>,
}

type ModelClient = ReplicaClient<CrashyStorage, MemRemote>;

/// The live (non-tombstoned) inbox placements.
fn live(client: &ModelClient) -> BTreeSet<ReplicaHandle> {
    client
        .storage()
        .inner
        .placements
        .iter()
        .filter(|((c, _), p)| c.as_str() == "inbox" && p.status != ReplicaStatus::Tombstone)
        .map(|((_, h), _)| h.clone())
        .collect()
}

fn on_server(client: &ModelClient, collection: &str) -> BTreeSet<ReplicaHandle> {
    client
        .remote()
        .items
        .get(&collection.into())
        .map(|c| c.keys().cloned().collect())
        .unwrap_or_default()
}

fn server_link(client: &ModelClient, handle: &ReplicaHandle) -> Option<ReplicaLinkId> {
    client
        .remote()
        .items
        .get(&"inbox".into())?
        .get(handle)
        .map(|i| i.link_id.clone())
}

/// The current server-side body of an inbox member.
fn server_body(client: &ModelClient, handle: &ReplicaHandle) -> Vec<u8> {
    client
        .remote()
        .items
        .get(&"inbox".into())
        .and_then(|c| c.get(handle))
        .map(|i| i.body.clone())
        .unwrap_or_default()
}

/// The object hash an inbox placement currently points at.
fn held_object(client: &ModelClient, handle: &ReplicaHandle) -> Option<ReplicaHash> {
    client
        .storage()
        .inner
        .placements
        .get(&("inbox".into(), handle.clone()))
        .and_then(|p| p.object.clone())
}

/// Voids the edit intents whose content a server-side change destroyed:
/// a landed edit dies with the content it landed as (matched by body,
/// since a resurrect may have re-keyed the handle), unless a local
/// placement still pends it and the resurrect or resolution path will
/// re-upload it.
fn void_superseded_edits(ledger: &mut Ledger, client: &ModelClient, destroyed: &[u8]) {
    let placements = &client.storage().inner.placements;
    ledger.edits.retain(|_, staged| {
        let landed_here = destroyed == staged.as_str().as_bytes();
        // pending mirrors the engine's resurrect predicate: an unlanded
        // staged edit (object differs from the base object), not a
        // placement that merely holds the landed content while dirty on
        // its flag axis
        let pending = placements.iter().any(|((c, _), p)| {
            c.as_str() == "inbox"
                && p.object.as_ref() == Some(staged)
                && (p.status == ReplicaStatus::Created
                    || (matches!(p.status, ReplicaStatus::Dirty | ReplicaStatus::Conflict)
                        && p.base.as_ref().is_none_or(|b| b.object != p.object)))
        });
        !landed_here || pending
    });
}

fn collection_has_link(client: &ModelClient, collection: &str, link: &ReplicaLinkId) -> bool {
    client
        .remote()
        .items
        .get(&collection.into())
        .into_iter()
        .flatten()
        .any(|(_, item)| &item.link_id == link)
}

/// Runs the mutable-content scenario: seeded server, random ops (syncs may
/// crash once at the injected write batch), quiescence, then conflict
/// resolution, then the convergence and ledger assertions. Shared by the
/// crash-free and the crash-injected properties.
fn check_mutable_model(ops: Vec<MutOp>, crash_after: Option<usize>) -> Result<(), TestCaseError> {
    let storage = CrashyStorage {
        inner: MemStorage::default(),
        remaining: crash_after,
    };
    let mut remote = MemRemote::default();
    remote.mutable = true;
    remote.seed("inbox", "m1", "l1", &[], b"one");
    remote.seed("inbox", "m2", "l2", &["seen"], b"two");

    let mut client = ReplicaClient::new(storage, remote);
    let opts = ReplicaSyncOptions::default();
    let _ = client.sync("inbox", opts);

    let mut ledger = Ledger::default();
    let mut placeholders = 0usize;
    let mut arrivals = 0usize;
    let mut bumps = 0usize;

    for op in ops {
        match op {
            MutOp::LocalSetFlags(i, flags) => {
                if let Some(handle) = nth(&live(&client), i) {
                    let base = client
                        .storage()
                        .inner
                        .placements
                        .get(&("inbox".into(), handle.clone()))
                        .and_then(|p| p.base.as_ref())
                        .map(|b| b.flags.clone())
                        .unwrap_or_default();
                    let added = flags.0.difference(&base.0).cloned().collect();
                    let removed = base.0.difference(&flags.0).cloned().collect();
                    let staged = client.mutate(
                        "inbox",
                        ReplicaMutation::SetFlags {
                            handle: handle.clone(),
                            flags,
                        },
                    );
                    if staged.is_ok() {
                        ledger.flags.insert(handle, (added, removed));
                    }
                }
            }
            MutOp::LocalRemove(i) => {
                if let Some(handle) = nth(&live(&client), i) {
                    let held = held_object(&client, &handle);
                    let server_body = server_body(&client, &handle);
                    if client
                        .mutate("inbox", ReplicaMutation::Remove(handle.clone()))
                        .is_ok()
                    {
                        // the user's own delete supersedes their edits:
                        // the content their placement pends, and the
                        // content that already landed in the deleted item
                        // (matched by body, the local pointer may be gone
                        // after a rekey rebuilt the spine)
                        if let Some(held) = held {
                            ledger.edits.retain(|_, staged| staged != &held);
                        }
                        void_superseded_edits(&mut ledger, &client, &server_body);
                        ledger.edits.remove(&handle);
                        ledger.flags.remove(&handle);
                    }
                }
            }
            MutOp::LocalEdit(i, n) => {
                if let Some(handle) = nth(&live(&client), i) {
                    let held = held_object(&client, &handle);
                    let server_body = server_body(&client, &handle);
                    let body = format!("edit-{n}-{}", handle.as_str()).into_bytes();
                    let object = ReplicaObject {
                        hash: hash(&body),
                        size: body.len(),
                    };
                    let staged = client.mutate(
                        "inbox",
                        ReplicaMutation::Edit {
                            handle: handle.clone(),
                            object,
                            body: body.clone(),
                            meta: None,
                        },
                    );
                    if staged.is_ok() {
                        // editing over content supersedes the edit that
                        // put it there: the content the placement pends,
                        // and the content that landed in the item (the
                        // local pointer may be gone after a rekey)
                        if let Some(held) = held {
                            ledger.edits.retain(|_, staged| staged != &held);
                        }
                        void_superseded_edits(&mut ledger, &client, &server_body);
                        ledger.edits.insert(handle, hash(&body));
                    }
                }
            }
            MutOp::LocalCopy(i) => {
                if let Some(handle) = nth(&live(&client), i) {
                    placeholders += 1;
                    let placeholder = ReplicaHandle::from(format!("tmp-{placeholders}"));
                    let link = server_link(&client, &handle);
                    let staged = client.mutate(
                        "inbox",
                        ReplicaMutation::Copy {
                            handle,
                            target: "archive".into(),
                            placeholder: placeholder.clone(),
                        },
                    );
                    if staged.is_ok() {
                        ledger.copies.push((placeholder, link));
                    }
                }
            }
            MutOp::LocalMove(i) => {
                if let Some(handle) = nth(&live(&client), i) {
                    let link = server_link(&client, &handle);
                    let server_body = server_body(&client, &handle);
                    // a remote content edit the replica has not reconciled
                    // yet beats the delete half of the move: the move is
                    // legitimately overridden from the start
                    let doomed = client
                        .storage()
                        .inner
                        .placements
                        .get(&("inbox".into(), handle.clone()))
                        .and_then(|p| p.base.as_ref())
                        .and_then(|b| b.revision.as_deref())
                        != client
                            .remote()
                            .items
                            .get(&"inbox".into())
                            .and_then(|c| c.get(&handle))
                            .map(|i| i.rev.to_string())
                            .as_deref();
                    let held = held_object(&client, &handle);
                    let staged = client.mutate(
                        "inbox",
                        ReplicaMutation::Move {
                            handle: handle.clone(),
                            target: "archive".into(),
                        },
                    );
                    if staged.is_ok() {
                        // the move supersedes the handle-bound intents,
                        // and takes any content the placement holds or
                        // landed out of the inbox where the ledger
                        // accounts for it
                        if let Some(held) = held {
                            ledger.edits.retain(|_, staged| staged != &held);
                        }
                        void_superseded_edits(&mut ledger, &client, &server_body);
                        ledger.edits.remove(&handle);
                        ledger.flags.remove(&handle);
                        ledger.moves.push((handle, link, doomed));
                    }
                }
            }
            MutOp::ServerSetFlags(i, flags) => {
                if let Some(handle) = nth(&on_server(&client, "inbox"), i) {
                    let flags: Vec<&str> = flags.0.iter().map(|f| f.as_str()).collect();
                    client
                        .remote_mut()
                        .set_flags("inbox", handle.as_str(), &flags);
                    ledger.flags.remove(&handle);
                }
            }
            MutOp::ServerRemove(i) => {
                if let Some(handle) = nth(&on_server(&client, "inbox"), i) {
                    let doomed = client
                        .remote()
                        .items
                        .get(&"inbox".into())
                        .and_then(|c| c.get(&handle))
                        .map(|i| i.body.clone())
                        .unwrap_or_default();
                    client.remote_mut().remove("inbox", handle.as_str());
                    void_superseded_edits(&mut ledger, &client, &doomed);
                    ledger.flags.remove(&handle);
                    for staged_move in &mut ledger.moves {
                        if staged_move.0 == handle {
                            staged_move.2 = true;
                        }
                    }
                }
            }
            MutOp::ServerEdit(i, n) => {
                if let Some(handle) = nth(&on_server(&client, "inbox"), i) {
                    let overwritten = client
                        .remote()
                        .items
                        .get(&"inbox".into())
                        .and_then(|c| c.get(&handle))
                        .map(|i| i.body.clone())
                        .unwrap_or_default();
                    let body = format!("srv-edit-{n}").into_bytes();
                    client.remote_mut().edit("inbox", handle.as_str(), &body);
                    void_superseded_edits(&mut ledger, &client, &overwritten);
                    // a remote edit beats a staged delete, so the move is
                    // legitimately overridden
                    for staged_move in &mut ledger.moves {
                        if staged_move.0 == handle {
                            staged_move.2 = true;
                        }
                    }
                }
            }
            MutOp::ServerAdd(n) => {
                arrivals += 1;
                let handle = format!("srv-{arrivals}");
                let link = format!("lnk-{arrivals}");
                let body = format!("new-{n}").into_bytes();
                client
                    .remote_mut()
                    .seed("inbox", &handle, &link, &[], &body);
            }
            MutOp::Upgrade(i) => {
                if let Some(handle) = nth(&live(&client), i) {
                    let _ = client.upgrade("inbox", vec![handle], ReplicaTier::Full);
                }
            }
            MutOp::Bump => {
                bumps += 1;

                // rekey matches by link id: pending flag deltas and
                // tombstones on link-less placements are dropped by
                // design, so their ledger claims void with them (staged
                // edits always survive, through carry or resurrect)
                let linked: BTreeSet<ReplicaHandle> = client
                    .storage()
                    .inner
                    .placements
                    .iter()
                    .filter(|((c, _), p)| c.as_str() == "inbox" && p.link_id.is_some())
                    .map(|((_, h), _)| h.clone())
                    .collect();
                ledger.flags.retain(|handle, _| linked.contains(handle));
                ledger
                    .moves
                    .retain(|(handle, _, _)| linked.contains(handle));

                let mapping = client.remote_mut().renumber("inbox", bumps);

                // a crash may eat the rekey write; the old spine is then
                // intact and the recovery is simply to run it again
                for _ in 0..3 {
                    if client.rekey("inbox").is_ok() {
                        break;
                    }
                }

                // handle-keyed claims follow their item onto the new space
                ledger.flags = std::mem::take(&mut ledger.flags)
                    .into_iter()
                    .map(|(h, v)| (mapping.get(&h).cloned().unwrap_or(h), v))
                    .collect();
                for staged_move in &mut ledger.moves {
                    if let Some(new) = mapping.get(&staged_move.0) {
                        staged_move.0 = new.clone();
                    }
                }
            }
            MutOp::Sync => {
                let _ = client.sync("inbox", opts);
            }
            MutOp::SyncArchive => {
                let _ = client.sync("archive", opts);
            }
        }
    }

    // drain the injected crash if it has not fired yet (every sync writes
    // at least a checkpoint batch, so this terminates), then quiesce
    while client.storage().remaining.is_some() {
        let _ = client.sync("inbox", opts);
        let _ = client.sync("archive", opts);
    }
    for _ in 0..3 {
        client.sync("inbox", opts).unwrap();
        client.sync("archive", opts).unwrap();
    }

    // resolve every content conflict the scenario produced (divergent
    // edits, or the at-least-once echo of a push whose recording write
    // crashed), the way a consumer would: merge with an edit. Resolutions
    // are local edits, so they register in the ledger like any other.
    for round in 0..3 {
        let conflicted: Vec<ReplicaHandle> = client
            .storage()
            .inner
            .placements
            .values()
            .filter(|p| p.status == ReplicaStatus::Conflict)
            .map(|p| p.handle.clone())
            .collect();
        if conflicted.is_empty() {
            break;
        }
        prop_assert!(round < 2, "conflict resolution must terminate");
        for handle in conflicted {
            let held = held_object(&client, &handle);
            let server_body = server_body(&client, &handle);
            let body = format!("resolved-{}", handle.as_str()).into_bytes();
            let object = ReplicaObject {
                hash: hash(&body),
                size: body.len(),
            };
            // the resolution supersedes the conflicted edit it overwrites,
            // pending or landed
            if let Some(held) = held {
                ledger.edits.retain(|_, staged| staged != &held);
            }
            void_superseded_edits(&mut ledger, &client, &server_body);
            ledger.edits.insert(handle.clone(), hash(&body));
            client
                .mutate(
                    "inbox",
                    ReplicaMutation::Edit {
                        handle,
                        object,
                        body,
                        meta: None,
                    },
                )
                .unwrap();
        }
        client.sync("inbox", opts).unwrap();
        client.sync("inbox", opts).unwrap();
    }

    // settle the archive after any late move or copy pushes
    for _ in 0..2 {
        client.sync("inbox", opts).unwrap();
        client.sync("archive", opts).unwrap();
    }

    // a copy whose source vanished can never land: its placeholder stays
    // visibly pending (Created), which is the accounted end state
    let inbox_server = on_server(&client, "inbox");
    let lingering = |p: &ReplicaPlacement| {
        p.status == ReplicaStatus::Created
            && p.origin
                .as_ref()
                .is_some_and(|o| !inbox_server.contains(&o.handle))
    };

    // convergence: each collection mirrors its server side exactly, dead
    // placeholders aside
    for collection in ["inbox", "archive"] {
        let placements: Vec<ReplicaPlacement> = client
            .storage()
            .inner
            .placements
            .iter()
            .filter(|((c, _), _)| c.as_str() == collection)
            .map(|(_, p)| p.clone())
            .collect();

        let local: BTreeSet<ReplicaHandle> = placements
            .iter()
            .filter(|p| !lingering(p))
            .map(|p| p.handle.clone())
            .collect();
        prop_assert_eq!(
            &local,
            &on_server(&client, collection),
            "{} mirrors the server",
            collection,
        );

        for placement in placements.iter().filter(|p| !lingering(p)) {
            prop_assert_eq!(
                placement.status,
                ReplicaStatus::Clean,
                "nothing pending after resolution: {:?}",
                placement,
            );
            let handle = placement.handle.as_str();
            prop_assert_eq!(
                &placement.flags,
                client.remote().flags_of(collection, handle)
            );
            let server_rev = client.remote().rev_of(collection, handle).to_string();
            prop_assert_eq!(
                placement.base.as_ref().and_then(|b| b.revision.as_deref()),
                Some(server_rev.as_str()),
                "the base revision tracks the server: {:?}",
                placement,
            );
        }
    }

    // ledger: every surviving edit intent's content is on the server (the
    // handle may have changed through a resurrect, so match by body)
    for (handle, staged) in &ledger.edits {
        let found = client
            .remote()
            .items
            .get(&"inbox".into())
            .into_iter()
            .flatten()
            .any(|(_, item)| item.body == staged.as_str().as_bytes());
        prop_assert!(found, "edit intent on {handle:?} lost: {staged:?}");
    }

    // ledger: a surviving flag intent holds per element on the server
    // while its handle exists (a resurrect re-keys the handle and ends
    // the claim)
    for (handle, (added, removed)) in &ledger.flags {
        if let Some(items) = client.remote().items.get(&"inbox".into()) {
            if let Some(item) = items.get(handle) {
                for flag in added {
                    prop_assert!(
                        item.flags.contains(flag),
                        "added flag {flag} on {handle:?} lost",
                    );
                }
                for flag in removed {
                    prop_assert!(
                        !item.flags.contains(flag),
                        "removed flag {flag} on {handle:?} came back",
                    );
                }
            }
        }
    }

    // ledger: a copy landed in the archive or stays visibly pending
    for (placeholder, link) in &ledger.copies {
        let Some(link) = link else { continue };
        let pending = client
            .storage()
            .inner
            .placements
            .get(&("archive".into(), placeholder.clone()))
            .is_some_and(|p| p.status == ReplicaStatus::Created);
        prop_assert!(
            collection_has_link(&client, "archive", link) || pending,
            "copy intent {placeholder:?} lost",
        );
    }

    // ledger: a move either landed in the archive or was overridden by an
    // edit that beat the delete, leaving the item in the inbox (a voided
    // entry saw the overriding server action directly; the echo of a
    // crash-lost update push overrides the same way without one). A move
    // that merely never pushed cannot hide here: a surviving tombstone
    // would fail the all-clean assertion above.
    for (handle, link, voided) in &ledger.moves {
        if *voided {
            continue;
        }
        let Some(link) = link else { continue };
        prop_assert!(
            collection_has_link(&client, "archive", link)
                || collection_has_link(&client, "inbox", link),
            "move intent {handle:?} lost",
        );
    }

    // idempotence: a quiescent sync changes nothing, except the retried
    // adds of dead placeholders, which the remote keeps rejecting
    let report = client.sync("inbox", opts).unwrap();
    prop_assert_eq!(report, ReplicaSyncReport::default());
    let dead_placeholders = client
        .storage()
        .inner
        .placements
        .iter()
        .filter(|((c, _), p)| c.as_str() == "archive" && lingering(p))
        .count();
    let report = client.sync("archive", opts).unwrap();
    let expected = ReplicaSyncReport {
        rejected: dead_placeholders,
        ..Default::default()
    };
    prop_assert_eq!(report, expected);
    Ok(())
}

proptest! {
    /// Mutable-content interleavings (edits on both sides, revision-gated
    /// pushes, real delta snapshots): after quiescence the only survivors
    /// are content conflicts, and resolving each one with an edit brings
    /// the replica to an exact mirror of the server.
    #[test]
    fn mutable_interleavings_converge_after_resolution(
        ops in proptest::collection::vec(arb_mut_op(), 0..25),
    ) {
        check_mutable_model(ops, None)?;
    }

    /// Same scenario with a crash injected at a random write batch: the
    /// batch is lost after the pushes were serviced, so every push replays
    /// at least once. Nothing may be lost or duplicated into divergence;
    /// the worst allowed outcome is a spurious conflict (our own echo),
    /// which resolution then clears.
    #[test]
    fn a_crashed_write_never_loses_data(
        ops in proptest::collection::vec(arb_mut_op(), 0..20),
        crash_after in 0usize..12,
    ) {
        check_mutable_model(ops, Some(crash_after))?;
    }
}

// ---- differential: a full-sync replica and a delta replica agree -------

/// A fake remote shared by two replicas, like one server behind two
/// devices.
#[derive(Clone)]
struct SharedRemote(Rc<RefCell<MemRemote>>);

impl ReplicaRemote for SharedRemote {
    type Error = Infallible;

    fn enumerate(
        &mut self,
        collection: &ReplicaCollectionId,
        cursor: Option<ReplicaCheckpoint>,
    ) -> Result<ReplicaRemoteSnapshot, Infallible> {
        self.0.borrow_mut().enumerate(collection, cursor)
    }

    fn fetch(
        &mut self,
        collection: &ReplicaCollectionId,
        handles: Vec<ReplicaHandle>,
        tier: ReplicaTier,
    ) -> Result<Vec<ReplicaFetchedItem>, Infallible> {
        self.0.borrow_mut().fetch(collection, handles, tier)
    }

    fn push(
        &mut self,
        collection: &ReplicaCollectionId,
        changes: Vec<ReplicaChange>,
    ) -> Result<Vec<ReplicaPushResult>, Infallible> {
        self.0.borrow_mut().push(collection, changes)
    }
}

/// One step of the two-replica scenario. Replica A edits locally and
/// syncs full every time; replica B is a passive incremental mirror.
#[derive(Clone, Debug)]
enum PairOp {
    LocalASetFlags(usize, ReplicaFlags),
    LocalARemove(usize),
    ServerSetFlags(usize, ReplicaFlags),
    ServerEdit(usize, u8),
    ServerRemove(usize),
    ServerAdd(u8),
    SyncA,
    SyncB,
}

fn arb_pair_op() -> impl Strategy<Value = PairOp> {
    prop_oneof![
        (any::<usize>(), arb_flags()).prop_map(|(i, f)| PairOp::LocalASetFlags(i, f)),
        any::<usize>().prop_map(PairOp::LocalARemove),
        (any::<usize>(), arb_flags()).prop_map(|(i, f)| PairOp::ServerSetFlags(i, f)),
        (any::<usize>(), any::<u8>()).prop_map(|(i, n)| PairOp::ServerEdit(i, n)),
        any::<usize>().prop_map(PairOp::ServerRemove),
        any::<u8>().prop_map(PairOp::ServerAdd),
        Just(PairOp::SyncA),
        Just(PairOp::SyncB),
    ]
}

proptest! {
    /// Two replicas of one server, one syncing full every time and one
    /// syncing incrementally from its checkpoint, must end in the same
    /// state: the delta path (changed items, vanished handles, unlisted
    /// untouched) is equivalent to re-reading the whole collection.
    #[test]
    fn full_and_delta_replicas_agree(ops in proptest::collection::vec(arb_pair_op(), 0..25)) {
        let mut server = MemRemote::default();
        server.mutable = true;
        server.seed("inbox", "m1", "l1", &[], b"one");
        server.seed("inbox", "m2", "l2", &["seen"], b"two");
        let server = Rc::new(RefCell::new(server));

        let full_opts = ReplicaSyncOptions {
            full: true,
            ..ReplicaSyncOptions::default()
        };
        let delta_opts = ReplicaSyncOptions::default();
        let mut a = ReplicaClient::new(MemStorage::default(), SharedRemote(server.clone()));
        let mut b = ReplicaClient::new(MemStorage::default(), SharedRemote(server.clone()));
        a.sync("inbox", full_opts).unwrap();
        b.sync("inbox", delta_opts).unwrap();

        let live_a = |a: &ReplicaClient<MemStorage, SharedRemote>| -> BTreeSet<ReplicaHandle> {
            a.storage()
                .placements
                .values()
                .filter(|p| p.status != ReplicaStatus::Tombstone)
                .map(|p| p.handle.clone())
                .collect()
        };
        let on_server = || -> BTreeSet<ReplicaHandle> {
            server
                .borrow()
                .items
                .get(&"inbox".into())
                .map(|c| c.keys().cloned().collect())
                .unwrap_or_default()
        };

        for op in ops {
            match op {
                PairOp::LocalASetFlags(i, flags) => {
                    if let Some(handle) = nth(&live_a(&a), i) {
                        a.mutate("inbox", ReplicaMutation::SetFlags { handle, flags }).unwrap();
                    }
                }
                PairOp::LocalARemove(i) => {
                    if let Some(handle) = nth(&live_a(&a), i) {
                        a.mutate("inbox", ReplicaMutation::Remove(handle)).unwrap();
                    }
                }
                PairOp::ServerSetFlags(i, flags) => {
                    if let Some(handle) = nth(&on_server(), i) {
                        let flags: Vec<&str> = flags.0.iter().map(|f| f.as_str()).collect();
                        server.borrow_mut().set_flags("inbox", handle.as_str(), &flags);
                    }
                }
                PairOp::ServerEdit(i, n) => {
                    if let Some(handle) = nth(&on_server(), i) {
                        let body = format!("srv-edit-{n}").into_bytes();
                        server.borrow_mut().edit("inbox", handle.as_str(), &body);
                    }
                }
                PairOp::ServerRemove(i) => {
                    if let Some(handle) = nth(&on_server(), i) {
                        server.borrow_mut().remove("inbox", handle.as_str());
                    }
                }
                PairOp::ServerAdd(n) => {
                    let handle = format!("srv-{n}");
                    let link = format!("lnk-{n}");
                    server.borrow_mut().seed("inbox", &handle, &link, &[], b"new");
                }
                PairOp::SyncA => {
                    a.sync("inbox", full_opts).unwrap();
                }
                PairOp::SyncB => {
                    b.sync("inbox", delta_opts).unwrap();
                }
            }
        }

        // quiesce both: A's pushes land, B mirrors them, echoes settle
        for _ in 0..3 {
            a.sync("inbox", full_opts).unwrap();
            b.sync("inbox", delta_opts).unwrap();
        }

        let placements_a: Vec<ReplicaPlacement> = a.storage().placements.values().cloned().collect();
        let placements_b: Vec<ReplicaPlacement> = b.storage().placements.values().cloned().collect();
        prop_assert_eq!(
            placements_a,
            placements_b,
            "the full-sync replica and the delta replica diverged",
        );
    }
}

// ---- two active replicas: the full-sync (Neverest) shape ---------------

/// One step with two replicas editing the same server concurrently.
#[derive(Clone, Debug)]
enum DuoOp {
    ASetFlags(usize, ReplicaFlags),
    AEdit(usize, u8),
    ARemove(usize),
    BSetFlags(usize, ReplicaFlags),
    BEdit(usize, u8),
    BRemove(usize),
    ServerAdd(u8),
    SyncA,
    SyncB,
}

fn arb_duo_op() -> impl Strategy<Value = DuoOp> {
    prop_oneof![
        (any::<usize>(), arb_flags()).prop_map(|(i, f)| DuoOp::ASetFlags(i, f)),
        (any::<usize>(), any::<u8>()).prop_map(|(i, n)| DuoOp::AEdit(i, n)),
        any::<usize>().prop_map(DuoOp::ARemove),
        (any::<usize>(), arb_flags()).prop_map(|(i, f)| DuoOp::BSetFlags(i, f)),
        (any::<usize>(), any::<u8>()).prop_map(|(i, n)| DuoOp::BEdit(i, n)),
        any::<usize>().prop_map(DuoOp::BRemove),
        any::<u8>().prop_map(DuoOp::ServerAdd),
        Just(DuoOp::SyncA),
        Just(DuoOp::SyncB),
    ]
}

type DuoClient = ReplicaClient<MemStorage, SharedRemote>;

fn duo_live(client: &DuoClient) -> BTreeSet<ReplicaHandle> {
    client
        .storage()
        .placements
        .values()
        .filter(|p| p.status != ReplicaStatus::Tombstone)
        .map(|p| p.handle.clone())
        .collect()
}

fn duo_mutate(
    client: &mut DuoClient,
    index: usize,
    mutation: impl Fn(ReplicaHandle) -> ReplicaMutation,
) {
    if let Some(handle) = nth(&duo_live(client), index) {
        client.mutate("inbox", mutation(handle)).unwrap();
    }
}

fn duo_edit(client: &mut DuoClient, index: usize, tag: &str, n: u8) {
    if let Some(handle) = nth(&duo_live(client), index) {
        let body = format!("edit-{tag}-{n}-{}", handle.as_str()).into_bytes();
        let object = ReplicaObject {
            hash: hash(&body),
            size: body.len(),
        };
        client
            .mutate(
                "inbox",
                ReplicaMutation::Edit {
                    handle,
                    object,
                    body,
                    meta: None,
                },
            )
            .unwrap();
    }
}

/// Resolves every content conflict on one replica with an edit.
fn duo_resolve(client: &mut DuoClient, tag: &str) -> bool {
    let conflicted: Vec<ReplicaHandle> = client
        .storage()
        .placements
        .values()
        .filter(|p| p.status == ReplicaStatus::Conflict)
        .map(|p| p.handle.clone())
        .collect();
    if conflicted.is_empty() {
        return false;
    }
    for handle in conflicted {
        let body = format!("resolved-{tag}-{}", handle.as_str()).into_bytes();
        let object = ReplicaObject {
            hash: hash(&body),
            size: body.len(),
        };
        client
            .mutate(
                "inbox",
                ReplicaMutation::Edit {
                    handle,
                    object,
                    body,
                    meta: None,
                },
            )
            .unwrap();
    }
    true
}

proptest! {
    /// Two replicas actively editing the same server (the Neverest
    /// full-sync shape): whatever the interleaving of edits, deletes and
    /// syncs on both sides, quiescence plus per-replica conflict
    /// resolution converges both replicas onto the same server state,
    /// with nothing pending anywhere and idempotent final syncs.
    #[test]
    fn two_active_replicas_converge(ops in proptest::collection::vec(arb_duo_op(), 0..25)) {
        let mut server = MemRemote::default();
        server.mutable = true;
        server.seed("inbox", "m1", "l1", &[], b"one");
        server.seed("inbox", "m2", "l2", &["seen"], b"two");
        let server = Rc::new(RefCell::new(server));

        let opts = ReplicaSyncOptions::default();
        let mut a = ReplicaClient::new(MemStorage::default(), SharedRemote(server.clone()));
        let mut b = ReplicaClient::new(MemStorage::default(), SharedRemote(server.clone()));
        a.sync("inbox", opts).unwrap();
        b.sync("inbox", opts).unwrap();

        let mut arrivals = 0usize;
        for op in ops {
            match op {
                DuoOp::ASetFlags(i, flags) => {
                    duo_mutate(&mut a, i, |handle| ReplicaMutation::SetFlags {
                        handle,
                        flags: flags.clone(),
                    });
                }
                DuoOp::AEdit(i, n) => duo_edit(&mut a, i, "a", n),
                DuoOp::ARemove(i) => duo_mutate(&mut a, i, ReplicaMutation::Remove),
                DuoOp::BSetFlags(i, flags) => {
                    duo_mutate(&mut b, i, |handle| ReplicaMutation::SetFlags {
                        handle,
                        flags: flags.clone(),
                    });
                }
                DuoOp::BEdit(i, n) => duo_edit(&mut b, i, "b", n),
                DuoOp::BRemove(i) => duo_mutate(&mut b, i, ReplicaMutation::Remove),
                DuoOp::ServerAdd(n) => {
                    arrivals += 1;
                    let handle = format!("srv-{arrivals}");
                    let link = format!("lnk-{arrivals}");
                    let body = format!("new-{n}").into_bytes();
                    server.borrow_mut().seed("inbox", &handle, &link, &[], &body);
                }
                DuoOp::SyncA => {
                    a.sync("inbox", opts).unwrap();
                }
                DuoOp::SyncB => {
                    b.sync("inbox", opts).unwrap();
                }
            }
        }

        // quiesce both, then resolve conflicts per replica until neither
        // holds one; a resolution can conflict with the other replica's
        // resolution, so this ping-pongs at most once before settling
        for _ in 0..4 {
            a.sync("inbox", opts).unwrap();
            b.sync("inbox", opts).unwrap();
        }
        for round in 0..4 {
            let unresolved_a = duo_resolve(&mut a, "a");
            if unresolved_a {
                a.sync("inbox", opts).unwrap();
                a.sync("inbox", opts).unwrap();
            }
            let unresolved_b = duo_resolve(&mut b, "b");
            if unresolved_b {
                b.sync("inbox", opts).unwrap();
                b.sync("inbox", opts).unwrap();
            }
            if !unresolved_a && !unresolved_b {
                break;
            }
            prop_assert!(round < 3, "conflict resolution must terminate");
            a.sync("inbox", opts).unwrap();
            b.sync("inbox", opts).unwrap();
        }
        for _ in 0..2 {
            a.sync("inbox", opts).unwrap();
            b.sync("inbox", opts).unwrap();
        }

        // convergence: both replicas mirror the same server state
        let on_server: BTreeSet<ReplicaHandle> = server
            .borrow()
            .items
            .get(&"inbox".into())
            .map(|c| c.keys().cloned().collect())
            .unwrap_or_default();

        for (name, replica) in [("a", &a), ("b", &b)] {
            let handles: BTreeSet<ReplicaHandle> = replica
                .storage()
                .placements
                .values()
                .map(|p| p.handle.clone())
                .collect();
            prop_assert_eq!(&handles, &on_server, "replica {} mirrors the server", name);

            for placement in replica.storage().placements.values() {
                prop_assert_eq!(
                    placement.status,
                    ReplicaStatus::Clean,
                    "nothing pending on replica {}: {:?}",
                    name,
                    placement,
                );
                let server = server.borrow();
                let item = server
                    .items
                    .get(&"inbox".into())
                    .and_then(|c| c.get(&placement.handle))
                    .expect("mirrored member");
                prop_assert_eq!(&placement.flags, &item.flags);
                let server_rev = item.rev.to_string();
                prop_assert_eq!(
                    placement.base.as_ref().and_then(|base| base.revision.as_deref()),
                    Some(server_rev.as_str()),
                    "the base revision tracks the server",
                );
            }
        }

        // idempotence on both sides
        let report = a.sync("inbox", opts).unwrap();
        prop_assert_eq!(report, ReplicaSyncReport::default());
        let report = b.sync("inbox", opts).unwrap();
        prop_assert_eq!(report, ReplicaSyncReport::default());
    }
}
