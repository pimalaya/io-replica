//! What each way of resolving a content conflict sends to the remote.
//!
//! A `Manual` conflict holds three bodies: the local one, the base one,
//! and the remote one at the observed revision. A consumer resolves it
//! with an `Edit` carrying whichever it decided on, and the decision is
//! only made when it reaches the remote, so every case here asserts what
//! the server holds afterwards rather than what the placement says.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use io_replica::{
    client::ReplicaClient,
    mutate::ReplicaMutation,
    object::ReplicaObject,
    placement::{ReplicaHandle, ReplicaStatus},
    remote::ReplicaTier,
    sync::ReplicaSyncOptions,
};

use crate::common::{MemRemote, MemStorage, hash};

const BASE: &[u8] = b"the ancestor body";
const LOCAL: &[u8] = b"the local body";
const REMOTE: &[u8] = b"the remote body";
const CUSTOM: &[u8] = b"a hand-merged body";

/// A client whose single inbox member is conflicted: the base holds
/// `BASE`, the placement `LOCAL`, and the remote `REMOTE` at the recorded
/// conflict revision, with the diverging body fetched into the store.
fn conflicted_client() -> ReplicaClient<MemStorage, MemRemote> {
    let mut remote = MemRemote::default();
    remote.mutable = true;
    remote.seed("inbox", "m1", "l1", &[], BASE);

    let mut client = ReplicaClient::new(MemStorage::default(), remote);
    let opts = ReplicaSyncOptions::default();
    client.sync("inbox", opts).unwrap();
    client
        .upgrade("inbox", vec![ReplicaHandle::from("m1")], ReplicaTier::Full)
        .unwrap();

    edit(&mut client, LOCAL);
    client.remote_mut().edit("inbox", "m1", REMOTE);
    client.sync("inbox", opts).unwrap();

    // what a resolver reads the divergence from: the engine fetches
    // nothing itself, so the upgrade pass supplies the diverging body
    client
        .upgrade("inbox", vec![ReplicaHandle::from("m1")], ReplicaTier::Full)
        .unwrap();

    let placement = client.storage().placement("inbox", "m1");
    assert_eq!(placement.status, ReplicaStatus::Conflict);
    assert_eq!(placement.conflict_revision.as_deref(), Some("1"));
    assert_eq!(placement.conflict_object, Some(hash(REMOTE)));

    client
}

fn edit(client: &mut ReplicaClient<MemStorage, MemRemote>, body: &[u8]) {
    let object = ReplicaObject {
        hash: hash(body),
        size: body.len(),
    };
    client
        .mutate(
            "inbox",
            ReplicaMutation::Edit {
                handle: ReplicaHandle::from("m1"),
                object,
                body: body.to_vec(),
                meta: None,
                sort_key: None,
            },
        )
        .unwrap();
}

/// The body the server holds for the inbox member. A pushed body reaches
/// it as its object hash, an uploaded one as its bytes.
fn server_body(client: &ReplicaClient<MemStorage, MemRemote>) -> Vec<u8> {
    client
        .remote()
        .items
        .get(&"inbox".into())
        .and_then(|c| c.get(&ReplicaHandle::from("m1")))
        .map(|item| item.body.clone())
        .expect("the server holds the member")
}

/// Resolves with `body`, syncs, and reports what the server holds, what
/// it counts the run as pushing, and the settled placement status.
fn resolve_and_sync(body: &[u8]) -> (Vec<u8>, usize, ReplicaStatus) {
    let mut client = conflicted_client();
    edit(&mut client, body);

    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    let placement = client.storage().placement("inbox", "m1");

    (server_body(&client), report.pushed, placement.status)
}

#[test]
fn resolving_with_the_ancestor_body_pushes_it() {
    // discarding both diverging bodies for the one they share is the
    // ordinary three-way merge answer, and the remote has to hear it:
    // the replica holding the ancestor while the server holds its own
    // edit is exactly the divergence the resolution settled
    let (body, pushed, status) = resolve_and_sync(BASE);

    assert_eq!(body, hash(BASE).as_str().as_bytes(), "the ancestor pushed");
    assert_eq!(pushed, 1);
    assert_eq!(status, ReplicaStatus::Clean);
}

#[test]
fn resolving_with_the_local_body_pushes_it() {
    let (body, pushed, status) = resolve_and_sync(LOCAL);

    assert_eq!(body, hash(LOCAL).as_str().as_bytes(), "the local body");
    assert_eq!(pushed, 1);
    assert_eq!(status, ReplicaStatus::Clean);
}

#[test]
fn resolving_with_a_hand_merged_body_pushes_it() {
    let (body, pushed, status) = resolve_and_sync(CUSTOM);

    assert_eq!(body, hash(CUSTOM).as_str().as_bytes(), "the merged body");
    assert_eq!(pushed, 1);
    assert_eq!(status, ReplicaStatus::Clean);
}

#[test]
fn resolving_with_the_remote_body_pushes_nothing_and_settles() {
    // adopting the remote wholesale owes the remote nothing: it already
    // holds that body, so the run derives no push and the placement
    // lands clean rather than pending forever
    let (body, pushed, status) = resolve_and_sync(REMOTE);

    assert_eq!(body, REMOTE, "the remote body is untouched");
    assert_eq!(pushed, 0, "the remote already holds the decision");
    assert_eq!(status, ReplicaStatus::Clean);
}

#[test]
fn a_resolution_is_measured_against_the_state_it_settled() {
    // the decision was taken against one remote state, so a remote that
    // moved on since is a fresh divergence: the resolution is kept and
    // conflicted anew rather than overwriting an edit nobody has seen
    let mut client = conflicted_client();
    edit(&mut client, BASE);
    client.remote_mut().edit("inbox", "m1", b"a later body");

    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    assert_eq!(report.pushed, 0, "an unseen remote edit is not overwritten");
    assert_eq!(report.conflicts, 1);
    assert_eq!(server_body(&client), b"a later body");
    let placement = client.storage().placement("inbox", "m1");
    assert_eq!(placement.status, ReplicaStatus::Conflict);
    assert_eq!(
        placement.object,
        Some(hash(BASE)),
        "the resolution survives as the local side of the new divergence",
    );
}
