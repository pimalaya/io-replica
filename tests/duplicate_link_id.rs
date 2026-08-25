//! An identity one collection holds twice is frozen, never guessed.
//!
//! A placement is identified by its collection and link id, and a source binds
//! it with one handle, so a second copy of one identity has nowhere to live.
//! The reproduction these tests encode: the engine paired one copy, the other
//! stayed invisible, and deleting the bound one propagated a delete that
//! removed the only copy on a side the user never touched.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use io_replica::{
    client::ReplicaClient,
    placement::{ReplicaHandle, ReplicaStatus},
    remote::ReplicaTier,
    sync::ReplicaSyncOptions,
};

use crate::common::{MemRemote, MemStorage};

/// A collection holding one message twice: two handles, one `Message-ID`.
fn twin_client() -> ReplicaClient<MemStorage, MemRemote> {
    let body = b"From: a\r\nMessage-ID: <msg-a>\r\n\r\nbody\r\n";

    let mut remote = MemRemote::default();
    remote.seed("inbox", "u1", "msg-a", &[], body);
    remote.seed("inbox", "u2", "msg-a", &[], body);

    ReplicaClient::new(MemStorage::default(), remote)
}

fn hydrate(client: &mut ReplicaClient<MemStorage, MemRemote>) {
    client
        .upgrade(
            "inbox",
            vec![ReplicaHandle::from("u1"), ReplicaHandle::from("u2")],
            ReplicaTier::Meta,
        )
        .unwrap();
}

/// The placement of `collection` whose status is `Ambiguous`, if any.
fn ambiguous(client: &ReplicaClient<MemStorage, MemRemote>) -> Vec<String> {
    client
        .storage()
        .placements
        .values()
        .filter(|p| p.status == ReplicaStatus::Ambiguous)
        .map(|p| p.handle.as_str().to_string())
        .collect()
}

#[test]
fn a_second_copy_is_recorded_rather_than_linked() {
    let mut client = twin_client();
    client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    hydrate(&mut client);

    let first = client.storage().placement("inbox", "u1");
    let second = client.storage().placement("inbox", "u2");

    assert!(
        first.link_id.is_some(),
        "the first copy keeps the identity it resolved",
    );
    assert!(
        second.link_id.is_none(),
        "the second has nowhere to live: linking it would overwrite the first \
         binding's handle and lose the fact that the source holds it twice",
    );
    assert_eq!(
        first
            .ambiguous_handles
            .iter()
            .map(|h| h.as_str())
            .collect::<Vec<_>>(),
        ["u2"],
        "the losing handle is recorded, so the freeze survives the round trip",
    );
    assert_eq!(first.status, ReplicaStatus::Ambiguous);
}

#[test]
fn an_ambiguous_placement_is_never_deleted_by_a_vanish() {
    // The reproduction's step 2: deleting the bound copy propagated a delete
    // and removed the only copy on a side the user never touched.
    let mut client = twin_client();
    let opts = ReplicaSyncOptions::default();
    client.sync("inbox", opts).unwrap();
    hydrate(&mut client);

    // the bound copy is expunged; the source still holds the other one
    client.remote_mut().remove("inbox", "u1");
    let report = client.sync("inbox", opts).unwrap();

    assert_eq!(
        report.pulled, 0,
        "no vanish is derived: the source demonstrably holds the identity still",
    );
    assert!(
        client
            .storage()
            .placements
            .contains_key(&("inbox".into(), ReplicaHandle::from("u1"))),
        "the frozen placement survives the vanish",
    );
}

#[test]
fn an_ambiguous_placement_derives_no_push() {
    let mut client = twin_client();
    let opts = ReplicaSyncOptions::default();
    client.sync("inbox", opts).unwrap();
    hydrate(&mut client);

    // a flag change the engine cannot attribute to either copy
    client.remote_mut().set_flags("inbox", "u1", &["seen"]);
    let report = client.sync("inbox", opts).unwrap();

    assert_eq!(report.pushed, 0);
    assert_eq!(report.pulled, 0, "nothing is derived in either direction");
    assert_eq!(ambiguous(&client), ["u1"], "still frozen");
}

#[test]
fn resolving_the_duplicate_resumes_the_sync() {
    let mut client = twin_client();
    let opts = ReplicaSyncOptions::default();
    client.sync("inbox", opts).unwrap();
    hydrate(&mut client);
    assert_eq!(ambiguous(&client), ["u1"]);

    // the user deletes the duplicate: the source now holds the identity once
    client.remote_mut().remove("inbox", "u2");
    client.sync("inbox", opts).unwrap();

    assert!(ambiguous(&client).is_empty(), "the freeze clears itself");
    assert_eq!(
        client.storage().placement("inbox", "u1").status,
        ReplicaStatus::Clean,
        "and the item resumes syncing with no further ceremony",
    );
}

#[test]
fn a_mutation_against_an_ambiguous_placement_is_refused() {
    use io_replica::{client::ReplicaClientError, mutate::ReplicaMutation};

    let mut client = twin_client();
    client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    hydrate(&mut client);

    let err = client
        .mutate(
            "inbox",
            ReplicaMutation::SetFlags {
                handle: ReplicaHandle::from("u1"),
                flags: io_replica::placement::ReplicaFlags::from_iter(["seen"]),
            },
        )
        .unwrap_err();

    assert!(
        matches!(err, ReplicaClientError::Coroutine(_)),
        "staging would attach the edit to whichever copy happens to be bound",
    );
}

#[test]
fn the_freeze_survives_a_run_that_never_mentions_the_twin() {
    // The reproduction's step 3: with an incremental enumeration the twin
    // appears exactly once, in the run that discovers it. A freeze that is
    // not persisted forgets, and the item goes back to being deletable.
    let mut client = twin_client();
    let opts = ReplicaSyncOptions::default();
    client.sync("inbox", opts).unwrap();
    hydrate(&mut client);

    // several delta runs later, nothing having mentioned the twin
    for _ in 0..3 {
        client.sync("inbox", opts).unwrap();
    }

    assert_eq!(ambiguous(&client), ["u1"], "still frozen");

    client.remote_mut().remove("inbox", "u1");
    client.sync("inbox", opts).unwrap();
    assert!(
        client
            .storage()
            .placements
            .contains_key(&("inbox".into(), ReplicaHandle::from("u1"))),
        "and still not deletable on the word of one vanished copy",
    );
}
