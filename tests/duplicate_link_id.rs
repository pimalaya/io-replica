//! An identity one collection holds twice is two items, never one.
//!
//! A placement is identified by its collection and link id, and a source
//! binds it with one handle, so the second copy cannot take the key the
//! first holds. What follows from that is which copy gets the hint, not
//! that the other goes without: a source holding two resources holds two
//! items, and a replica storing one of them loses data at the point
//! where it noticed the problem.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use io_replica::{
    client::ReplicaClient,
    mutate::ReplicaMutation,
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaStatus},
    remote::ReplicaTier,
    sync::ReplicaSyncOptions,
};

use crate::common::{MemRemote, MemStorage};

/// The two resources a Posteo calendar was found holding: one `UID`,
/// two hrefs, two genuinely different bodies.
const FIRST: &[u8] = b"From: a\r\nMessage-ID: <msg-a>\r\n\r\nthe meeting\r\n";
const SECOND: &[u8] = b"From: a\r\nMessage-ID: <msg-a>\r\n\r\nanother meeting\r\n";

/// The key the second copy is minted under: `dup:`, the hint, `#`, and
/// the handle the source holds it at.
const MINTED: &str = "dup:msg-a#u2";

/// A collection holding one identity twice: two handles, one hint.
fn twin_client() -> ReplicaClient<MemStorage, MemRemote> {
    let mut remote = MemRemote::default();
    remote.seed("inbox", "u1", "msg-a", &[], FIRST);
    remote.seed("inbox", "u2", "msg-a", &[], SECOND);

    ReplicaClient::new(MemStorage::default(), remote)
}

/// Syncs the collection and hydrates both copies, bodies included.
fn hydrate(client: &mut ReplicaClient<MemStorage, MemRemote>, handles: [&str; 2]) {
    client.sync("inbox", full()).unwrap();
    let handles = handles.iter().copied().map(ReplicaHandle::from).collect();
    client.upgrade("inbox", handles, ReplicaTier::Full).unwrap();
}

/// A run enumerating the whole collection, which is what a DAV server
/// implementing no `sync-collection` leaves a consumer doing.
fn full() -> ReplicaSyncOptions {
    ReplicaSyncOptions {
        full: true,
        ..Default::default()
    }
}

fn link(client: &ReplicaClient<MemStorage, MemRemote>, handle: &str) -> Option<ReplicaLinkId> {
    client.storage().placement("inbox", handle).link_id.clone()
}

#[test]
fn a_second_copy_is_minted_and_stored_with_its_own_body() {
    let mut client = twin_client();
    hydrate(&mut client, ["u1", "u2"]);

    let first = client.storage().placement("inbox", "u1");
    let second = client.storage().placement("inbox", "u2");

    assert_eq!(
        first.link_id,
        Some(ReplicaLinkId::from("msg-a")),
        "the first copy keeps the identity it resolved",
    );
    assert_eq!(
        second.link_id,
        Some(ReplicaLinkId::from(MINTED)),
        "and the second is minted from the hint and its own handle",
    );

    let object = second.object.clone().expect("the second copy has a body");
    assert_ne!(first.object, second.object, "two resources, two bodies");
    assert_eq!(
        client
            .storage()
            .objects
            .get(&object)
            .map(|(_, b)| b.clone()),
        Some(SECOND.to_vec()),
        "the body the user would otherwise never see is stored",
    );
    assert_eq!(second.status, ReplicaStatus::Clean, "an ordinary item");
}

#[test]
fn the_mint_is_stable_across_a_fresh_store() {
    let mut first = twin_client();
    hydrate(&mut first, ["u1", "u2"]);

    // the same collection read again from nothing, hydrated in the other
    // order: a fetch batch is order-independent, so the mint may not
    // depend on which copy the consumer happened to return first
    let mut second = twin_client();
    hydrate(&mut second, ["u2", "u1"]);

    assert_eq!(link(&first, "u1"), link(&second, "u1"));
    assert_eq!(link(&first, "u2"), link(&second, "u2"));
    assert_eq!(link(&second, "u2"), Some(ReplicaLinkId::from(MINTED)));
}

#[test]
fn the_second_copy_is_fetched_once_and_kept() {
    // the defect this replaces: a server with no incremental enumeration
    // listed the twin on every run, its body was fetched to resolve its
    // identity, the claim was lost again, and the bytes were left
    // unreferenced. Four downloads and four orphan blobs per sync
    let mut client = twin_client();
    hydrate(&mut client, ["u1", "u2"]);
    let fetched = client.remote().full_fetches.len();

    for _ in 0..3 {
        client.sync("inbox", full()).unwrap();
    }

    assert_eq!(
        client.remote().full_fetches.len(),
        fetched,
        "a complete enumeration re-lists the twin and re-fetches nothing",
    );
    assert_eq!(link(&client, "u2"), Some(ReplicaLinkId::from(MINTED)));
    assert_eq!(client.storage().objects.len(), 2, "no orphan bodies");
}

#[test]
fn a_vanish_removes_the_copy_that_went_and_no_other() {
    let mut client = twin_client();
    hydrate(&mut client, ["u1", "u2"]);

    // the copy holding the bare hint is expunged; the other one is not
    client.remote_mut().remove("inbox", "u1");
    client.sync("inbox", full()).unwrap();

    assert!(
        !client
            .storage()
            .placements
            .contains_key(&("inbox".into(), ReplicaHandle::from("u1"))),
        "the copy the source dropped is gone",
    );
    assert_eq!(
        link(&client, "u2"),
        Some(ReplicaLinkId::from(MINTED)),
        "and the survivor keeps the key it was minted under: re-canonicalising \
         it would change an identity a consumer has already shown",
    );
}

#[test]
fn each_copy_reconciles_on_its_own() {
    let mut client = twin_client();
    hydrate(&mut client, ["u1", "u2"]);

    // one flag pulled from the remote, one pushed to it, one per copy
    client.remote_mut().set_flags("inbox", "u1", &["seen"]);
    client
        .mutate(
            "inbox",
            ReplicaMutation::SetFlags {
                handle: ReplicaHandle::from("u2"),
                flags: ReplicaFlags::from_iter(["flagged"]),
            },
        )
        .unwrap();
    let report = client.sync("inbox", full()).unwrap();

    assert_eq!(report.pulled, 1);
    assert_eq!(report.pushed, 1);
    assert!(
        client
            .storage()
            .placement("inbox", "u1")
            .flags
            .contains("seen"),
        "the remote change reached the copy it names",
    );
    assert!(
        client.remote().flags_of("inbox", "u2").contains("flagged"),
        "and the staged edit reached the other, addressed as the one \
         item it is",
    );
    assert!(
        !client.remote().flags_of("inbox", "u1").contains("flagged"),
        "neither change crossed over to the other copy",
    );
}
