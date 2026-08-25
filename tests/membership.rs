//! Cross-collection membership: a move must deliver exactly one copy.
//!
//! A move is staged as a copy into the target plus a remove from the
//! source (see `ReplicaMutation::Move`), so the two halves are derived by
//! two independent syncs. These tests pin what each order produces: one
//! member in the target, never two, and never a source deleted before its
//! copy landed.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use io_replica::{
    client::ReplicaClient,
    mutate::ReplicaMutation,
    placement::{ReplicaHandle, ReplicaStatus},
    remote::ReplicaTier,
    sync::ReplicaSyncOptions,
};

use crate::common::{MemRemote, MemStorage};

fn seeded_client() -> ReplicaClient<MemStorage, MemRemote> {
    let body = b"From: a\r\nMessage-ID: <msg-a>\r\n\r\nbody\r\n";

    let mut remote = MemRemote::default();
    remote.seed("inbox", "i1", "msg-a", &[], body);

    ReplicaClient::new(MemStorage::default(), remote)
}

/// Members the remote holds in `collection`.
fn remote_members(client: &ReplicaClient<MemStorage, MemRemote>, collection: &str) -> Vec<String> {
    client
        .remote()
        .items
        .get(&collection.into())
        .map(|c| c.keys().map(|h| h.as_str().to_string()).collect())
        .unwrap_or_default()
}

/// Placements the storage holds in `collection`.
fn local_members(client: &ReplicaClient<MemStorage, MemRemote>, collection: &str) -> Vec<String> {
    client
        .storage()
        .placements
        .iter()
        .filter(|((c, _), _)| c.as_str() == collection)
        .map(|((_, h), _)| h.as_str().to_string())
        .collect()
}

/// Resolves the item's link id, the delivery key both halves of a move
/// check before delivering.
fn resolve_link(client: &mut ReplicaClient<MemStorage, MemRemote>) {
    client
        .upgrade("inbox", vec![ReplicaHandle::from("i1")], ReplicaTier::Meta)
        .unwrap();
}

fn stage_move(client: &mut ReplicaClient<MemStorage, MemRemote>) {
    client
        .mutate(
            "inbox",
            ReplicaMutation::Move {
                handle: ReplicaHandle::from("i1"),
                target: "archive".into(),
                placeholder: ReplicaHandle::from("tmp-i1"),
            },
        )
        .unwrap();
}

#[test]
fn a_move_synced_target_first_delivers_exactly_one_copy() {
    let mut client = seeded_client();
    let opts = ReplicaSyncOptions::default();

    client.sync("inbox", opts).unwrap();
    resolve_link(&mut client);
    stage_move(&mut client);

    // the target's copy lands first, then the source's remove
    client.sync("archive", opts).unwrap();
    client.sync("inbox", opts).unwrap();

    assert_eq!(
        remote_members(&client, "archive").len(),
        1,
        "the target holds exactly one member, not the copy and a move",
    );
    assert!(
        remote_members(&client, "inbox").is_empty(),
        "the source member is gone",
    );
    assert!(
        local_members(&client, "inbox").is_empty(),
        "the source tombstone is dropped once the remove is confirmed",
    );
    assert_eq!(
        local_members(&client, "archive").len(),
        1,
        "one target placement, no lingering placeholder",
    );
}

#[test]
fn a_move_synced_source_first_delivers_exactly_one_copy() {
    let mut client = seeded_client();
    let opts = ReplicaSyncOptions::default();

    client.sync("inbox", opts).unwrap();
    resolve_link(&mut client);
    stage_move(&mut client);

    // the source's remove lands first and relocates the member, so the
    // copy's origin is gone by the time the target syncs
    client.sync("inbox", opts).unwrap();
    client.sync("archive", opts).unwrap();

    assert_eq!(
        remote_members(&client, "archive").len(),
        1,
        "the relocation delivered it; the copy must not deliver a second",
    );
    assert!(remote_members(&client, "inbox").is_empty());
    // NOTE: the create can no longer copy from a relocated source, so it
    // stays visibly pending beside the member the remove delivered: an
    // add carries no key separating a second copy the user asked for from
    // one the remove already served.
    assert_eq!(
        client.storage().placement("archive", "tmp-i1").status,
        ReplicaStatus::Created,
    );
}

#[test]
fn a_copy_leaves_the_source_and_delivers_one_member() {
    let mut client = seeded_client();
    let opts = ReplicaSyncOptions::default();

    client.sync("inbox", opts).unwrap();
    client
        .mutate(
            "inbox",
            ReplicaMutation::Copy {
                handle: ReplicaHandle::from("i1"),
                target: "archive".into(),
                placeholder: ReplicaHandle::from("tmp-i1"),
            },
        )
        .unwrap();
    client.sync("archive", opts).unwrap();
    client.sync("inbox", opts).unwrap();

    assert_eq!(remote_members(&client, "archive").len(), 1);
    assert_eq!(
        remote_members(&client, "inbox").len(),
        1,
        "a copy leaves the source in place",
    );
    assert_eq!(
        client.storage().placement("inbox", "i1").status,
        ReplicaStatus::Clean,
    );
}

#[test]
fn a_move_of_a_never_fetched_item_delivers_exactly_one_copy() {
    // with no link id resolved neither half can recognise what the other
    // did, so only the source-side relocation is staged: it delivers in
    // either order, and the target picks the member up on its next
    // enumerate
    for target_first in [true, false] {
        let mut client = seeded_client();
        let opts = ReplicaSyncOptions::default();

        client.sync("inbox", opts).unwrap();
        stage_move(&mut client);

        if target_first {
            client.sync("archive", opts).unwrap();
            client.sync("inbox", opts).unwrap();
        } else {
            client.sync("inbox", opts).unwrap();
        }
        client.sync("archive", opts).unwrap();

        assert_eq!(
            remote_members(&client, "archive").len(),
            1,
            "target_first={target_first}",
        );
        assert!(remote_members(&client, "inbox").is_empty());
        assert_eq!(
            local_members(&client, "archive").len(),
            1,
            "no placeholder lingers: target_first={target_first}",
        );
    }
}
