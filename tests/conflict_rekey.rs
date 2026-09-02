//! A content conflict crossed with a handle-space rebuild.
//!
//! An IMAP UIDVALIDITY bump renumbers every handle at once, and the rekey
//! verb carries the local state over by link id. A conflicted placement
//! is the state with the most to lose there: it holds the local body, the
//! observed remote revision and the remote body at it, none of which the
//! new handle space carries. The tests below run both orders a consumer
//! can hit, the rekey before the resolution and after it, and assert the
//! conflict is neither resolved, dropped nor duplicated by the
//! renumbering.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::collections::BTreeMap;

use io_replica::{
    client::ReplicaClient,
    collection::ReplicaCheckpoint,
    mutate::ReplicaMutation,
    object::ReplicaObject,
    placement::{ReplicaHandle, ReplicaLinkId, ReplicaStatus},
    remote::ReplicaTier,
    sync::{ReplicaSyncOptions, ReplicaSyncReport},
};

use crate::common::{MemRemote, MemStorage, hash};

const BASE: &[u8] = b"the ancestor body";
const LOCAL: &[u8] = b"the local body";
const REMOTE: &[u8] = b"the remote body";
const CUSTOM: &[u8] = b"a hand-merged body";
const LATER: &[u8] = b"a later remote body";

type Client = ReplicaClient<MemStorage, MemRemote>;

/// A client whose inbox holds a conflicted member `m1` and a clean
/// bystander `m2`: the base holds `BASE`, the placement `LOCAL`, and the
/// remote `REMOTE` at the recorded conflict revision, with the diverging
/// body fetched into the store.
fn conflicted_client() -> Client {
    let mut remote = MemRemote::default();
    remote.mutable = true;
    remote.seed("inbox", "m1", "l1", &[], BASE);
    remote.seed("inbox", "m2", "l2", &["seen"], b"the bystander body");

    let mut client = ReplicaClient::new(MemStorage::default(), remote);
    let opts = ReplicaSyncOptions::default();
    client.sync("inbox", opts).unwrap();
    client
        .upgrade(
            "inbox",
            vec![ReplicaHandle::from("m1"), ReplicaHandle::from("m2")],
            ReplicaTier::Full,
        )
        .unwrap();

    edit(&mut client, "m1", LOCAL);
    client.remote_mut().edit("inbox", "m1", REMOTE);
    client.sync("inbox", opts).unwrap();

    // NOTE: the engine fetches nothing itself, so the upgrade pass is
    // what supplies the diverging body a resolver reads.
    client
        .upgrade("inbox", vec![ReplicaHandle::from("m1")], ReplicaTier::Full)
        .unwrap();

    let placement = client.storage().placement("inbox", "m1");
    assert_eq!(placement.status, ReplicaStatus::Conflict);
    assert_eq!(placement.conflict_revision.as_deref(), Some("1"));
    assert_eq!(placement.conflict_object, Some(hash(REMOTE)));

    client
}

fn edit(client: &mut Client, handle: &str, body: &[u8]) {
    let object = ReplicaObject {
        hash: hash(body),
        size: body.len(),
    };
    client
        .mutate(
            "inbox",
            ReplicaMutation::Edit {
                handle: ReplicaHandle::from(handle),
                object,
                body: body.to_vec(),
                meta: None,
                sort_key: None,
            },
        )
        .unwrap();
}

/// Renumbers the whole inbox onto a second handle generation and returns
/// the new handle of the conflicted member.
fn renumber(client: &mut Client) -> ReplicaHandle {
    let mapping: BTreeMap<ReplicaHandle, ReplicaHandle> = client.remote_mut().renumber("inbox", 2);
    mapping
        .get(&ReplicaHandle::from("m1"))
        .expect("the conflicted member is renumbered")
        .clone()
}

fn checkpoint(client: &Client) -> Option<ReplicaCheckpoint> {
    client.storage().checkpoints.get(&"inbox".into()).cloned()
}

/// The handles the replica holds for the inbox, so a renumbering that
/// duplicates or drops one is visible.
fn handles(client: &Client) -> Vec<String> {
    client
        .storage()
        .placements
        .keys()
        .filter(|(collection, _)| collection.as_str() == "inbox")
        .map(|(_, handle)| handle.as_str().to_string())
        .collect()
}

/// The body the server holds under `handle`. A pushed body reaches it as
/// its object hash, an uploaded one as its bytes.
fn server_body(client: &Client, handle: &ReplicaHandle) -> Vec<u8> {
    client
        .remote()
        .items
        .get(&"inbox".into())
        .and_then(|c| c.get(handle))
        .map(|item| item.body.clone())
        .expect("the server holds the member")
}

/// A renumbering carries a conflict whole. The three things one is made
/// of, the local body, the observed revision and the remote body at it,
/// all live in the old handle's row, and a rekey keeping the row but not
/// the pair would leave a conflict nobody can resolve. The spine moves
/// to the new generation once, and the move resolves nothing.
#[test]
fn a_rekey_carries_a_conflict_whole_onto_the_new_handle() {
    let mut client = conflicted_client();
    let before = checkpoint(&client);
    let new = renumber(&mut client);

    let report = client.rekey("inbox").unwrap();

    assert_eq!(report.rekeyed, 2, "both members carried over");
    assert_eq!(report.pulled, 0, "nothing read as a new arrival");
    assert_eq!(report.dropped, 0, "no pending state lost");

    let placement = client.storage().placement("inbox", new.as_str());
    assert_eq!(placement.status, ReplicaStatus::Conflict);
    assert_eq!(placement.link_id, Some(ReplicaLinkId::from("l1")));
    assert_eq!(placement.object, Some(hash(LOCAL)), "the local side");
    assert_eq!(placement.conflict_revision.as_deref(), Some("1"));
    assert_eq!(
        placement.conflict_object,
        Some(hash(REMOTE)),
        "the diverging body survives the renumbering",
    );
    assert_eq!(
        placement.base.as_ref().and_then(|b| b.object.clone()),
        Some(hash(BASE)),
        "and so does the ancestor the merge reconciles against",
    );

    let mut handles = handles(&client);
    handles.sort();
    assert_eq!(handles, vec!["v2-0".to_string(), "v2-1".to_string()]);
    assert_ne!(checkpoint(&client), before, "the checkpoint advanced");

    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    assert_eq!(report, ReplicaSyncReport::default(), "a settled spine");
    let placement = client.storage().placement("inbox", new.as_str());
    assert_eq!(placement.status, ReplicaStatus::Conflict);
    assert_eq!(placement.conflict_object, Some(hash(REMOTE)));
}

/// The rekey-then-resolve order: a consumer deciding after the handle
/// space moved settles the divergence, and its decision reaches the
/// server under the new handle.
#[test]
fn a_conflict_carried_by_a_rekey_still_resolves_to_the_remote() {
    let mut client = conflicted_client();
    let new = renumber(&mut client);
    client.rekey("inbox").unwrap();

    edit(&mut client, new.as_str(), CUSTOM);
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    assert_eq!(report.pushed, 1);
    assert_eq!(report.conflicts, 0, "the divergence is settled, not re-run");
    assert_eq!(
        server_body(&client, &new),
        hash(CUSTOM).as_str().as_bytes(),
        "the resolution reached the remote",
    );
    assert_eq!(
        client.storage().placement("inbox", new.as_str()).status,
        ReplicaStatus::Clean,
    );
}

/// The resolve-then-rekey order: a decision taken while the old handle
/// space was still live survives the renumbering as the pending push it
/// is, keeps the remote state it settled as its base, and is neither
/// undone nor conflicted anew.
#[test]
fn a_resolution_survives_a_rekey_and_still_reaches_the_remote() {
    let mut client = conflicted_client();
    edit(&mut client, "m1", CUSTOM);
    let new = renumber(&mut client);

    let report = client.rekey("inbox").unwrap();
    assert_eq!(report.rekeyed, 2);
    assert_eq!(
        report.dropped, 0,
        "the resolution is not pending state lost"
    );

    let placement = client.storage().placement("inbox", new.as_str());
    assert_eq!(placement.status, ReplicaStatus::Dirty, "a pending push");
    assert_eq!(placement.object, Some(hash(CUSTOM)));
    assert_eq!(placement.conflict_revision, None, "nothing left to resolve");
    assert_eq!(placement.conflict_object, None);
    assert_eq!(
        placement.base.as_ref().and_then(|b| b.object.clone()),
        Some(hash(REMOTE)),
        "the base is still the state the resolution settled",
    );

    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    assert_eq!(report.pushed, 1);
    assert_eq!(report.conflicts, 0);
    assert_eq!(
        server_body(&client, &new),
        hash(CUSTOM).as_str().as_bytes(),
        "the resolution reached the remote across the renumbering",
    );
}

/// The stored diverging body describes the revision recorded beside it,
/// so a rekey observing a newer one keeps the conflict and drops the
/// body, the upgrade pass then answering the request that leaves. A
/// resolver merging against bytes the remote no longer holds would be
/// deciding against the wrong version.
#[test]
fn a_rekey_over_a_newer_revision_keeps_the_conflict_and_re_asks_for_the_body() {
    let mut client = conflicted_client();
    client.remote_mut().edit("inbox", "m1", LATER);
    let new = renumber(&mut client);

    client.rekey("inbox").unwrap();

    let placement = client.storage().placement("inbox", new.as_str());
    assert_eq!(placement.status, ReplicaStatus::Conflict);
    assert_eq!(placement.conflict_revision.as_deref(), Some("2"));
    assert_eq!(placement.conflict_object, None, "the stale body is dropped");
    assert_eq!(placement.object, Some(hash(LOCAL)), "the local side stays");

    client
        .upgrade("inbox", vec![new.clone()], ReplicaTier::Full)
        .unwrap();
    let placement = client.storage().placement("inbox", new.as_str());
    assert_eq!(placement.status, ReplicaStatus::Conflict);
    assert_eq!(placement.conflict_object, Some(hash(LATER)));
}
