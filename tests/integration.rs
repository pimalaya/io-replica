//! End-to-end concept validation with an in-memory storage and a fake
//! remote.
//!
//! Exercises the whole lifecycle on a generic collection of items: initial
//! pull, fully offline open, progressive upgrade with cross-collection
//! object dedup (one item present in two collections), local mutation,
//! push, remote pull, and a divergent flag merge where both sides survive.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use io_replica::{
    client::ReplicaClient,
    mutate::ReplicaMutation,
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaStatus},
    remote::ReplicaTier,
    sync::ReplicaSyncOptions,
};

use crate::common::{MemRemote, MemStorage};

fn seeded_client() -> ReplicaClient<MemStorage, MemRemote> {
    let body_a = b"From: a\r\nMessage-ID: <msg-a>\r\n\r\nshared body\r\n";
    let body_b = b"From: b\r\nMessage-ID: <msg-b>\r\n\r\nother body\r\n";

    let mut remote = MemRemote::default();
    // NOTE: inbox/i1 and archive/a1 are the SAME logical item (link msg-a,
    // identical body) present in two collections: the dedup case
    remote.seed("inbox", "i1", "msg-a", &[], body_a);
    remote.seed("inbox", "i2", "msg-b", &[], body_b);
    remote.seed("archive", "a1", "msg-a", &["seen"], body_a);

    ReplicaClient::new(MemStorage::default(), remote)
}

#[test]
fn full_offline_lifecycle() {
    let mut client = seeded_client();

    // 1. initial sync pulls a complete probed spine
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    assert_eq!(report.pulled, 2, "both inbox items pulled");
    assert_eq!(report.pushed, 0);

    // 2. open is fully offline: it must touch storage only, never the
    // remote
    let calls_before = client.remote().calls;
    let loaded = client.open("inbox").unwrap();
    assert_eq!(loaded.placements.len(), 2);
    assert!(
        loaded
            .placements
            .iter()
            .all(|p| p.level == ReplicaLevel::Probed)
    );
    assert_eq!(
        client.remote().calls,
        calls_before,
        "open must not hit the remote",
    );

    // 3. progressive upgrade: headers, then one body
    let report = client
        .upgrade(
            "inbox",
            vec![ReplicaHandle::from("i1"), ReplicaHandle::from("i2")],
            ReplicaTier::Meta,
        )
        .unwrap();
    assert_eq!(report.upgraded, 2);
    assert_eq!(
        client.storage().placement("inbox", "i1").level,
        ReplicaLevel::Meta
    );

    let report = client
        .upgrade("inbox", vec![ReplicaHandle::from("i1")], ReplicaTier::Full)
        .unwrap();
    assert_eq!(report.fetched, 1);
    assert_eq!(report.deduped, 0);
    assert_eq!(
        client.storage().placement("inbox", "i1").level,
        ReplicaLevel::Full
    );
    assert_eq!(client.storage().objects.len(), 1, "one stored body");

    // 4. second collection, same logical item: upgrading its body must
    // dedup against the already-stored object, with zero new fetch.
    // ReplicaMeta first resolves a1's link id (enumerate does not carry it), then
    // the Full upgrade links the shared body by that link id.
    client
        .sync("archive", ReplicaSyncOptions::default())
        .unwrap();
    client
        .upgrade(
            "archive",
            vec![ReplicaHandle::from("a1")],
            ReplicaTier::Meta,
        )
        .unwrap();
    let fetches_before = client.remote().full_fetches.len();

    let report = client
        .upgrade(
            "archive",
            vec![ReplicaHandle::from("a1")],
            ReplicaTier::Full,
        )
        .unwrap();
    assert_eq!(report.deduped, 1, "shared body deduped");
    assert_eq!(report.fetched, 0);
    assert_eq!(
        client.remote().full_fetches.len(),
        fetches_before,
        "dedup must skip the network fetch",
    );
    assert_eq!(client.storage().objects.len(), 1, "still one stored body");
    // the two placements share one object but keep distinct flags
    assert_eq!(
        client.storage().placement("inbox", "i1").object,
        client.storage().placement("archive", "a1").object,
    );

    // 5. local mutation is offline, then pushed on sync
    client
        .mutate(
            "inbox",
            ReplicaMutation::SetFlags {
                handle: ReplicaHandle::from("i1"),
                flags: ReplicaFlags::from_iter(["seen"]),
            },
        )
        .unwrap();
    assert_eq!(
        client.storage().placement("inbox", "i1").status,
        ReplicaStatus::Dirty,
    );

    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    assert_eq!(report.pushed, 1, "local seen flag pushed");
    assert!(client.remote().flags_of("inbox", "i1").contains("seen"));
    assert_eq!(
        client.storage().placement("inbox", "i1").status,
        ReplicaStatus::Clean,
        "pushed placement is rebased clean",
    );

    // 6. a remote-side change is pulled on the next sync
    client.remote_mut().set_flags("inbox", "i2", &["flagged"]);
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    assert_eq!(report.pulled, 1);
    assert!(
        client
            .storage()
            .placement("inbox", "i2")
            .flags
            .contains("flagged")
    );

    // 7. divergent flag edits on both sides merge element-wise: the local
    // removal of "seen" and addition of "draft" and the remote addition of
    // "important" all survive, with no conflict and no silent loss
    client
        .mutate(
            "inbox",
            ReplicaMutation::SetFlags {
                handle: ReplicaHandle::from("i1"),
                flags: ReplicaFlags::from_iter(["draft"]),
            },
        )
        .unwrap();
    client
        .remote_mut()
        .set_flags("inbox", "i1", &["seen", "important"]);

    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    assert_eq!(report.conflicts, 0, "flags never conflict");
    assert_eq!(report.pushed, 1);
    assert_eq!(report.pulled, 1);

    let merged = &client.storage().placement("inbox", "i1").flags;
    assert!(merged.contains("draft"), "the local addition survives");
    assert!(merged.contains("important"), "the remote addition survives");
    assert!(!merged.contains("seen"), "the local removal wins");
    assert_eq!(
        client.remote().flags_of("inbox", "i1"),
        merged,
        "both sides converged on the merged set",
    );
    assert_eq!(
        client.storage().placement("inbox", "i1").status,
        ReplicaStatus::Clean,
    );
}

#[test]
fn offline_copy_creates_pushes_and_rekeys() {
    let mut client = seeded_client();
    let opts = ReplicaSyncOptions::default();

    // Know the inbox spine, so inbox/i2 is a real placement to copy.
    client.sync("inbox", opts).unwrap();

    // Copy inbox/i2 into archive offline: a Created placeholder is staged in
    // archive carrying its origin, and the source is left untouched.
    client
        .mutate(
            "inbox",
            ReplicaMutation::Copy {
                handle: ReplicaHandle::from("i2"),
                target: "archive".into(),
                placeholder: ReplicaHandle::from("tmp-i2"),
            },
        )
        .unwrap();
    let staged = client.storage().placement("archive", "tmp-i2");
    assert_eq!(staged.status, ReplicaStatus::Created);
    assert!(staged.origin.is_some());
    assert_eq!(
        client.storage().placement("inbox", "i2").status,
        ReplicaStatus::Clean,
        "the copy source is untouched",
    );

    // Syncing archive pushes the create (a server-side copy) and rekeys the
    // placeholder to the server-assigned handle, clean and based.
    let report = client.sync("archive", opts).unwrap();
    assert_eq!(report.pushed, 1);
    assert!(
        !client
            .storage()
            .placements
            .contains_key(&("archive".into(), ReplicaHandle::from("tmp-i2"))),
        "the placeholder is dropped once the copy is confirmed",
    );
    let real = client.storage().placement("archive", "i2-copy");
    assert_eq!(real.status, ReplicaStatus::Clean);
    assert!(real.base.is_some());
    assert!(real.origin.is_none());
}
