//! A run pushes and records in chunks, so a lost write costs one chunk
//! rather than every push the run derived.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::collections::BTreeMap;

use io_replica::{
    change::ReplicaWriteOp,
    client::{ReplicaClient, ReplicaStorage},
    collection::ReplicaCollectionId,
    mutate::ReplicaMutation,
    object::ReplicaHash,
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaStatus},
    storage::{ReplicaLoadScope, ReplicaLoaded},
    sync::{ReplicaSync, ReplicaSyncOptions},
};

use crate::common::{MemRemote, MemStorage};

/// A storage that counts the write batches it is handed and drops the one
/// at `crash_at`, which is what a crash between a serviced push and its
/// recording write looks like from inside a run.
struct CrashyStorage {
    inner: MemStorage,
    batches: usize,
    crash_at: Option<usize>,
}

impl ReplicaStorage for CrashyStorage {
    type Error = &'static str;

    fn load(
        &self,
        collection: &ReplicaCollectionId,
        scope: &ReplicaLoadScope,
    ) -> Result<ReplicaLoaded, Self::Error> {
        Ok(self.inner.load(collection, scope).unwrap())
    }

    fn lookup_objects(
        &self,
        links: &[ReplicaLinkId],
    ) -> Result<BTreeMap<ReplicaLinkId, ReplicaHash>, Self::Error> {
        Ok(self.inner.lookup_objects(links).unwrap())
    }

    fn write(&mut self, ops: Vec<ReplicaWriteOp>) -> Result<(), Self::Error> {
        self.batches += 1;

        if self.crash_at == Some(self.batches) {
            return Err("crashed before the write landed");
        }

        self.inner.write(ops).unwrap();
        Ok(())
    }
}

/// One more member than a chunk and a half holds, so a run over them
/// derives a full chunk plus a partial one.
const EXTRA: usize = 3;
const MEMBERS: usize = ReplicaSync::PUSH_CHUNK + EXTRA;

fn handle(index: usize) -> String {
    format!("{index:03}")
}

/// A replica of `MEMBERS` synced members, every one carrying a local flag
/// edit: one pending push each.
fn dirty_client() -> ReplicaClient<CrashyStorage, MemRemote> {
    let mut remote = MemRemote::default();
    for index in 0..MEMBERS {
        let handle = handle(index);
        remote.seed("inbox", &handle, &handle, &[], handle.as_bytes());
    }

    let storage = CrashyStorage {
        inner: MemStorage::default(),
        batches: 0,
        crash_at: None,
    };
    let mut client = ReplicaClient::new(storage, remote);
    client.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    for index in 0..MEMBERS {
        client
            .mutate(
                "inbox",
                ReplicaMutation::SetFlags {
                    handle: ReplicaHandle::from(handle(index)),
                    flags: ReplicaFlags::from_iter(["seen"]),
                },
            )
            .unwrap();
    }

    client.remote_mut().push_batches.clear();
    client.storage_mut().batches = 0;
    client
}

fn status(client: &ReplicaClient<CrashyStorage, MemRemote>, index: usize) -> ReplicaStatus {
    client
        .storage()
        .inner
        .placement("inbox", &handle(index))
        .status
}

#[test]
fn a_run_pushes_and_records_one_chunk_at_a_time() {
    let mut client = dirty_client();
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    assert_eq!(report.pushed, MEMBERS);
    assert_eq!(
        client.remote().push_batches,
        [ReplicaSync::PUSH_CHUNK, EXTRA],
        "the changes must go out in chunks",
    );
    assert_eq!(
        client.storage().batches,
        2,
        "each chunk is recorded by its own write",
    );
    for index in 0..MEMBERS {
        assert_eq!(status(&client, index), ReplicaStatus::Clean);
        assert!(
            client
                .remote()
                .flags_of("inbox", &handle(index))
                .contains("seen")
        );
    }
}

#[test]
fn a_lost_write_costs_its_own_chunk_only() {
    let mut client = dirty_client();
    client.storage_mut().crash_at = Some(2);

    client
        .sync("inbox", ReplicaSyncOptions::default())
        .expect_err("the second chunk's write is lost");

    // the first chunk was recorded before the second was pushed, so only
    // the second is still pending: the crash window is one chunk.
    for index in 0..ReplicaSync::PUSH_CHUNK {
        assert_eq!(status(&client, index), ReplicaStatus::Clean, "chunk 1 lost");
    }
    for index in ReplicaSync::PUSH_CHUNK..MEMBERS {
        assert_eq!(status(&client, index), ReplicaStatus::Dirty, "chunk 2 kept");
    }

    // and the replica still converges: nothing was lost on either side.
    client.storage_mut().crash_at = None;
    client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    for index in 0..MEMBERS {
        assert_eq!(status(&client, index), ReplicaStatus::Clean);
        assert!(
            client
                .remote()
                .flags_of("inbox", &handle(index))
                .contains("seen")
        );
    }
}
