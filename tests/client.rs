//! Client seam coverage: error propagation from each seam and the
//! error display shapes.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::collections::BTreeMap;

use io_replica::{
    change::{ReplicaChange, ReplicaWriteOp},
    client::{ReplicaClient, ReplicaClientError, ReplicaRemote, ReplicaStorage},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    mutate::ReplicaMutation,
    object::ReplicaHash,
    placement::{ReplicaHandle, ReplicaLinkId},
    remote::{ReplicaFetchedItem, ReplicaPushResult, ReplicaRemoteSnapshot, ReplicaTier},
    storage::{ReplicaLoadScope, ReplicaLoaded},
    sync::ReplicaSyncOptions,
};

use crate::common::{MemRemote, MemStorage};

/// A storage whose every call fails.
struct BrokenStorage;

impl ReplicaStorage for BrokenStorage {
    type Error = &'static str;

    fn load(
        &self,
        _: &ReplicaCollectionId,
        _: &ReplicaLoadScope,
    ) -> Result<ReplicaLoaded, Self::Error> {
        Err("disk on fire")
    }

    fn lookup_objects(
        &self,
        _: &[ReplicaLinkId],
    ) -> Result<BTreeMap<ReplicaLinkId, ReplicaHash>, Self::Error> {
        Err("disk on fire")
    }

    fn write(&mut self, _: Vec<ReplicaWriteOp>) -> Result<(), Self::Error> {
        Err("disk on fire")
    }
}

/// A remote whose every call fails.
struct BrokenRemote;

impl ReplicaRemote for BrokenRemote {
    type Error = &'static str;

    fn enumerate(
        &mut self,
        _: &ReplicaCollectionId,
        _: Option<ReplicaCheckpoint>,
    ) -> Result<ReplicaRemoteSnapshot, Self::Error> {
        Err("network unplugged")
    }

    fn fetch(
        &mut self,
        _: &ReplicaCollectionId,
        _: Vec<ReplicaHandle>,
        _: ReplicaTier,
    ) -> Result<Vec<ReplicaFetchedItem>, Self::Error> {
        Err("network unplugged")
    }

    fn push(
        &mut self,
        _: &ReplicaCollectionId,
        _: Vec<ReplicaChange>,
    ) -> Result<Vec<ReplicaPushResult>, Self::Error> {
        Err("network unplugged")
    }
}

#[test]
fn storage_error_propagates() {
    let mut client = ReplicaClient::new(BrokenStorage, MemRemote::default());

    let err = client.open("inbox").unwrap_err();
    assert!(matches!(err, ReplicaClientError::Storage("disk on fire")));
    assert_eq!(err.to_string(), "Storage seam failed: disk on fire");
}

#[test]
fn remote_error_propagates() {
    let mut client = ReplicaClient::new(MemStorage::default(), BrokenRemote);

    let err = client
        .sync("inbox", ReplicaSyncOptions::default())
        .unwrap_err();
    assert!(matches!(
        err,
        ReplicaClientError::Remote("network unplugged")
    ));
    assert_eq!(err.to_string(), "Remote seam failed: network unplugged");
}

#[test]
fn coroutine_error_propagates() {
    let mut client = ReplicaClient::new(MemStorage::default(), MemRemote::default());

    let err = client
        .mutate(
            "inbox",
            ReplicaMutation::Remove(ReplicaHandle::from("nope")),
        )
        .unwrap_err();
    assert!(matches!(err, ReplicaClientError::Coroutine(_)));
    assert_eq!(
        err.to_string(),
        "Replica engine failed: Replica MUTATE failed: unknown handle nope",
    );
}

#[test]
fn seams_are_borrowable_both_ways() {
    let mut client = ReplicaClient::new(MemStorage::default(), MemRemote::default());

    client.remote_mut().seed("inbox", "1", "msg-1", &[], b"x");
    client
        .storage_mut()
        .checkpoints
        .insert("inbox".into(), ReplicaCheckpoint(b"cp".to_vec()));

    assert_eq!(client.remote().calls, 0);
    assert_eq!(client.storage().checkpoints.len(), 1);
}
