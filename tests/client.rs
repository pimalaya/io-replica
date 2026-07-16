//! Std-client seam coverage: error propagation from each seam and the
//! error display shapes.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::collections::BTreeMap;

use io_offline::{
    change::{OfflineChange, OfflineWriteOp},
    client::{OfflineClient, OfflineClientError, OfflineRemote, OfflineStorage},
    collection::{OfflineCheckpoint, OfflineCollectionId},
    mutate::OfflineMutation,
    object::OfflineHash,
    placement::{OfflineHandle, OfflineLinkId},
    remote::{OfflineFetchedItem, OfflinePushResult, OfflineRemoteSnapshot, OfflineTier},
    storage::OfflineLoaded,
    sync::OfflineSyncOptions,
};

use crate::common::{MemRemote, MemStorage};

/// A storage whose every call fails.
struct BrokenStorage;

impl OfflineStorage for BrokenStorage {
    type Error = &'static str;

    fn load(&self, _: &OfflineCollectionId) -> Result<OfflineLoaded, Self::Error> {
        Err("disk on fire")
    }

    fn lookup_objects(
        &self,
        _: &[OfflineLinkId],
    ) -> Result<BTreeMap<OfflineLinkId, OfflineHash>, Self::Error> {
        Err("disk on fire")
    }

    fn write(&mut self, _: Vec<OfflineWriteOp>) -> Result<(), Self::Error> {
        Err("disk on fire")
    }
}

/// A remote whose every call fails.
struct BrokenRemote;

impl OfflineRemote for BrokenRemote {
    type Error = &'static str;

    fn enumerate(
        &mut self,
        _: &OfflineCollectionId,
        _: Option<OfflineCheckpoint>,
    ) -> Result<OfflineRemoteSnapshot, Self::Error> {
        Err("network unplugged")
    }

    fn fetch(
        &mut self,
        _: &OfflineCollectionId,
        _: Vec<OfflineHandle>,
        _: OfflineTier,
    ) -> Result<Vec<OfflineFetchedItem>, Self::Error> {
        Err("network unplugged")
    }

    fn push(
        &mut self,
        _: &OfflineCollectionId,
        _: Vec<OfflineChange>,
    ) -> Result<Vec<OfflinePushResult>, Self::Error> {
        Err("network unplugged")
    }
}

#[test]
fn storage_error_propagates() {
    let mut client = OfflineClient::new(BrokenStorage, MemRemote::default());

    let err = client.open("inbox").unwrap_err();
    assert!(matches!(
        err,
        OfflineClientError::OfflineStorage("disk on fire")
    ));
    assert_eq!(err.to_string(), "OfflineStorage seam failed: disk on fire");
}

#[test]
fn remote_error_propagates() {
    let mut client = OfflineClient::new(MemStorage::default(), BrokenRemote);

    let err = client
        .sync("inbox", OfflineSyncOptions::default())
        .unwrap_err();
    assert!(matches!(
        err,
        OfflineClientError::OfflineRemote("network unplugged")
    ));
    assert_eq!(
        err.to_string(),
        "OfflineRemote seam failed: network unplugged"
    );
}

#[test]
fn coroutine_error_propagates() {
    let mut client = OfflineClient::new(MemStorage::default(), MemRemote::default());

    let err = client
        .mutate(
            "inbox",
            OfflineMutation::Remove(OfflineHandle::from("nope")),
        )
        .unwrap_err();
    assert!(matches!(err, OfflineClientError::Coroutine(_)));
    assert_eq!(
        err.to_string(),
        "Offline engine failed: Offline MUTATE failed: unknown handle nope",
    );
}

#[test]
fn seams_are_borrowable_both_ways() {
    let mut client = OfflineClient::new(MemStorage::default(), MemRemote::default());

    client.remote_mut().seed("inbox", "1", "msg-1", &[], b"x");
    client
        .storage_mut()
        .checkpoints
        .insert("inbox".into(), OfflineCheckpoint(b"cp".to_vec()));

    assert_eq!(client.remote().calls, 0);
    assert_eq!(client.storage().checkpoints.len(), 1);
}
