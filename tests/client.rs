//! Std-client seam coverage: error propagation from each seam and the
//! error display shapes.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::collections::BTreeMap;

use io_offline::{
    change::{Change, WriteOp},
    client::{OfflineClient, OfflineClientError, Remote, Storage},
    collection::{Checkpoint, CollectionId},
    mutate::Mutation,
    object::Hash,
    placement::{Handle, LinkId},
    remote::{FetchedItem, PushResult, RemoteSnapshot, Tier},
    storage::Loaded,
    sync::OfflineSyncOptions,
};

use crate::common::{MemRemote, MemStorage};

/// A storage whose every call fails.
struct BrokenStorage;

impl Storage for BrokenStorage {
    type Error = &'static str;

    fn load(&self, _: &CollectionId) -> Result<Loaded, Self::Error> {
        Err("disk on fire")
    }

    fn lookup_objects(&self, _: &[LinkId]) -> Result<BTreeMap<LinkId, Hash>, Self::Error> {
        Err("disk on fire")
    }

    fn write(&mut self, _: Vec<WriteOp>) -> Result<(), Self::Error> {
        Err("disk on fire")
    }
}

/// A remote whose every call fails.
struct BrokenRemote;

impl Remote for BrokenRemote {
    type Error = &'static str;

    fn enumerate(
        &mut self,
        _: &CollectionId,
        _: Option<Checkpoint>,
    ) -> Result<RemoteSnapshot, Self::Error> {
        Err("network unplugged")
    }

    fn fetch(
        &mut self,
        _: &CollectionId,
        _: Vec<Handle>,
        _: Tier,
    ) -> Result<Vec<FetchedItem>, Self::Error> {
        Err("network unplugged")
    }

    fn push(&mut self, _: &CollectionId, _: Vec<Change>) -> Result<Vec<PushResult>, Self::Error> {
        Err("network unplugged")
    }
}

#[test]
fn storage_error_propagates() {
    let mut client = OfflineClient::new(BrokenStorage, MemRemote::default());

    let err = client.open("inbox").unwrap_err();
    assert!(matches!(err, OfflineClientError::Storage("disk on fire")));
    assert_eq!(err.to_string(), "Storage seam failed: disk on fire");
}

#[test]
fn remote_error_propagates() {
    let mut client = OfflineClient::new(MemStorage::default(), BrokenRemote);

    let err = client
        .sync("inbox", OfflineSyncOptions::default())
        .unwrap_err();
    assert!(matches!(
        err,
        OfflineClientError::Remote("network unplugged")
    ));
    assert_eq!(err.to_string(), "Remote seam failed: network unplugged");
}

#[test]
fn coroutine_error_propagates() {
    let mut client = OfflineClient::new(MemStorage::default(), MemRemote::default());

    let err = client
        .mutate("inbox", Mutation::Remove(Handle::from("nope")))
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
        .insert("inbox".into(), Checkpoint(b"cp".to_vec()));

    assert_eq!(client.remote().calls, 0);
    assert_eq!(client.storage().checkpoints.len(), 1);
}
