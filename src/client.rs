//! # Standard, blocking offline client
//!
//! A driver that runs any standard-shape coroutine to completion by
//! servicing each [`OfflineYield`] through two consumer-supplied traits:
//! [`Storage`] for the index and blob store, [`Remote`] for the protocol
//! seam. These traits live on the consumer side, not inside the engine,
//! so the I/O-free contract holds: the coroutines still only emit `Wants`.
//!
//! A desktop or Neverest consumer backs [`Remote`] with io-email's
//! blocking clients and [`Storage`] with sqlite plus a blob dir; an
//! Android consumer backs [`Remote`] with io-imap over JNI.

use std::{collections::BTreeMap, fmt, vec::Vec};

use crate::{
    change::{Change, WriteOp},
    collection::{Checkpoint, CollectionId},
    coroutine::*,
    mutate::{Mutation, OfflineMutate, OfflineMutateError},
    object::Hash,
    open::{OfflineOpen, OfflineOpenError},
    placement::{Handle, LinkId},
    remote::{FetchedItem, PushResult, RemoteSnapshot, Tier},
    storage::Loaded,
    sync::{OfflineSync, OfflineSyncError, OfflineSyncOptions, OfflineSyncReport},
    upgrade::{OfflineUpgrade, OfflineUpgradeError, OfflineUpgradeReport},
};

/// The local index plus blob store seam.
pub trait Storage {
    /// The error this storage raises.
    type Error;

    /// Loads a collection's placements and checkpoint.
    fn load(&self, collection: &CollectionId) -> Result<Loaded, Self::Error>;

    /// Resolves which link ids already map to a stored object.
    fn lookup_objects(&self, links: &[LinkId]) -> Result<BTreeMap<LinkId, Hash>, Self::Error>;

    /// Applies a batch of writes atomically.
    fn write(&mut self, ops: Vec<WriteOp>) -> Result<(), Self::Error>;
}

/// The remote protocol seam (IMAP, JMAP, WebDAV).
pub trait Remote {
    /// The error this remote raises.
    type Error;

    /// Reports the remote member count, when the protocol offers one
    /// cheaply.
    fn count(&mut self, collection: &CollectionId) -> Result<usize, Self::Error>;

    /// Enumerates the collection: a full set, or a delta from `cursor`.
    fn enumerate(
        &mut self,
        collection: &CollectionId,
        cursor: Option<Checkpoint>,
    ) -> Result<RemoteSnapshot, Self::Error>;

    /// Fetches each handle at the requested tier.
    fn fetch(
        &mut self,
        collection: &CollectionId,
        handles: Vec<Handle>,
        tier: Tier,
    ) -> Result<Vec<FetchedItem>, Self::Error>;

    /// Pushes each change, returning a per-change outcome.
    fn push(
        &mut self,
        collection: &CollectionId,
        changes: Vec<Change>,
    ) -> Result<Vec<PushResult>, Self::Error>;
}

/// Errors returned by [`OfflineClient`].
#[derive(Debug)]
pub enum OfflineClientError<S, R, C> {
    /// A storage seam call failed.
    Storage(S),
    /// A remote seam call failed.
    Remote(R),
    /// The coroutine itself completed with an error.
    Coroutine(C),
}

impl<S, R, C> fmt::Display for OfflineClientError<S, R, C>
where
    S: fmt::Display,
    R: fmt::Display,
    C: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "Storage seam failed: {err}"),
            Self::Remote(err) => write!(f, "Remote seam failed: {err}"),
            Self::Coroutine(err) => write!(f, "Offline engine failed: {err}"),
        }
    }
}

impl<S, R, C> std::error::Error for OfflineClientError<S, R, C>
where
    S: fmt::Display + fmt::Debug,
    R: fmt::Display + fmt::Debug,
    C: fmt::Display + fmt::Debug,
{
}

/// Std-blocking offline client wrapping a storage and a remote.
pub struct OfflineClient<S, R> {
    storage: S,
    remote: R,
}

impl<S, R> OfflineClient<S, R>
where
    S: Storage,
    R: Remote,
{
    /// Builds a client over a storage and a remote.
    pub fn new(storage: S, remote: R) -> Self {
        Self { storage, remote }
    }

    /// Borrows the storage seam.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Borrows the remote seam.
    pub fn remote(&self) -> &R {
        &self.remote
    }

    /// Mutably borrows the storage seam.
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    /// Mutably borrows the remote seam.
    pub fn remote_mut(&mut self) -> &mut R {
        &mut self.remote
    }

    /// Drives any standard-shape coroutine to completion, servicing each
    /// yield through the storage and remote seams.
    pub fn run<C, T, E>(
        &mut self,
        mut coroutine: C,
    ) -> Result<T, OfflineClientError<S::Error, R::Error, E>>
    where
        C: OfflineCoroutine<Yield = OfflineYield, Return = Result<T, E>>,
    {
        let mut arg: Option<OfflineArg> = None;

        loop {
            match coroutine.resume(arg.take()) {
                OfflineCoroutineState::Complete(Ok(out)) => return Ok(out),
                OfflineCoroutineState::Complete(Err(err)) => {
                    return Err(OfflineClientError::Coroutine(err));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsCount(collection)) => {
                    let count = self
                        .remote
                        .count(&collection)
                        .map_err(OfflineClientError::Remote)?;
                    arg = Some(OfflineArg::Count(count));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsEnumerate {
                    collection,
                    cursor,
                }) => {
                    let snapshot = self
                        .remote
                        .enumerate(&collection, cursor)
                        .map_err(OfflineClientError::Remote)?;
                    arg = Some(OfflineArg::Enumerate(snapshot));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsFetch {
                    collection,
                    handles,
                    tier,
                }) => {
                    let items = self
                        .remote
                        .fetch(&collection, handles, tier)
                        .map_err(OfflineClientError::Remote)?;
                    arg = Some(OfflineArg::Fetch(items));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsPush {
                    collection,
                    changes,
                }) => {
                    let results = self
                        .remote
                        .push(&collection, changes)
                        .map_err(OfflineClientError::Remote)?;
                    arg = Some(OfflineArg::Push(results));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsLoad(collection)) => {
                    let loaded = self
                        .storage
                        .load(&collection)
                        .map_err(OfflineClientError::Storage)?;
                    arg = Some(OfflineArg::Load(loaded));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsLookupObject(links)) => {
                    let known = self
                        .storage
                        .lookup_objects(&links)
                        .map_err(OfflineClientError::Storage)?;
                    arg = Some(OfflineArg::LookupObject(known));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => {
                    self.storage
                        .write(ops)
                        .map_err(OfflineClientError::Storage)?;
                    arg = Some(OfflineArg::Write);
                }
            }
        }
    }

    /// Opens a collection fully offline.
    pub fn open(
        &mut self,
        collection: impl Into<CollectionId>,
    ) -> Result<Loaded, OfflineClientError<S::Error, R::Error, OfflineOpenError>> {
        self.run(OfflineOpen::new(collection))
    }

    /// Raises `handles` in `collection` to `tier`, deduping bodies.
    pub fn upgrade(
        &mut self,
        collection: impl Into<CollectionId>,
        handles: Vec<Handle>,
        tier: Tier,
    ) -> Result<OfflineUpgradeReport, OfflineClientError<S::Error, R::Error, OfflineUpgradeError>>
    {
        self.run(OfflineUpgrade::new(collection, handles, tier))
    }

    /// Applies a local mutation with no network.
    pub fn mutate(
        &mut self,
        collection: impl Into<CollectionId>,
        mutation: Mutation,
    ) -> Result<(), OfflineClientError<S::Error, R::Error, OfflineMutateError>> {
        self.run(OfflineMutate::new(collection, mutation))
    }

    /// Reconciles a collection with its remote.
    pub fn sync(
        &mut self,
        collection: impl Into<CollectionId>,
        opts: OfflineSyncOptions,
    ) -> Result<OfflineSyncReport, OfflineClientError<S::Error, R::Error, OfflineSyncError>> {
        self.run(OfflineSync::new(collection, opts))
    }
}
