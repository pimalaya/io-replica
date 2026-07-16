//! # Standard, blocking offline client
//!
//! A driver that runs any standard-shape coroutine to completion by
//! servicing each [`OfflineYield`] through two consumer-supplied traits:
//! [`OfflineStorage`] for the index and blob store, [`OfflineRemote`] for the protocol
//! seam. These traits live on the consumer side, not inside the engine,
//! so the I/O-free contract holds: the coroutines still only emit `Wants`.
//!
//! A desktop or Neverest consumer backs [`OfflineRemote`] with io-email's
//! blocking clients and [`OfflineStorage`] with sqlite plus a blob dir; an
//! Android consumer backs [`OfflineRemote`] with io-imap over JNI.

use std::{collections::BTreeMap, fmt, vec::Vec};

use crate::{
    change::{OfflineChange, OfflineWriteOp},
    collection::{OfflineCheckpoint, OfflineCollectionId},
    coroutine::*,
    mutate::{OfflineMutate, OfflineMutateError, OfflineMutation},
    object::OfflineHash,
    open::{OfflineOpen, OfflineOpenError},
    placement::{OfflineHandle, OfflineLinkId},
    rekey::{OfflineRekey, OfflineRekeyError, OfflineRekeyReport},
    remote::{OfflineFetchedItem, OfflinePushResult, OfflineRemoteSnapshot, OfflineTier},
    storage::OfflineLoaded,
    sync::{OfflineSync, OfflineSyncError, OfflineSyncOptions, OfflineSyncReport},
    upgrade::{OfflineUpgrade, OfflineUpgradeError, OfflineUpgradeReport},
};

/// The local index plus blob store seam.
pub trait OfflineStorage {
    /// The error this storage raises.
    type Error;

    /// Loads a collection's placements and checkpoint.
    fn load(&self, collection: &OfflineCollectionId) -> Result<OfflineLoaded, Self::Error>;

    /// Resolves which link ids already map to a stored object.
    fn lookup_objects(
        &self,
        links: &[OfflineLinkId],
    ) -> Result<BTreeMap<OfflineLinkId, OfflineHash>, Self::Error>;

    /// Applies a batch of writes atomically, maintaining the
    /// pointer-derived object references [`OfflineWriteOp`] documents.
    ///
    /// The engine assumes a single writer per collection between a load
    /// and the write derived from it: a batch applied over state another
    /// actor changed in between clobbers that change. How the guarantee
    /// is provided is the storage's business (a sqlite transaction, a
    /// lock file, process-level serialization).
    fn write(&mut self, ops: Vec<OfflineWriteOp>) -> Result<(), Self::Error>;
}

/// The remote protocol seam (IMAP, JMAP, WebDAV).
pub trait OfflineRemote {
    /// The error this remote raises.
    type Error;

    /// Enumerates the collection: a full set, or a delta from `cursor`.
    fn enumerate(
        &mut self,
        collection: &OfflineCollectionId,
        cursor: Option<OfflineCheckpoint>,
    ) -> Result<OfflineRemoteSnapshot, Self::Error>;

    /// Fetches each handle at the requested tier.
    fn fetch(
        &mut self,
        collection: &OfflineCollectionId,
        handles: Vec<OfflineHandle>,
        tier: OfflineTier,
    ) -> Result<Vec<OfflineFetchedItem>, Self::Error>;

    /// Pushes each change, returning a per-change outcome.
    ///
    /// Pushes are at-least-once: a crash between a serviced push and its
    /// recording write replays the change on the next sync, so the
    /// consumer keeps retries harmless (see [`OfflineChange`]).
    fn push(
        &mut self,
        collection: &OfflineCollectionId,
        changes: Vec<OfflineChange>,
    ) -> Result<Vec<OfflinePushResult>, Self::Error>;
}

/// Errors returned by [`OfflineClient`].
#[derive(Debug)]
pub enum OfflineClientError<S, R, C> {
    /// A storage seam call failed.
    OfflineStorage(S),
    /// A remote seam call failed.
    OfflineRemote(R),
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
            Self::OfflineStorage(err) => write!(f, "OfflineStorage seam failed: {err}"),
            Self::OfflineRemote(err) => write!(f, "OfflineRemote seam failed: {err}"),
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
    S: OfflineStorage,
    R: OfflineRemote,
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
                OfflineCoroutineState::Yielded(OfflineYield::WantsEnumerate {
                    collection,
                    cursor,
                }) => {
                    let snapshot = self
                        .remote
                        .enumerate(&collection, cursor)
                        .map_err(OfflineClientError::OfflineRemote)?;
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
                        .map_err(OfflineClientError::OfflineRemote)?;
                    arg = Some(OfflineArg::Fetch(items));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsPush {
                    collection,
                    changes,
                }) => {
                    let results = self
                        .remote
                        .push(&collection, changes)
                        .map_err(OfflineClientError::OfflineRemote)?;
                    arg = Some(OfflineArg::Push(results));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsLoad(collection)) => {
                    let loaded = self
                        .storage
                        .load(&collection)
                        .map_err(OfflineClientError::OfflineStorage)?;
                    arg = Some(OfflineArg::Load(loaded));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsLookupObject(links)) => {
                    let known = self
                        .storage
                        .lookup_objects(&links)
                        .map_err(OfflineClientError::OfflineStorage)?;
                    arg = Some(OfflineArg::LookupObject(known));
                }
                OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => {
                    self.storage
                        .write(ops)
                        .map_err(OfflineClientError::OfflineStorage)?;
                    arg = Some(OfflineArg::Write);
                }
            }
        }
    }

    /// Opens a collection fully offline.
    pub fn open(
        &mut self,
        collection: impl Into<OfflineCollectionId>,
    ) -> Result<OfflineLoaded, OfflineClientError<S::Error, R::Error, OfflineOpenError>> {
        self.run(OfflineOpen::new(collection))
    }

    /// Raises `handles` in `collection` to `tier`, deduping bodies.
    pub fn upgrade(
        &mut self,
        collection: impl Into<OfflineCollectionId>,
        handles: Vec<OfflineHandle>,
        tier: OfflineTier,
    ) -> Result<OfflineUpgradeReport, OfflineClientError<S::Error, R::Error, OfflineUpgradeError>>
    {
        self.run(OfflineUpgrade::new(collection, handles, tier))
    }

    /// Applies a local mutation with no network.
    pub fn mutate(
        &mut self,
        collection: impl Into<OfflineCollectionId>,
        mutation: OfflineMutation,
    ) -> Result<(), OfflineClientError<S::Error, R::Error, OfflineMutateError>> {
        self.run(OfflineMutate::new(collection, mutation))
    }

    /// Reconciles a collection with its remote.
    pub fn sync(
        &mut self,
        collection: impl Into<OfflineCollectionId>,
        opts: OfflineSyncOptions,
    ) -> Result<OfflineSyncReport, OfflineClientError<S::Error, R::Error, OfflineSyncError>> {
        self.run(OfflineSync::new(collection, opts))
    }

    /// Rebuilds a collection after a handle-space change (an IMAP
    /// UIDVALIDITY bump), carrying local state over by link id.
    pub fn rekey(
        &mut self,
        collection: impl Into<OfflineCollectionId>,
    ) -> Result<OfflineRekeyReport, OfflineClientError<S::Error, R::Error, OfflineRekeyError>> {
        self.run(OfflineRekey::new(collection))
    }
}
