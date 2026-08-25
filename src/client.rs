//! # Blocking reference driver
//!
//! A driver that runs any standard-shape coroutine to completion by
//! servicing each [`ReplicaYield`] through two consumer-supplied traits:
//! [`ReplicaStorage`] for the index and blob store, [`ReplicaRemote`] for the
//! protocol seam. These traits live on the consumer side, not inside the
//! engine, so the I/O-free contract holds: the coroutines still only emit
//! `Wants`. The driver itself performs no I/O either, so it needs no std
//! and no feature gate: blocking happens inside the trait impls.
//!
//! A desktop or Neverest consumer backs [`ReplicaRemote`] with a blocking
//! protocol client (io-imap, io-jmap, io-webdav) and [`ReplicaStorage`] with
//! sqlite plus a blob dir; an Android consumer backs [`ReplicaRemote`] with
//! io-imap over JNI.

use core::{error, fmt};

use alloc::{collections::BTreeMap, vec::Vec};

use crate::{
    change::{ReplicaChange, ReplicaWriteOp},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    coroutine::*,
    mutate::{ReplicaMutate, ReplicaMutateError, ReplicaMutation},
    object::ReplicaHash,
    open::ReplicaOpen,
    placement::{ReplicaHandle, ReplicaLinkId},
    rekey::{ReplicaRekey, ReplicaRekeyReport},
    remote::{ReplicaFetchedItem, ReplicaPushResult, ReplicaRemoteSnapshot, ReplicaTier},
    storage::{ReplicaLoadScope, ReplicaLoaded},
    sync::{ReplicaSync, ReplicaSyncOptions, ReplicaSyncReport},
    upgrade::{ReplicaUpgrade, ReplicaUpgradeReport},
};

/// The local index plus blob store seam.
pub trait ReplicaStorage {
    /// The error this storage raises.
    type Error;

    /// Loads a collection's placements and checkpoint.
    ///
    /// `scope` is a floor: the reply SHALL hold at least the placements it
    /// names and MAY hold more, so a storage that ignores it and returns
    /// the whole collection stays correct. Honouring it is what keeps a
    /// one-row edit from reading a whole mailbox.
    fn load(
        &self,
        collection: &ReplicaCollectionId,
        scope: &ReplicaLoadScope,
    ) -> Result<ReplicaLoaded, Self::Error>;

    /// Resolves which link ids already map to a stored object.
    fn lookup_objects(
        &self,
        links: &[ReplicaLinkId],
    ) -> Result<BTreeMap<ReplicaLinkId, ReplicaHash>, Self::Error>;

    /// Applies a batch of writes atomically and **in order**, maintaining
    /// the pointer-derived object references [`ReplicaWriteOp`] documents.
    ///
    /// Order matters because a batch may name one handle twice: a sync that
    /// clears an ambiguity and then reads the same handle as vanished writes
    /// the cleared placement and then drops it, and the drop is the answer.
    /// A storage may not group the batch by op kind, or otherwise reorder
    /// it.
    ///
    /// The engine assumes a single writer per collection between a load
    /// and the write derived from it: a batch applied over state another
    /// actor changed in between clobbers that change. How the guarantee
    /// is provided is the storage's business (a sqlite transaction, a
    /// lock file, process-level serialization).
    fn write(&mut self, ops: Vec<ReplicaWriteOp>) -> Result<(), Self::Error>;
}

/// The remote protocol seam (IMAP, JMAP, WebDAV).
pub trait ReplicaRemote {
    /// The error this remote raises.
    type Error;

    /// Enumerates the collection: a full set, or a delta from `cursor`.
    fn enumerate(
        &mut self,
        collection: &ReplicaCollectionId,
        cursor: Option<ReplicaCheckpoint>,
    ) -> Result<ReplicaRemoteSnapshot, Self::Error>;

    /// Fetches each handle at the requested tier.
    fn fetch(
        &mut self,
        collection: &ReplicaCollectionId,
        handles: Vec<ReplicaHandle>,
        tier: ReplicaTier,
    ) -> Result<Vec<ReplicaFetchedItem>, Self::Error>;

    /// Pushes each change, returning a per-change outcome.
    ///
    /// Pushes are at-least-once: a crash between a serviced push and its
    /// recording write replays the change on the next sync, so the
    /// consumer keeps retries harmless (see [`ReplicaChange`]).
    fn push(
        &mut self,
        collection: &ReplicaCollectionId,
        changes: Vec<ReplicaChange>,
    ) -> Result<Vec<ReplicaPushResult>, Self::Error>;
}

/// Errors returned by [`ReplicaClient`].
#[derive(Debug)]
pub enum ReplicaClientError<S, R, C> {
    /// A storage seam call failed.
    Storage(S),
    /// A remote seam call failed.
    Remote(R),
    /// The coroutine itself completed with an error.
    Coroutine(C),
}

impl<S, R, C> fmt::Display for ReplicaClientError<S, R, C>
where
    S: fmt::Display,
    R: fmt::Display,
    C: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "Storage seam failed: {err}"),
            Self::Remote(err) => write!(f, "Remote seam failed: {err}"),
            Self::Coroutine(err) => write!(f, "Replica engine failed: {err}"),
        }
    }
}

impl<S, R, C> error::Error for ReplicaClientError<S, R, C>
where
    S: fmt::Display + fmt::Debug,
    R: fmt::Display + fmt::Debug,
    C: fmt::Display + fmt::Debug,
{
}

/// Blocking replica client wrapping a storage and a remote.
pub struct ReplicaClient<S, R> {
    storage: S,
    remote: R,
}

impl<S, R> ReplicaClient<S, R>
where
    S: ReplicaStorage,
    R: ReplicaRemote,
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
    ) -> Result<T, ReplicaClientError<S::Error, R::Error, E>>
    where
        C: ReplicaCoroutine<Yield = ReplicaYield, Return = Result<T, E>>,
    {
        let mut arg: Option<ReplicaArg> = None;

        loop {
            match coroutine.resume(arg.take()) {
                ReplicaCoroutineState::Complete(Ok(out)) => return Ok(out),
                ReplicaCoroutineState::Complete(Err(err)) => {
                    return Err(ReplicaClientError::Coroutine(err));
                }
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsEnumerate {
                    collection,
                    cursor,
                }) => {
                    let snapshot = self
                        .remote
                        .enumerate(&collection, cursor)
                        .map_err(ReplicaClientError::Remote)?;
                    arg = Some(ReplicaArg::Enumerate(snapshot));
                }
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch {
                    collection,
                    handles,
                    tier,
                }) => {
                    let items = self
                        .remote
                        .fetch(&collection, handles, tier)
                        .map_err(ReplicaClientError::Remote)?;
                    arg = Some(ReplicaArg::Fetch(items));
                }
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsPush {
                    collection,
                    changes,
                }) => {
                    let results = self
                        .remote
                        .push(&collection, changes)
                        .map_err(ReplicaClientError::Remote)?;
                    arg = Some(ReplicaArg::Push(results));
                }
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad { collection, scope }) => {
                    let loaded = self
                        .storage
                        .load(&collection, &scope)
                        .map_err(ReplicaClientError::Storage)?;
                    arg = Some(ReplicaArg::Load(loaded));
                }
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLookupObject(links)) => {
                    let known = self
                        .storage
                        .lookup_objects(&links)
                        .map_err(ReplicaClientError::Storage)?;
                    arg = Some(ReplicaArg::LookupObject(known));
                }
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => {
                    self.storage
                        .write(ops)
                        .map_err(ReplicaClientError::Storage)?;
                    arg = Some(ReplicaArg::Write);
                }
            }
        }
    }

    /// Opens a collection fully offline.
    pub fn open(
        &mut self,
        collection: impl Into<ReplicaCollectionId>,
    ) -> Result<ReplicaLoaded, ReplicaClientError<S::Error, R::Error, ReplicaArgError>> {
        self.run(ReplicaOpen::new(collection))
    }

    /// Raises `handles` in `collection` to `tier`, deduping bodies.
    pub fn upgrade(
        &mut self,
        collection: impl Into<ReplicaCollectionId>,
        handles: Vec<ReplicaHandle>,
        tier: ReplicaTier,
    ) -> Result<ReplicaUpgradeReport, ReplicaClientError<S::Error, R::Error, ReplicaArgError>> {
        self.run(ReplicaUpgrade::new(collection, handles, tier))
    }

    /// Applies a local mutation with no network.
    pub fn mutate(
        &mut self,
        collection: impl Into<ReplicaCollectionId>,
        mutation: ReplicaMutation,
    ) -> Result<(), ReplicaClientError<S::Error, R::Error, ReplicaMutateError>> {
        self.run(ReplicaMutate::new(collection, mutation))
    }

    /// Reconciles a collection with its remote.
    pub fn sync(
        &mut self,
        collection: impl Into<ReplicaCollectionId>,
        opts: ReplicaSyncOptions,
    ) -> Result<ReplicaSyncReport, ReplicaClientError<S::Error, R::Error, ReplicaArgError>> {
        self.run(ReplicaSync::new(collection, opts))
    }

    /// Rebuilds a collection after a handle-space change (an IMAP
    /// UIDVALIDITY bump), carrying local state over by link id.
    pub fn rekey(
        &mut self,
        collection: impl Into<ReplicaCollectionId>,
    ) -> Result<ReplicaRekeyReport, ReplicaClientError<S::Error, R::Error, ReplicaArgError>> {
        self.run(ReplicaRekey::new(collection))
    }
}
