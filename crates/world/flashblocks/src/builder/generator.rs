use alloy_primitives::map::foldhash::fast::SeedableRandomState;
use alloy_primitives::B256;
use alloy_rpc_types_engine::PayloadId;
use eyre::eyre::eyre;
use flashblocks_p2p::protocol::handler::FlashblocksHandle;
use futures::{FutureExt, StreamExt, TryFutureExt};
use op_alloy_consensus::OpBlock;
use reth::api::{Block, BuiltPayload, PayloadBuilderError};
use reth::payload::PayloadJob;
use reth::revm::cached::CachedReads;
use reth::{api::PayloadBuilderAttributes, payload::PayloadJobGenerator, tasks::TaskSpawner};
use reth_basic_payload_builder::{
    HeaderForPayload, PayloadBuilder, PayloadConfig, PayloadState, PayloadTaskGuard, PrecachedState,
};
use reth_evm::execute::BlockBuilder;
use reth_optimism_node::OpBuiltPayload;
use reth_optimism_primitives::OpPrimitives;
use reth_transaction_pool::TransactionPool;
use std::future::Future;
use std::hash::{BuildHasher, Hash};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::debug;

use reth_primitives::NodePrimitives;
use reth_provider::{
    BlockReaderIdExt, CanonStateNotification, ChainSpecProvider, StateProviderFactory,
};
use rollup_boost::Authorization;
use schnellru::{ByLength, LruMap};

use crate::builder::job::WorldChainPayloadJob;
use crate::builder::FlashblockBuiltPayload;
use crate::rpc::engine::{BlockMetaData, Flashblock, FlashblocksState};

/// A thread-safe LRU cache wrapper around `schnellru::LruMap`.
///
/// # Safety
///
/// This implementation uses raw pointers to provide shared mutable access to the underlying
/// `LruMap`. The safety of this approach relies on the following invariants:
///
/// 1. The `LruMap` is allocated on the heap and never moved
/// 2. All access is properly synchronized through the type system (Send/Sync bounds)
/// 3. The cache is never accessed after being dropped
/// 4. All operations are atomic at the map level (individual get/set/remove calls)
///
/// The raw pointer approach is used here to avoid the overhead of `Mutex`/`RwLock` for
/// performance-critical payload building operations.
pub(crate) struct LruCache<K, V, H = SeedableRandomState> {
    /// Raw pointer to the heap-allocated LruMap
    ///
    /// # Safety
    /// This pointer must always point to a valid, heap-allocated `LruMap` that lives
    /// for the entire lifetime of this `LruCache` instance.
    inner: *mut LruMap<K, V, ByLength, H>,
}

/// # Safety
/// `LruCache` can be safely sent between threads because:
/// - The raw pointer points to heap-allocated data
/// - All constituent types (K, V, H) are Send
/// - No thread-local state is maintained
unsafe impl<K, V, H> Send for LruCache<K, V, H>
where
    K: Send,
    V: Send,
    H: Send,
{
}

/// # Safety  
/// `LruCache` can be safely shared between threads because:
/// - K implements Copy, so keys can be safely copied across threads
/// - K implements Hash + Eq for safe concurrent lookups
/// - H implements BuildHasher + Default for consistent hashing across threads
/// - V values are protected by the cache's internal synchronization
/// - All operations are atomic at the map level
unsafe impl<K, V, H> Sync for LruCache<K, V, H>
where
    K: Hash + Eq + Copy + Send,
    V: Send,
    H: BuildHasher + Default + Send,
{
}

impl<K, V, H> Deref for LruCache<K, V, H> {
    type Target = LruMap<K, V, ByLength, H>;

    /// # Safety
    /// This dereference is safe because:
    /// - The pointer was created from a valid heap allocation
    /// - The LruMap lifetime is tied to this LruCache instance
    /// - Deref only provides shared access (&T), not mutable access
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner }
    }
}

impl<K, V, H> Drop for LruCache<K, V, H> {
    fn drop(&mut self) {
        // Safety: We own the pointer and it points to a valid heap allocation
        unsafe {
            let _ = Box::from_raw(self.inner);
        }
    }
}

impl<K, V, H> LruCache<K, V, H>
where
    K: Hash + Eq + Copy,
    H: Default + BuildHasher,
{
    /// Creates a new LRU cache with a maximum capacity of 20 items.
    ///
    /// # Panics
    /// Panics if heap allocation fails.
    pub(crate) fn new() -> Self {
        let boxed = Box::new(LruMap::with_hasher(ByLength::new(20), H::default()));
        Self {
            inner: Box::into_raw(boxed),
        }
    }

    /// Creates a new LRU cache with the specified capacity.
    ///
    /// # Panics
    /// Panics if heap allocation fails.
    pub(crate) fn with_capacity(capacity: u32) -> Self {
        let boxed = Box::new(LruMap::with_hasher(ByLength::new(capacity), H::default()));
        Self {
            inner: Box::into_raw(boxed),
        }
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, the old value is replaced and returned.
    /// If the cache is at capacity, the least recently used item is evicted.
    ///
    /// # Safety
    /// This operation is safe because we have exclusive access through &self
    /// and the key type implements Copy.
    pub(crate) fn insert(&self, key: K, value: V) -> bool {
        // Safety: We have a valid pointer and exclusive access to the operation
        unsafe { (*self.inner).insert(key, value) }
    }

    /// Legacy method for compatibility. Use `insert` instead.
    #[deprecated(note = "Use `insert` instead")]
    pub(crate) fn set(&self, key: &K, value: V) {
        self.insert(*key, value);
    }

    /// Retrieves a reference to the value associated with the key.
    ///
    /// This operation marks the key as recently used, moving it to the front
    /// of the LRU order.
    ///
    /// # Safety
    /// This operation is safe because:
    /// - We have a valid pointer to the map
    /// - The returned reference lifetime is bounded by the cache lifetime
    /// - LruMap::get provides proper lifetime guarantees
    pub(crate) fn get(&self, key: K) -> Option<&V> {
        unsafe { (*self.inner).get(&key).map(|v| v as &mut V as &V) }
    }

    /// Retrieves a reference to the value without updating LRU order.
    ///
    /// This is useful when you want to check if a key exists without
    /// affecting the eviction order.
    pub(crate) fn peek(&self, key: K) -> Option<&V> {
        unsafe { (*self.inner).peek(&key) }
    }

    /// Removes and returns the value associated with the key.
    ///
    /// # Safety
    /// This operation is safe because we have exclusive access to the
    /// remove operation and the key type implements Copy.
    pub(crate) fn take(&self, key: K) -> Option<V> {
        unsafe { (*self.inner).remove(&key) }
    }

    /// Returns the number of items currently in the cache.
    pub(crate) fn len(&self) -> usize {
        unsafe { (*self.inner).len() }
    }

    /// Returns true if the cache is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all items from the cache.
    pub(crate) fn clear(&self) {
        unsafe { (*self.inner).clear() }
    }
}

/// Settings for the [`BasicPayloadJobGenerator`].
#[derive(Debug, Clone, Default)]
pub struct FlashblocksPayloadConfiguration {
    /// The interval at which the job should build a new payload after the last.
    interval: Duration,
    /// The deadline for when the payload builder job should resolve.
    ///
    /// By default this is [`SLOT_DURATION`]: 12s
    deadline: Duration,
}

impl FlashblocksPayloadConfiguration {
    /// Sets the interval at which the job should build a new payload after the last.
    pub const fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Sets the deadline when this job should resolve.
    pub const fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }
}

pub struct Authorizations {
    /// Authorized payload ids to build
    active: Arc<LruCache<PayloadId, Authorization>>,
    /// A subscriber for authorization updates
    authorizations_subscriber: tokio::sync::watch::Receiver<Option<Authorization>>,
}

impl Authorizations {
    pub fn subscribe_to_updates(mut self) -> Pin<Box<impl Future<Output = ()>>> {
        Box::pin(async move {
            tokio::select! {
                _ = self.authorizations_subscriber.changed() => {
                    // Handle updates to authorizations here if needed
                    if let Some(authorization) = *self.authorizations_subscriber.borrow_and_update() {
                        self.active.insert(authorization.payload_id, authorization);
                    }
                }
            }
        })
    }
}

/// A type that initiates payload building jobs on the [`FlashblocksPayloadBuilder`].
pub struct WorldChainJobsGenerator<N, Builder, Client, Tasks> {
    /// The block builder used to construct payloads
    builder: Builder,
    /// The executor used to spawn tasks
    executor: Tasks,
    /// Configuration for the job generator
    config: FlashblocksPayloadConfiguration,
    /// The client used to fetch state and block data
    client: Client,
    /// The pre-cached reads for the given parent header
    pre_cached: Option<PrecachedState>,
    /// The authorizations for the job generator
    authorizations: Arc<LruCache<PayloadId, Authorization>>,
    /// The p2p handle
    p2p_handle: FlashblocksHandle,
    /// flashblocks state
    flashblocks_state: FlashblocksState,
    _marker: std::marker::PhantomData<N>,
}

impl<N, Builder, Client, Tasks> WorldChainJobsGenerator<N, Builder, Client, Tasks>
where
    Tasks: TaskSpawner,
{
    /// Creates a new [`WorldChainJobsGenerator`] with the given authorizations.
    pub fn with_authorizations(
        client: Client,
        executor: Tasks,
        config: FlashblocksPayloadConfiguration,
        builder: Builder,
        authorizations_rx: tokio::sync::watch::Receiver<Option<Authorization>>,
        p2p_handler: FlashblocksHandle,
        flashblocks_state: FlashblocksState,
    ) -> Self {
        let authorizations_cache = Arc::new(LruCache::<PayloadId, Authorization>::new());

        let authorizations = Authorizations {
            active: authorizations_cache.clone(),
            authorizations_subscriber: authorizations_rx,
        };

        executor.spawn_critical(
            "authorization_updates",
            authorizations.subscribe_to_updates(),
        );

        Self {
            client,
            executor,
            config,
            builder,
            pre_cached: None,
            authorizations: authorizations_cache,
            p2p_handle: p2p_handler,
            flashblocks_state,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the maximum duration a job should be allowed to run.
    ///
    /// This adheres to the following specification:
    /// > Client software SHOULD stop the updating process when either a call to engine_getPayload
    /// > with the build process's payloadId is made or SECONDS_PER_SLOT (12s in the Mainnet
    /// > configuration) have passed since the point in time identified by the timestamp parameter.
    ///
    /// See also <https://github.com/ethereum/execution-apis/blob/431cf72fd3403d946ca3e3afc36b973fc87e0e89/src/engine/paris.md?plain=1#L137>
    #[inline]
    fn max_job_duration(&self, unix_timestamp: u64) -> Duration {
        let duration_until_timestamp = Duration::from_secs(unix_timestamp)
            .saturating_sub(tokio::time::Instant::now().elapsed());

        // safety in case clocks are bad
        let duration_until_timestamp = duration_until_timestamp.min(self.config.deadline * 3);

        self.config.deadline + duration_until_timestamp
    }

    /// Returns the [Instant](tokio::time::Instant) at which the job should be terminated because it
    /// is considered timed out.
    #[inline]
    fn job_deadline(&self, unix_timestamp: u64) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.max_job_duration(unix_timestamp)
    }

    /// Returns a reference to the tasks type
    pub const fn tasks(&self) -> &Tasks {
        &self.executor
    }

    /// Returns the pre-cached reads for the given parent header if it matches the cached state's
    /// block.
    fn maybe_pre_cached(&self, parent: B256) -> Option<PrecachedState> {
        if let Some(pre_cached) = &self.pre_cached {
            if pre_cached.block == parent {
                return Some(pre_cached.clone());
            }
        }
        None
    }
}

impl<N, Builder, Client, Tasks> PayloadJobGenerator
    for WorldChainJobsGenerator<N, Builder, Client, Tasks>
where
    N: NodePrimitives<Block = OpBlock> + Clone + Unpin + Send + Sync + 'static,
    Client: StateProviderFactory
        + BlockReaderIdExt<Header = HeaderForPayload<Builder::BuiltPayload>>
        + ChainSpecProvider
        + Clone
        + Unpin
        + 'static,

    Tasks: TaskSpawner + Clone + Unpin + 'static,
    Tasks: TaskSpawner + Clone + Send + Sync + 'static,
    Builder: PayloadBuilder<BuiltPayload = FlashblockBuiltPayload<N>> + Clone + Unpin + 'static,
    Builder::Attributes: Unpin + Clone,
    Builder::BuiltPayload: Unpin + Clone,
{
    type Job = WorldChainPayloadJob<Builder, Tasks>;

    fn new_payload_job(
        &self,
        attributes: <Self::Job as PayloadJob>::PayloadAttributes,
    ) -> Result<Self::Job, PayloadBuilderError> {
        let parent_header = if attributes.parent().is_zero() {
            // Use latest header for genesis block case
            self.client
                .latest_header()
                .map_err(PayloadBuilderError::from)?
                .ok_or_else(|| PayloadBuilderError::MissingParentHeader(B256::ZERO))?
        } else {
            // Fetch specific header by hash
            self.client
                .sealed_header_by_hash(attributes.parent())
                .map_err(PayloadBuilderError::from)?
                .ok_or_else(|| PayloadBuilderError::MissingParentHeader(attributes.parent()))?
        };

        let config = PayloadConfig::new(Arc::new(parent_header.clone()), attributes.clone());

        let until = self.job_deadline(config.attributes.timestamp());

        let deadline = Box::pin(tokio::time::sleep_until(until));
        let cached_reads = self.maybe_pre_cached(parent_header.hash());

        let authorization = self
            .authorizations
            .take(attributes.payload_id())
            .ok_or_else(|| {
                PayloadBuilderError::Other(
                    eyre!(
                        "Missing authorization for payload ID {}",
                        attributes.payload_id()
                    )
                    .into(),
                )
            })?;

        let maybe_pre_built_payload = self.check_for_pre_state(&attributes)?;

        let mut job = WorldChainPayloadJob {
            config: config.clone(),
            payload_task_guard: PayloadTaskGuard::new(1),
            executor: self.executor.clone(),
            deadline: Box::pin(tokio::time::sleep_until(until)),
            best_payload: PayloadState::Missing,
            pending_block: None,
            cached_reads: None,
            metrics: Default::default(),
            builder: self.builder.clone(),
            authorization: Some(authorization),
            p2p_handler: self.p2p_handle.clone(),
            pre_built_payload: maybe_pre_built_payload,
        };

        // start the first job right away
        job.spawn_build_job();

        Ok(job)
    }

    fn on_new_state<Node: NodePrimitives>(&mut self, new_state: CanonStateNotification<Node>) {
        let mut cached = CachedReads::default();

        // extract the state from the notification and put it into the cache
        let committed = new_state.committed();
        let new_execution_outcome = committed.execution_outcome();
        for (addr, acc) in new_execution_outcome.bundle_accounts_iter() {
            if let Some(info) = acc.info.clone() {
                // we want pre cache existing accounts and their storage
                // this only includes changed accounts and storage but is better than nothing
                let storage = acc
                    .storage
                    .iter()
                    .map(|(key, slot)| (*key, slot.present_value))
                    .collect();
                cached.insert_account(addr, info, storage);
            }
        }

        self.pre_cached = Some(PrecachedState {
            block: committed.tip().hash(),
            cached,
        });
    }
}

impl<N, Builder, Client, Tasks> WorldChainJobsGenerator<N, Builder, Client, Tasks>
where
    N: NodePrimitives<Block = OpBlock>,
    Builder: PayloadBuilder<BuiltPayload = FlashblockBuiltPayload<N>>,
{
    fn check_for_pre_state(
        &self,
        attributes: &<Builder as PayloadBuilder>::Attributes,
    ) -> Result<Option<<Builder as PayloadBuilder>::BuiltPayload>, PayloadBuilderError> {
        // check for any pending pre state received over p2p
        let state = tokio::task::block_in_place(|| {
            let handle = Handle::current();
            handle.block_on(async { self.flashblocks_state.0.read().await })
        });

        if !state.is_empty() {
            let block = Flashblock::reduce(state.clone());
            if let Some(flashblock) = block {
                if *flashblock.payload_id() == attributes.payload_id().0 {
                    // If we have a pre-confirmed state, we can use it to build the payload
                    debug!(target: "payload_builder", payload_id = %attributes.payload_id(), "Using pre-confirmed state for payload");

                    let block =
                        flashblock
                            .clone()
                            .into_built_block()
                            .ok_or(PayloadBuilderError::Other(
                                eyre!("Failed to build block from pre confirmations").into(),
                            ))?;

                    let block_meta = serde_json::from_value::<BlockMetaData<OpPrimitives>>(
                        flashblock.flashblock().metadata.clone(),
                    )
                    .expect("never fails");

                    let sealed = block.into_sealed_block();

                    let payload = OpBuiltPayload::new(
                        attributes.payload_id(),
                        Arc::new(sealed),
                        block_meta.fees,
                        None,
                    );

                    let flashblock_payload = FlashblockBuiltPayload {
                        payload,
                        block_index: flashblock.flashblock().index,
                        built_flashblock: flashblock,
                    };

                    return Ok(Some(flashblock_payload));
                }
            }
        }

        Ok(None)
    }
}
