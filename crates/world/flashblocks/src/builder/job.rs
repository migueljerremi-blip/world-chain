use std::{
    future::Future,
    hash::BuildHasher,
    pin::Pin,
    task::{Context, Poll},
};

use flashblocks_p2p::protocol::handler::FlashblocksHandle;
use futures::{FutureExt, StreamExt};
use reth::{
    api::{BuiltPayload, PayloadBuilderError, PayloadKind},
    payload::{KeepPayloadJobAlive, PayloadJob},
    revm::{cached::CachedReads, cancelled::CancelOnDrop},
    tasks::TaskSpawner,
};
use reth_basic_payload_builder::{
    BuildArguments, BuildOutcome, HeaderForPayload, MissingPayloadBehaviour, PayloadBuilder,
    PayloadConfig, PayloadState, PayloadTaskGuard, PendingPayload, ResolveBestPayload,
};
use rollup_boost::Authorization;
use tokio::{sync::oneshot, time::Sleep};
use tracing::{debug, trace};

use crate::builder::metrics::PayloadBuilderMetrics;

/// A basic payload job that continuously builds a payload with the best transactions from the pool.
///
/// This type is a [`PayloadJob`] and [`Future`] that terminates when the deadline is reached or
/// when the job is resolved: [`PayloadJob::resolve`].
///
/// This basic job implementation will trigger new payload build task continuously until the job is
/// resolved or the deadline is reached, or until the built payload is marked as frozen:
/// [`BuildOutcome::Freeze`]. Once a frozen payload is returned, no additional payloads will be
/// built and this future will wait to be resolved: [`PayloadJob::resolve`] or terminated if the
/// deadline is reached..
pub struct WorldChainPayloadJob<Builder: PayloadBuilder, Tasks> {
    /// The configuration for how the payload will be created.
    pub(crate) config: PayloadConfig<Builder::Attributes, HeaderForPayload<Builder::BuiltPayload>>,
    /// How to spawn building tasks
    pub(crate) executor: Tasks,
    /// The best payload so far and its state.
    pub(crate) best_payload: PayloadState<Builder::BuiltPayload>,
    /// Receiver for the block that is currently being built.
    pub(crate) pending_block: Option<PendingPayload<Builder::BuiltPayload>>,
    /// Restricts how many generator tasks can be executed at once.
    pub(crate) payload_task_guard: PayloadTaskGuard,
    /// Caches all disk reads for the state the new payloads builds on
    ///
    /// This is used to avoid reading the same state over and over again when new attempts are
    /// triggered, because during the building process we'll repeatedly execute the transactions.
    pub(crate) cached_reads: Option<CachedReads>,
    /// metrics for this type
    pub(crate) metrics: PayloadBuilderMetrics,
    /// The type responsible for building payloads.
    ///
    /// See [`PayloadBuilder`]
    pub(crate) builder: Builder,
    /// The authorization information for this job
    pub(crate) authorization: Option<Authorization>,
    /// The deadline when this job should resolve.
    pub(crate) deadline: Pin<Box<Sleep>>,
    /// The p2p handler for flashblocks
    pub(crate) p2p_handler: FlashblocksHandle,
    /// Any pre-confirmed state on the Payload ID corresponding to this job
    pub(crate) pre_built_payload: Option<Builder::BuiltPayload>,
}

impl<Builder, Tasks> WorldChainPayloadJob<Builder, Tasks>
where
    Tasks: TaskSpawner + Clone + 'static,
    Builder: PayloadBuilder + Unpin + 'static,
    Builder::Attributes: Unpin + Clone,
    Builder::BuiltPayload: Unpin + Clone,
{
    /// Spawns a new payload build task.
    pub(crate) fn spawn_build_job(&mut self) {
        trace!(target: "payload_builder", id = %self.config.payload_id(), "spawn new payload build task");
        let (tx, rx) = oneshot::channel();
        let cancel = CancelOnDrop::default();
        let _cancel = cancel.clone();
        let guard = self.payload_task_guard.clone();
        let payload_config = self.config.clone();
        let best_payload = self.best_payload.payload().cloned();
        self.metrics.inc_initiated_payload_builds();

        let cached_reads = self.cached_reads.take().unwrap_or_default();
        let builder = self.builder.clone();

        if let Some(pre_built_payload) = self.pre_built_payload.clone() {
            self.best_payload = PayloadState::Frozen(pre_built_payload);
        } else {
            if let Some(auth) = self.authorization {
                self.p2p_handler.start_publishing(auth);
            }

            self.executor.spawn_blocking(Box::pin(async move {
                let _permit = guard.acquire().await;
                let args = BuildArguments {
                    cached_reads,
                    config: payload_config,
                    cancel,
                    best_payload,
                };

                //
                let result = builder.try_build(args);
                let _ = tx.send(result);
            }));

            self.pending_block = Some(PendingPayload::new(_cancel, rx));
        }
    }
}

impl<Builder, Tasks> Future for WorldChainPayloadJob<Builder, Tasks>
where
    Tasks: TaskSpawner + Clone + 'static,
    Builder: PayloadBuilder + Unpin + 'static,
    Builder::Attributes: Unpin + Clone,
    Builder::BuiltPayload: Unpin + Clone,
{
    type Output = Result<(), PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // check if the deadline is reached
        if this.deadline.as_mut().poll(cx).is_ready() {
            trace!(target: "payload_builder", "payload building deadline reached");
            return Poll::Ready(Ok(()));
        }

        // poll the pending block
        if let Some(mut fut) = this.pending_block.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(outcome)) => match outcome {
                    BuildOutcome::Better {
                        payload,
                        cached_reads,
                    } => {
                        this.cached_reads = Some(cached_reads);
                        debug!(target: "payload_builder", value = %payload.fees(), "built better payload");
                        this.best_payload = PayloadState::Best(payload);
                    }
                    BuildOutcome::Freeze(payload) => {
                        debug!(target: "payload_builder", "payload frozen, no further building will occur");
                        this.best_payload = PayloadState::Frozen(payload);
                    }
                    BuildOutcome::Aborted { fees, cached_reads } => {
                        this.cached_reads = Some(cached_reads);
                        trace!(target: "payload_builder", worse_fees = %fees, "skipped payload build of worse block");
                    }
                    BuildOutcome::Cancelled => {
                        unreachable!("the cancel signal never fired")
                    }
                },
                Poll::Ready(Err(error)) => {
                    // job failed, but we simply try again next interval
                    debug!(target: "payload_builder", %error, "payload build attempt failed");
                    this.metrics.inc_failed_payload_builds();
                }
                Poll::Pending => {
                    this.pending_block = Some(fut);
                }
            }
        }

        Poll::Pending
    }
}

impl<Builder, Tasks> PayloadJob for WorldChainPayloadJob<Builder, Tasks>
where
    Tasks: TaskSpawner + Clone + 'static,
    Builder: PayloadBuilder + Unpin + 'static,
    Builder::Attributes: Unpin + Clone,
    Builder::BuiltPayload: Unpin + Clone,
{
    type PayloadAttributes = Builder::Attributes;
    type ResolvePayloadFuture = ResolveBestPayload<Self::BuiltPayload>;
    type BuiltPayload = Builder::BuiltPayload;

    fn best_payload(&self) -> Result<Self::BuiltPayload, PayloadBuilderError> {
        if let Some(payload) = self.best_payload.payload() {
            Ok(payload.clone())
        } else {
            // No payload has been built yet, but we need to return something that the CL then
            // can deliver, so we need to return an empty payload.
            //
            // Note: it is assumed that this is unlikely to happen, as the payload job is
            // started right away and the first full block should have been
            // built by the time CL is requesting the payload.
            self.metrics.inc_requested_empty_payload();
            self.builder.build_empty_payload(self.config.clone())
        }
    }

    fn payload_attributes(&self) -> Result<Self::PayloadAttributes, PayloadBuilderError> {
        Ok(self.config.attributes.clone())
    }

    fn resolve_kind(
        &mut self,
        kind: PayloadKind,
    ) -> (Self::ResolvePayloadFuture, KeepPayloadJobAlive) {
        let best_payload = self.best_payload.payload().cloned();
        if best_payload.is_none() && self.pending_block.is_none() {
            // ensure we have a job scheduled if we don't have a best payload yet and none is active
            self.spawn_build_job();
        }

        let maybe_better = self.pending_block.take();
        let mut empty_payload = None;

        if best_payload.is_none() {
            debug!(target: "payload_builder", id=%self.config.payload_id(), "no best payload yet to resolve, building empty payload");

            let args = BuildArguments {
                cached_reads: self.cached_reads.take().unwrap_or_default(),
                config: self.config.clone(),
                cancel: CancelOnDrop::default(),
                best_payload: None,
            };

            match self.builder.on_missing_payload(args) {
                MissingPayloadBehaviour::AwaitInProgress => {
                    debug!(target: "payload_builder", id=%self.config.payload_id(), "awaiting in progress payload build job");
                }
                MissingPayloadBehaviour::RaceEmptyPayload => {
                    debug!(target: "payload_builder", id=%self.config.payload_id(), "racing empty payload");

                    // if no payload has been built yet
                    self.metrics.inc_requested_empty_payload();
                    // no payload built yet, so we need to return an empty payload
                    let (tx, rx) = oneshot::channel();
                    let config = self.config.clone();
                    let builder = self.builder.clone();
                    self.executor.spawn_blocking(Box::pin(async move {
                        let res = builder.build_empty_payload(config);
                        let _ = tx.send(res);
                    }));

                    empty_payload = Some(rx);
                }
                MissingPayloadBehaviour::RacePayload(job) => {
                    debug!(target: "payload_builder", id=%self.config.payload_id(), "racing fallback payload");
                    // race the in progress job with this job
                    let (tx, rx) = oneshot::channel();
                    self.executor.spawn_blocking(Box::pin(async move {
                        let _ = tx.send(job());
                    }));
                    empty_payload = Some(rx);
                }
            };
        }

        let fut = ResolveBestPayload {
            best_payload,
            maybe_better,
            empty_payload: empty_payload.filter(|_| kind != PayloadKind::WaitForPending),
        };

        (fut, KeepPayloadJobAlive::No)
    }
}
