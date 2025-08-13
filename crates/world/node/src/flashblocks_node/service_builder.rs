use flashblocks::{
    builder::{
        generator::{FlashblocksPayloadConfiguration, WorldChainJobsGenerator},
        FlashblockBuiltPayload,
    },
    rpc::engine::FlashblocksState,
};
use flashblocks_p2p::protocol::handler::FlashblocksHandle;
use reth::payload::PayloadBuilderHandle;
use reth_node_api::{Events, FullNodeTypes, NodeTypes, PayloadTypes};
use reth_node_builder::{
    components::{PayloadBuilderBuilder, PayloadServiceBuilder},
    BuilderContext,
};
use reth_optimism_node::{OpBuiltPayload, OpEngineTypes, OpNodeTypes, OpPayloadTypes};
use reth_optimism_primitives::OpPrimitives;
use reth_payload_builder::PayloadBuilderService;
use reth_provider::{CanonStateNotificationStream, CanonStateSubscriptions};
use reth_transaction_pool::TransactionPool;
use rollup_boost::Authorization;
use tokio::sync::broadcast::Sender;

use crate::flashblocks_node::{FlashblocksPayloadTypes, WorldChainFlashblocksNode};

/// Basic payload service builder that spawns a [`BasicPayloadJobGenerator`]
#[derive(Debug, Clone)]
pub struct FlashblocksPayloadServiceBuilder<PB> {
    pb: PB,
    p2p_handler: FlashblocksHandle,
    to_jobs_generator: tokio::sync::watch::Sender<Option<Authorization>>,
    flashblocks_state: FlashblocksState,
}

impl<PB> FlashblocksPayloadServiceBuilder<PB> {
    /// Create a new [`FlashblocksPayloadServiceBuilder`].
    pub const fn new(
        pb: PB,
        p2p_handler: FlashblocksHandle,
        to_jobs_generator: tokio::sync::watch::Sender<Option<Authorization>>,
        flashblocks_state: FlashblocksState,
    ) -> Self {
        Self {
            pb,
            p2p_handler,
            to_jobs_generator,
            flashblocks_state,
        }
    }
}

impl<Node, Pool, PB, EvmConfig> PayloadServiceBuilder<Node, Pool, EvmConfig>
    for FlashblocksPayloadServiceBuilder<PB>
where
    Node: FullNodeTypes<Types = WorldChainFlashblocksNode>,
    Pool: TransactionPool,
    EvmConfig: Send,
    PB: PayloadBuilderBuilder<Node, Pool, EvmConfig>,
{
    async fn spawn_payload_builder_service(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
        evm_config: EvmConfig,
    ) -> eyre::Result<PayloadBuilderHandle<<Node::Types as NodeTypes>::Payload>> {
        let payload_builder = self.pb.build_payload_builder(ctx, pool, evm_config).await?;

        let conf = ctx.config().builder.clone();

        let payload_job_config = FlashblocksPayloadConfiguration::default()
            .interval(conf.interval)
            .deadline(conf.deadline);

        let payload_generator = WorldChainJobsGenerator::with_authorizations(
            ctx.provider().clone(),
            ctx.task_executor().clone(),
            payload_job_config,
            payload_builder,
            self.to_jobs_generator.subscribe(),
            self.p2p_handler.clone(),
            self.flashblocks_state.clone(),
        );

        let (payload_service, payload_service_handle) =
            PayloadBuilderService::new(payload_generator, ctx.provider().canonical_state_stream());

        let handle = payload_service_handle.clone();

        let payload_events = handle.subscribe();

        ctx.task_executor()
            .spawn_critical("payload builder service", Box::pin(payload_service));

        Ok(payload_service_handle)
    }
}
