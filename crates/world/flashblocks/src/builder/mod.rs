use crate::{
    builder::executor::{
        FlashblocksBlockBuilder, FlashblocksBlockExecutor, FlashblocksBlockExecutorFactory,
    },
    rpc::engine::Flashblock,
    PayloadBuilderCtx, PayloadBuilderCtxBuilder,
};
use alloy_consensus::{BlockHeader, Transaction};
use alloy_op_evm::{block::receipt_builder::OpReceiptBuilder, OpEvm};
use alloy_primitives::{B256, U256};
use alloy_rpc_types_engine::{
    ExecutionPayloadEnvelopeV2, ExecutionPayloadEnvelopeV3, ExecutionPayloadEnvelopeV4,
    ExecutionPayloadV1,
};
use op_alloy_consensus::{OpBlock, OpTxEnvelope};
use op_alloy_rpc_types_engine::OpExecutionPayloadEnvelopeV3;
use op_alloy_rpc_types_engine::OpExecutionPayloadEnvelopeV4;
use reth::api::{BlockBody, BuiltPayload};
use reth::{
    api::{PayloadBuilderAttributes, PayloadBuilderError},
    chainspec::EthChainSpec,
    revm::{cancelled::CancelOnDrop, database::StateProviderDatabase, State},
};
use reth_basic_payload_builder::{BuildArguments, BuildOutcome, BuildOutcomeKind};
use reth_basic_payload_builder::{MissingPayloadBehaviour, PayloadBuilder, PayloadConfig};
use reth_chain_state::{ExecutedBlock, ExecutedBlockWithTrieUpdates, ExecutedTrieUpdates};
use reth_evm::{
    block::BlockExecutorFactory,
    execute::{BlockBuilder, BlockBuilderOutcome},
    op_revm::OpHaltReason,
    precompiles::PrecompilesMap,
    ConfigureEvm, EvmFactory, FromRecoveredTx, FromTxWithEncoded,
};
use reth_primitives::{transaction::SignedTransaction, Block};
use reth_primitives::{NodePrimitives, Recovered};

use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_forks::OpHardforks;
use reth_optimism_node::{
    txpool::OpPooledTx, OpBlockAssembler, OpEvmConfig, OpNextBlockEnvAttributes,
    OpRethReceiptBuilder,
};
use reth_optimism_payload_builder::config::OpBuilderConfig;
use reth_optimism_payload_builder::{
    builder::OpPayloadTransactions,
    payload::{OpBuiltPayload, OpPayloadBuilderAttributes},
};
use reth_optimism_primitives::{OpPrimitives, OpReceipt, OpTransactionSigned};
use reth_payload_util::{NoopPayloadTransactions, PayloadTransactions};
use reth_provider::{ChainSpecProvider, ExecutionOutcome, StateProvider, StateProviderFactory};

use reth_transaction_pool::{BestTransactionsAttributes, PoolTransaction, TransactionPool};
use revm::{inspector::NoOpInspector, Inspector};
use rollup_boost::{Authorization, Flashblocks};
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, span};

pub mod executor;
pub mod generator;
pub mod job;
pub mod metrics;
pub mod payload_txns;
pub mod traits;

#[derive(Debug, Clone)]
pub struct BlockBuilderConfig<N: NodePrimitives> {
    pub executed_block: ExecutionOutcome<N::Receipt>,
    pub transactions: Vec<Recovered<N::SignedTx>>,
    pub flash_hash: B256,
    pub block_index: u64,
    pub gas_used: u64,
    pub fees: U256,
}

impl<N: NodePrimitives> Default for BlockBuilderConfig<N> {
    fn default() -> Self {
        Self {
            executed_block: ExecutionOutcome::default(),
            transactions: Vec::new(),
            flash_hash: B256::default(),
            block_index: 0,
            gas_used: 0,
            fees: U256::default(),
        }
    }
}

impl<N: NodePrimitives> BlockBuilderConfig<N> {
    fn new(payload: &OpBuiltPayload<N>, flashblock_idx: u64) -> Self {
        let block = payload.block();
        let executed_block = if let Some(executed) = payload.executed_block() {
            executed.block.execution_outcome().clone()
        } else {
            ExecutionOutcome::default()
        };

        let transactions = payload
            .block()
            .body()
            .transactions_iter()
            .map(|tx| tx.try_clone_into_recovered_unchecked())
            .collect::<Result<Vec<_>, _>>()
            .ok()
            .expect("transactions should be recoverable");

        Self {
            flash_hash: block.hash(),
            executed_block,
            gas_used: block.gas_used(),
            fees: payload.fees(),
            block_index: flashblock_idx,
            transactions,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FlashblocksBuilderConfig<N: NodePrimitives> {
    pub da_config: OpBuilderConfig,
    pub block_config: BlockBuilderConfig<N>,
}
// T::BuiltPayload: BuiltPayload<Primitives: NodePrimitives<Block = OpBlock>>
//         + TryInto<ExecutionPayloadV1>
//         + TryInto<ExecutionPayloadEnvelopeV2>
//         + TryInto<OpExecutionPayloadEnvelopeV3>
//         + TryInto<OpExecutionPayloadEnvelopeV4>,
#[derive(Debug, Clone)]
pub struct FlashblockBuiltPayload<N: NodePrimitives<Block = OpBlock>> {
    pub built_flashblock: Flashblock,
    pub payload: OpBuiltPayload<N>,
    pub block_index: u64,
}

impl<N> From<FlashblockBuiltPayload<N>> for ExecutionPayloadV1
where
    N: NodePrimitives<Block = OpBlock>,
{
    fn from(value: FlashblockBuiltPayload<N>) -> Self {
        value.payload.into()
    }
}

impl<N> TryFrom<FlashblockBuiltPayload<N>> for ExecutionPayloadEnvelopeV2
where
    N: NodePrimitives<Block = OpBlock>,
{
    type Error = ();
    fn try_from(value: FlashblockBuiltPayload<N>) -> Result<Self, Self::Error> {
        Ok(value.payload.into())
    }
}

impl<N> TryFrom<FlashblockBuiltPayload<N>> for OpExecutionPayloadEnvelopeV3
where
    N: NodePrimitives<Block = OpBlock>,
{
    type Error = ();
    fn try_from(value: FlashblockBuiltPayload<N>) -> Result<Self, Self::Error> {
        Ok(value.payload.into())
    }
}

impl<N> TryFrom<FlashblockBuiltPayload<N>> for OpExecutionPayloadEnvelopeV4
where
    N: NodePrimitives<Block = OpBlock>,
{
    type Error = ();
    fn try_from(value: FlashblockBuiltPayload<N>) -> Result<Self, Self::Error> {
        Ok(value.payload.into())
    }
}

impl<N: NodePrimitives<Block = OpBlock>> FlashblockBuiltPayload<N> {
    pub fn new(built_flashblock: Flashblock, payload: OpBuiltPayload<N>, block_index: u64) -> Self {
        Self {
            built_flashblock,
            payload,
            block_index,
        }
    }
}

impl<N: NodePrimitives<Block = OpBlock>> BuiltPayload for FlashblockBuiltPayload<N> {
    type Primitives = N;

    fn block(
        &self,
    ) -> &reth::core::primitives::SealedBlock<<Self::Primitives as NodePrimitives>::Block> {
        self.payload.block()
    }

    fn executed_block(&self) -> Option<ExecutedBlockWithTrieUpdates<Self::Primitives>> {
        self.payload.executed_block()
    }

    fn fees(&self) -> U256 {
        self.payload.fees()
    }

    fn requests(&self) -> Option<alloy_eips::eip7685::Requests> {
        self.payload.requests()
    }
}

/// Flashblocks Paylod builder
///
/// A payload builder
#[derive(Debug)]
pub struct FlashblocksPayloadBuilder<N, Pool, Client, CtxBuilder, Txs = ()> {
    /// The type responsible for creating the evm.
    pub evm_config: OpEvmConfig,
    /// Transaction pool.
    pub pool: Pool,
    /// Node client.
    pub client: Client,
    /// Settings for the builder, e.g. DA settings.
    pub config: FlashblocksBuilderConfig<OpPrimitives>,
    /// Iterator over best transactions from the pool.
    pub best_transactions: Txs,
    /// Context builder for the payload.
    pub ctx_builder: CtxBuilder,
    /// cancellation token
    pub cancel: CancelOnDrop,
    _marker: std::marker::PhantomData<N>,
}

impl<N, Pool, Client, CtxBuilder, Txs> Clone
    for FlashblocksPayloadBuilder<N, Pool, Client, CtxBuilder, Txs>
where
    Pool: Clone,
    Client: Clone,
    Txs: Clone,
    CtxBuilder: Clone,
    N: Clone,
{
    fn clone(&self) -> Self {
        Self {
            evm_config: self.evm_config.clone(),
            pool: self.pool.clone(),
            client: self.client.clone(),
            config: self.config.clone(),
            best_transactions: self.best_transactions.clone(),
            ctx_builder: self.ctx_builder.clone(),
            cancel: self.cancel.clone(),
            _marker: self._marker.clone(),
        }
    }
}

impl<N, Pool, Client, CtxBuilder, Txs> FlashblocksPayloadBuilder<N, Pool, Client, CtxBuilder, Txs>
where
    Txs: OpPayloadTransactions<Pool::Transaction>,
    Pool: TransactionPool<Transaction: OpPooledTx<Consensus = N::SignedTx>>,
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec: OpHardforks + EthChainSpec>,
    CtxBuilder: PayloadBuilderCtxBuilder<OpEvmConfig, OpChainSpec, Pool::Transaction>,
    N: NodePrimitives<Block = OpBlock, SignedTx = OpTxEnvelope, Receipt = OpReceipt>,
{
    /// Constructs an Optimism payload from the transactions sent via the
    /// Payload attributes by the sequencer. If the `no_tx_pool` argument is passed in
    /// the payload attributes, the transaction pool will be ignored and the only transactions
    /// included in the payload will be those sent through the attributes.
    ///
    /// Given build arguments including an Optimism client, transaction pool,
    /// and configuration, this function creates a transaction payload. Returns
    /// a result indicating success with the payload or an error in case of failure.    
    fn build_payload<'a, T>(
        &self,
        args: BuildArguments<OpPayloadBuilderAttributes<N::SignedTx>, FlashblockBuiltPayload<N>>,
        best: impl Fn(BestTransactionsAttributes) -> T + Send + Sync + 'a,
    ) -> Result<BuildOutcome<FlashblockBuiltPayload<N>>, PayloadBuilderError>
    where
        T: PayloadTransactions<Transaction = <Pool as TransactionPool>::Transaction>,
    {
        let BuildArguments {
            config,
            cached_reads,
            cancel,
            best_payload,
        } = args;

        let op_payload = best_payload
            .as_ref()
            .map(|best_payload| best_payload.payload.clone());

        let ctx = self.ctx_builder.build::<N, Client, Txs>(
            self.evm_config.clone(),
            self.config.da_config.da_config.clone(),
            self.client.chain_spec(),
            config,
            cancel,
            op_payload,
        );

        let builder = FlashblockBuilder::new(best);
        let state_provider = self.client.state_by_block_hash(ctx.parent().hash())?;

        if ctx.attributes().no_tx_pool {
            builder.build(&state_provider, &ctx, best_payload.clone())
        } else {
            // sequencer mode we can reuse cachedreads from previous runs
            builder.build(&state_provider, &ctx, best_payload)
        }
        .map(|out| out.with_cached_reads(cached_reads))
    }
}

impl<N, Pool, Client, CtxBuilder, Txs> PayloadBuilder
    for FlashblocksPayloadBuilder<N, Pool, Client, CtxBuilder, Txs>
where
    N: NodePrimitives<Block = OpBlock, SignedTx = OpTxEnvelope, Receipt = OpReceipt>,
    Client: Clone + StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
    Pool: TransactionPool<Transaction: OpPooledTx<Consensus = OpTxEnvelope>>,
    CtxBuilder: PayloadBuilderCtxBuilder<OpEvmConfig, Client::ChainSpec, Pool::Transaction>,
    Txs: OpPayloadTransactions<Pool::Transaction>,
{
    type Attributes = OpPayloadBuilderAttributes<OpTxEnvelope>;
    type BuiltPayload = FlashblockBuiltPayload<N>;

    fn try_build(
        &self,
        args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
    ) -> Result<BuildOutcome<Self::BuiltPayload>, PayloadBuilderError> {
        self.build_payload(args, |attrs| {
            self.best_transactions
                .best_transactions(self.pool.clone(), attrs)
        })
    }

    fn on_missing_payload(
        &self,
        _args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
    ) -> MissingPayloadBehaviour<Self::BuiltPayload> {
        // we want to await the job that's already in progress because that should be returned as
        // is, there's no benefit in racing another job
        MissingPayloadBehaviour::AwaitInProgress
    }

    fn build_empty_payload(
        &self,
        config: PayloadConfig<Self::Attributes, <N as NodePrimitives>::BlockHeader>,
    ) -> Result<Self::BuiltPayload, PayloadBuilderError> {
        let args = BuildArguments {
            config,
            cached_reads: Default::default(),
            cancel: Default::default(),
            best_payload: None,
        };
        self.build_payload(args, |_| {
            NoopPayloadTransactions::<Pool::Transaction>::default()
        })?
        .into_payload()
        .ok_or_else(|| PayloadBuilderError::MissingPayload)
    }
}

/// The type that builds the payload.
///
/// Payload building for optimism is composed of several steps.
/// The first steps are mandatory and defined by the protocol.
///
/// 1. first all System calls are applied.
/// 2. After canyon the forced deployed `create2deployer` must be loaded
/// 3. all sequencer transactions are executed (part of the payload attributes)
///
/// Depending on whether the node acts as a sequencer and is allowed to include additional
/// transactions (`no_tx_pool == false`):
/// 4. include additional transactions
///
/// And finally
/// 5. build the block: compute all roots (txs, state)
pub struct FlashblockBuilder<'a, Txs>
where
    Txs: PayloadTransactions,
{
    /// Yields the best transaction to include if transactions from the mempool are allowed.
    best: Box<dyn Fn(BestTransactionsAttributes) -> Txs + 'a>,
}

impl<'a, Txs> FlashblockBuilder<'a, Txs>
where
    Txs: PayloadTransactions,
{
    #[allow(clippy::too_many_arguments)]
    /// Creates a new [`FlashblockBuilder`].
    pub fn new(best: impl Fn(BestTransactionsAttributes) -> Txs + Send + Sync + 'a) -> Self {
        Self {
            best: Box::new(best),
        }
    }
}

impl<'a, Txs> FlashblockBuilder<'_, Txs>
where
    Txs: PayloadTransactions,
{
    /// Builds the payload on top of the state.
    pub fn build<N, Ctx, Tx>(
        self,
        state_provider: impl StateProvider + Clone,
        ctx: &Ctx,
        best: Option<FlashblockBuiltPayload<N>>,
    ) -> Result<BuildOutcomeKind<FlashblockBuiltPayload<N>>, PayloadBuilderError>
    where
        N: NodePrimitives<Block = OpBlock, SignedTx = OpTxEnvelope, Receipt = OpReceipt>,
        Tx: PoolTransaction<Consensus = N::SignedTx> + OpPooledTx,
        Txs: PayloadTransactions<Transaction = Tx>,
        Ctx: PayloadBuilderCtx<
            Evm = OpEvmConfig,
            Transaction = Tx,
            ChainSpec: OpHardforks + EthChainSpec,
        >,
    {
        let span = span!(
            tracing::Level::INFO,
            "flashblock_builder",
            id = %ctx.payload_id(),
        );

        let _enter = span.enter();

        debug!(target: "payload_builder", "building new payload");

        // 1. Prepare the db
        let state = StateProviderDatabase::new(&state_provider);
        let mut state = State::builder()
            .with_database(state)
            .with_bundle_update()
            .build();

        // 2. Create the block builder
        let mut builder = self.block_builder(&mut state, best.as_ref(), ctx)?;

        // 3. Execute Deposit transactions
        let info = ctx
            .execute_sequencer_transactions(&mut builder)
            .map_err(PayloadBuilderError::other)?;

        // 4. Build the block
        let build_outcome = builder.finish(&state_provider)?;

        // 5. Seal the block
        let BlockBuilderOutcome {
            execution_result,
            block,
            hashed_state,
            trie_updates,
        } = build_outcome;

        let sealed_block = Arc::new(block.sealed_block().clone());

        let execution_outcome = ExecutionOutcome::new(
            state.take_bundle(),
            vec![execution_result.receipts.clone()],
            block.number(),
            Vec::new(),
        );

        // create the executed block data
        let executed: ExecutedBlockWithTrieUpdates<N> = ExecutedBlockWithTrieUpdates {
            block: ExecutedBlock {
                recovered_block: Arc::new(block),
                execution_output: Arc::new(execution_outcome),
                hashed_state: Arc::new(hashed_state),
            },
            trie: ExecutedTrieUpdates::Present(Arc::new(trie_updates)),
        };

        let payload = OpBuiltPayload::<N>::new(
            ctx.payload_id(),
            sealed_block,
            info.total_fees,
            Some(executed),
        );

        let curr_index = best.as_ref().map_or(0, |b| b.block_index + 1);

        let executed_flashblock = Flashblock::new::<N, Ctx>(
            &payload,
            &ctx.attributes().clone(),
            &ctx.spec().clone(),
            curr_index,
            best.map_or(0, |b| b.payload.block().body().transaction_count()),
        );

        debug!(target: "payload_builder", id=%ctx.attributes().payload_id(), "built payload");

        let built_flashblock = FlashblockBuiltPayload::<N> {
            payload,
            built_flashblock: executed_flashblock,
            block_index: curr_index,
        };

        if ctx.attributes().no_tx_pool {
            // if `no_tx_pool` is set only transactions from the payload attributes will be included
            // in the payload. In other words, the payload is deterministic and we can
            // freeze it once we've successfully built it.
            Ok(BuildOutcomeKind::Freeze(built_flashblock))
        } else {
            // always better since we are re-using built payloads
            Ok(BuildOutcomeKind::Better {
                payload: built_flashblock,
            })
        }
    }

    pub fn block_builder<N, Ctx, DB, Tx, R, F, Builder, BB, I>(
        &self,
        db: &'a mut State<DB>,
        best: Option<&FlashblockBuiltPayload<N>>,
        ctx: &'a Ctx,
    ) -> Result<BB, PayloadBuilderError>
    where
        I: Inspector<<F::EvmFactory as EvmFactory>::Context<&'a mut State<DB>>>,
        R: OpReceiptBuilder<Transaction = N::SignedTx, Receipt = N::Receipt>,
        N: NodePrimitives<Block = OpBlock, SignedTx = OpTxEnvelope, Receipt = OpReceipt>,
        Tx: PoolTransaction<Consensus = N::SignedTx> + OpPooledTx,
        F: BlockExecutorFactory<
            Transaction = N::SignedTx,
            Receipt = N::Receipt,
            EvmFactory: EvmFactory<
                Spec = OpChainSpec,
                Tx: FromRecoveredTx<op_alloy_consensus::OpTxEnvelope>
                        + FromTxWithEncoded<op_alloy_consensus::OpTxEnvelope>,
                HaltReason = OpHaltReason,
            >,
        >,
        BB: BlockBuilder<
            Primitives = N,
            Executor = FlashblocksBlockExecutor<
                <<F as BlockExecutorFactory>::EvmFactory as EvmFactory>::Evm<
                    &'a mut revm::database::State<DB>,
                    I,
                >,
                R,
            >,
        >,
        DB: revm::Database + Debug + 'a,
        DB::Error: Send + Sync + 'static,
        Ctx: PayloadBuilderCtx<
            Evm = OpEvmConfig,
            Transaction = Tx,
            ChainSpec: OpHardforks + EthChainSpec,
        >,
    {

        
        let config = if let Some(FlashblockBuiltPayload {
            payload,
            block_index,
            ..
        }) = best
        {
            BlockBuilderConfig::new(payload, block_index + 1)
        } else {
            BlockBuilderConfig::default()
        };

        // Prepare EVM environment.
        let evm_env = ctx
            .evm_config()
            .next_evm_env(ctx.parent(), &attributes)
            .map_err(PayloadBuilderError::other)?;

        // Prepare EVM.
        let evm = ctx.evm_config().evm_with_env(db, evm_env);

        // Prepare block execution context.
        let execution_ctx = ctx
            .evm_config()
            .context_for_next_block(ctx.parent(), attributes);

        let ExecutionOutcome {
            bundle,
            receipts,
            requests: _,
            first_block,
        } = config.executed_block;

        let receipts = receipts.iter().flatten().cloned().collect::<Vec<_>>();

        let executor = FlashblocksBlockExecutor::new(
            evm,
            execution_ctx.clone(),
            ctx.spec().clone(),
            OpRethReceiptBuilder::default(),
        )
        .with_receipts(receipts)
        .with_gas_used(config.gas_used)
        .with_bundle_prestate(bundle);

        Ok(FlashblocksBlockBuilder::new(
            execution_ctx,
            ctx.parent(),
            executor,
            config.transactions,
            Arc::new(ctx.spec().clone()),
        ))
    }
}
