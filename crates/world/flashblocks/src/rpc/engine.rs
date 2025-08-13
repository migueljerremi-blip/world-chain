use std::{future::Future, pin::Pin, sync::Arc};

use alloy_consensus::{
    proofs::ordered_trie_root_with_encoder, Block, BlockBody, BlockHeader, Header,
};
use alloy_eips::eip7685::Requests;
use alloy_eips::Decodable2718;
use alloy_eips::Encodable2718;
use alloy_primitives::{BlockHash, FixedBytes, B256, U256, U64};
use alloy_rpc_types_engine::{
    ClientVersionV1, ExecutionPayloadBodiesV1, ExecutionPayloadInputV2, ExecutionPayloadV3,
    ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus,
};
use eyre::eyre::eyre;
use futures::{Stream, StreamExt};
use jsonrpsee_core::{async_trait, server::RpcModule, RpcResult};
use op_alloy_consensus::OpBlock;
use op_alloy_consensus::OpTxEnvelope;
use op_alloy_rpc_types_engine::{
    OpExecutionData, OpExecutionPayloadV4, ProtocolVersion, SuperchainSignal,
};
use reth::api::BlockBody as _;
use reth::{api::BuiltPayload, payload::PayloadBuilderAttributes};
use reth::{
    api::{Block as _, EngineApiValidator, EngineTypes},
    rpc::api::IntoEngineApiRpcModule,
    tasks::TaskSpawner,
};
use reth_chain_state::ExecutedBlockWithTrieUpdates;
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_evm::ConfigureEvm;
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_forks::OpHardforks;
use reth_optimism_node::{OpBuiltPayload, OpPayloadBuilderAttributes};
use reth_optimism_primitives::{OpPrimitives, OpReceipt};
use reth_optimism_rpc::{OpEngineApi, OpEngineApiServer};
use reth_primitives::{NodePrimitives, RecoveredBlock};
use reth_provider::ChainSpecProvider;
use reth_provider::{BlockReader, HeaderProvider, StateProviderFactory};
use reth_transaction_pool::{PoolTransaction, TransactionPool};
use rollup_boost::{
    ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, FlashblocksPayloadV1,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::PayloadBuilderCtx;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Eq)]
pub struct BlockMetaData<N: NodePrimitives> {
    pub fees: U256,
    pub receipts: Vec<OpReceipt>,
    pub transactions_root: B256,
    #[serde(skip)]
    pub executed: Option<ExecutedBlockWithTrieUpdates<N>>,
}

#[derive(Clone, Debug, PartialEq, Default, Deserialize, Serialize, Eq)]
pub struct Flashblock {
    flashblock: FlashblocksPayloadV1,
}

impl Flashblock {
    pub fn new<N, Ctx>(
        payload: &OpBuiltPayload<N>,
        attributes: &OpPayloadBuilderAttributes<N::SignedTx>,
        spec: &<Ctx as PayloadBuilderCtx>::ChainSpec,
        index: u64,
        transactions_offset: usize,
    ) -> Self
    where
        Ctx: PayloadBuilderCtx<ChainSpec: OpHardforks + EthChainSpec>,
        N: NodePrimitives<Block = OpBlock, SignedTx = OpTxEnvelope, Receipt = OpReceipt>,
    {
        let block = payload.block();
        let fees = payload.fees();

        // todo cache trie updated
        let payload_base = if index == 0 {
            Some(ExecutionPayloadBaseV1 {
                parent_beacon_block_root: attributes
                    .payload_attributes
                    .parent_beacon_block_root
                    .unwrap(),
                parent_hash: attributes.parent(),
                fee_recipient: attributes.payload_attributes.suggested_fee_recipient(),
                prev_randao: attributes.payload_attributes.prev_randao,
                block_number: block.number(),
                gas_limit: block.gas_limit(),
                timestamp: attributes.payload_attributes.timestamp,
                extra_data: attributes
                    .get_holocene_extra_data(
                        spec.base_fee_params_at_timestamp(attributes.timestamp()),
                    )
                    .unwrap_or_default(),
                base_fee_per_gas: block.base_fee_per_gas().map(U256::from).unwrap_or_default(),
            })
        } else {
            None
        };

        let transactions = block
            .body()
            .transactions_iter()
            .skip(transactions_offset)
            .map(|tx| tx.encoded_2718().into())
            .collect::<Vec<_>>();

        let metadata = BlockMetaData {
            fees,
            receipts: payload
                .executed_block()
                .map(|block| {
                    block
                        .execution_output
                        .receipts
                        .iter()
                        .skip(transactions_offset)
                        .cloned()
                        .flatten()
                        .collect()
                })
                .unwrap_or_default(),
            transactions_root: block.transactions_root(),
            executed: payload.executed_block(),
        };
        Flashblock {
            flashblock: FlashblocksPayloadV1 {
                payload_id: attributes.payload_id(),
                index: index,
                base: payload_base,
                diff: ExecutionPayloadFlashblockDeltaV1 {
                    state_root: block.state_root(),
                    receipts_root: block.receipts_root(),
                    logs_bloom: block.logs_bloom(),
                    gas_used: block.gas_used(),
                    block_hash: block.hash(),
                    transactions,
                    withdrawals: block
                        .body()
                        .withdrawals()
                        .cloned()
                        .unwrap_or_default()
                        .to_vec(),
                    withdrawals_root: block.withdrawals_root().unwrap_or_default(),
                },
                metadata: serde_json::to_value(metadata)
                    .expect("BlockMetaData should always serialize to JSON"),
            },
        }
    }
}

impl Flashblock {
    pub fn flashblock(&self) -> &FlashblocksPayloadV1 {
        &self.flashblock
    }

    pub fn payload_id(&self) -> &FixedBytes<8> {
        &self.flashblock.payload_id.0
    }

    pub fn base(&self) -> Option<&ExecutionPayloadBaseV1> {
        self.flashblock.base.as_ref()
    }

    pub fn diff(&self) -> &ExecutionPayloadFlashblockDeltaV1 {
        &self.flashblock.diff
    }
}

impl TryFrom<Flashblock> for RecoveredBlock<alloy_consensus::Block<OpTxEnvelope>> {
    type Error = eyre::Report;

    fn try_from(
        value: Flashblock,
    ) -> Result<RecoveredBlock<alloy_consensus::Block<OpTxEnvelope>>, Self::Error> {
        value
            .into_built_block()
            .ok_or(eyre!("Failed to convert Flashblock to BuiltFlashblock"))
    }
}

pub type Flashblocks = Vec<Flashblock>;

impl Flashblock {
    pub fn reduce(flashblocks: Flashblocks) -> Option<Flashblock> {
        let mut iter = flashblocks.into_iter();
        let mut acc = iter.next()?.flashblock;

        for next in iter {
            debug_assert_eq!(
                acc.payload_id, next.flashblock.payload_id,
                "all flashblocks should have the same payload_id"
            );

            if acc.base.is_none() && next.flashblock.base.is_some() {
                acc.base = next.flashblock.base;
            }

            acc.diff.gas_used += next.flashblock.diff.gas_used;

            acc.diff
                .transactions
                .extend(next.flashblock.diff.transactions);
            acc.diff
                .withdrawals
                .extend(next.flashblock.diff.withdrawals);

            acc.diff.state_root = next.flashblock.diff.state_root;
            acc.diff.receipts_root = next.flashblock.diff.receipts_root;
            acc.diff.logs_bloom = next.flashblock.diff.logs_bloom;
            acc.diff.block_hash = next.flashblock.diff.block_hash;
            acc.diff.withdrawals_root = next.flashblock.diff.withdrawals_root;
        }

        Some(Flashblock { flashblock: acc })
    }

    pub fn into_built_block(self) -> Option<RecoveredBlock<alloy_consensus::Block<OpTxEnvelope>>> {
        let base = self.flashblock.base.clone()?;
        let diff = self.flashblock.diff.clone();

        let header = Header {
            parent_beacon_block_root: Some(base.parent_beacon_block_root),
            state_root: diff.state_root,
            receipts_root: diff.receipts_root,
            logs_bloom: diff.logs_bloom,
            withdrawals_root: Some(diff.withdrawals_root),
            parent_hash: base.parent_hash,
            base_fee_per_gas: Some(base.base_fee_per_gas.to()),
            beneficiary: base.fee_recipient,
            transactions_root: ordered_trie_root_with_encoder(&diff.transactions, |tx, e| {
                *e = tx.as_ref().to_vec()
            }),
            ommers_hash: B256::ZERO,
            blob_gas_used: None,
            difficulty: U256::ZERO,
            number: base.block_number,
            gas_limit: base.gas_limit,
            gas_used: diff.gas_used,
            timestamp: base.timestamp,
            extra_data: base.extra_data.clone(),
            mix_hash: B256::ZERO,
            nonce: FixedBytes::default(),
            requests_hash: None,
            excess_blob_gas: None,
        };

        let transactions_encoded = diff
            .transactions
            .iter()
            .cloned()
            .map(|t| <OpPrimitives as NodePrimitives>::SignedTx::decode_2718(&mut t.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .ok()?;

        let body = BlockBody {
            transactions: transactions_encoded,
            withdrawals: Some(alloy_eips::eip4895::Withdrawals(diff.withdrawals.to_vec())),
            ommers: vec![],
        };

        let block = Block::new(header, body).try_into_recovered().ok()?;

        Some(block)
    }
}
/// The current state of all known pre confirmations received over the P2P layer
/// or generated from the payload building job of this node.
///
/// The state is flushed when FCU is received with a parent hash that matches the block hash
/// of the latest pre confirmation _or_ when an FCU is received that does not match the latest pre confirmation,
/// in which case the pre confirmations were not included as part of the canonical chain.
#[derive(Debug, Clone)]
pub struct FlashblocksState(pub Arc<RwLock<Flashblocks>>);

impl Default for FlashblocksState {
    fn default() -> Self {
        Self::new()
    }
}

impl FlashblocksState {
    /// Creates a new instance of [`FlashblocksState`].
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(Vec::new())))
    }

    /// Returns a reference to the latest flashblock.
    pub async fn last(&self) -> Option<FlashblocksPayloadV1> {
        self.0.read().await.last().map(|f| f.flashblock().clone())
    }

    /// Appends a new flashblock to the state.
    pub async fn push(&self, payload: FlashblocksPayloadV1) {
        let mut state = self.0.write().await;
        state.retain(|p| *p.payload_id() == payload.payload_id.0);
        state.push(Flashblock {
            flashblock: payload,
        });
    }

    /// Clears the current state of flashblocks.
    pub async fn clear(&self) {
        self.0.write().await.clear();
    }
}

#[derive(Debug)]
pub struct OpEngineApiExt<Provider, EngineT: EngineTypes, Pool, Validator, ChainSpec> {
    /// The inner [`OpEngineApi`] instance that this extension wraps.
    inner: OpEngineApi<Provider, EngineT, Pool, Validator, ChainSpec>,
    /// The current store of all pre confirmations ahead of the canonical chain.
    flashblocks_state: FlashblocksState,
}

impl<Provider, EngineT: EngineTypes, Pool, Validator, ChainSpec>
    OpEngineApiExt<Provider, EngineT, Pool, Validator, ChainSpec>
{
    /// Creates a new instance of [`OpEngineApiExt`], and spawns a task to handle incoming flashblocks.
    pub fn new(
        inner: OpEngineApi<Provider, EngineT, Pool, Validator, ChainSpec>,
        flashblocks_state: FlashblocksState,
        executor: impl TaskSpawner,
        stream: impl Stream<Item = FlashblocksPayloadV1> + Send + Unpin + 'static,
    ) -> Self {
        executor.spawn_critical(
            "subscription_handle",
            Self::spawn_subscription_handle(stream, flashblocks_state.clone()),
        );

        Self {
            inner,
            flashblocks_state,
        }
    }

    /// Returns a reference to the inner [`FlashblocksState`].
    pub fn flashblocks_state(&self) -> FlashblocksState {
        self.flashblocks_state.clone()
    }

    /// Spawns a task _solely_ responsible for appending new flashblocks to the state.
    /// Flushing happens when FCU's arrive with parent attributes matching the latest pre confirmed block hash.
    fn spawn_subscription_handle(
        mut stream: impl Stream<Item = FlashblocksPayloadV1> + Send + Unpin + 'static,
        flashblocks_state: FlashblocksState,
    ) -> Pin<Box<impl Future<Output = ()> + Send + 'static>> {
        Box::pin(async move {
            while let Some(payload) = stream.next().await {
                flashblocks_state.push(payload).await;
            }
        })
    }
}

#[async_trait]
impl<Provider, EngineT, Pool, Validator, ChainSpec> OpEngineApiServer<EngineT>
    for OpEngineApiExt<Provider, EngineT, Pool, Validator, ChainSpec>
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    EngineT: EngineTypes<ExecutionData = OpExecutionData>,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EngineT>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    async fn new_payload_v2(&self, payload: ExecutionPayloadInputV2) -> RpcResult<PayloadStatus> {
        Ok(self.inner.new_payload_v2(payload).await?)
    }

    async fn new_payload_v3(
        &self,
        payload: ExecutionPayloadV3,
        versioned_hashes: Vec<B256>,
        parent_beacon_block_root: B256,
    ) -> RpcResult<PayloadStatus> {
        Ok(self
            .inner
            .new_payload_v3(payload, versioned_hashes, parent_beacon_block_root)
            .await?)
    }

    async fn new_payload_v4(
        &self,
        payload: OpExecutionPayloadV4,
        versioned_hashes: Vec<B256>,
        parent_beacon_block_root: B256,
        execution_requests: Requests,
    ) -> RpcResult<PayloadStatus> {
        Ok(self
            .inner
            .new_payload_v4(
                payload,
                versioned_hashes,
                parent_beacon_block_root,
                execution_requests,
            )
            .await?)
    }

    async fn fork_choice_updated_v1(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<EngineT::PayloadAttributes>,
    ) -> RpcResult<ForkchoiceUpdated> {
        let (res, _) = tokio::join!(
            self.inner
                .fork_choice_updated_v1(fork_choice_state, payload_attributes),
            self.handle_fork_choice_updated(fork_choice_state)
        );
        Ok(res?)
    }

    async fn fork_choice_updated_v2(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<EngineT::PayloadAttributes>,
    ) -> RpcResult<ForkchoiceUpdated> {
        let (res, _) = tokio::join!(
            self.inner
                .fork_choice_updated_v2(fork_choice_state, payload_attributes),
            self.handle_fork_choice_updated(fork_choice_state)
        );
        Ok(res?)
    }

    async fn fork_choice_updated_v3(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<EngineT::PayloadAttributes>,
    ) -> RpcResult<ForkchoiceUpdated> {
        let (res, _) = tokio::join!(
            self.inner
                .fork_choice_updated_v3(fork_choice_state, payload_attributes),
            self.handle_fork_choice_updated(fork_choice_state)
        );
        Ok(res?)
    }

    async fn get_payload_v2(
        &self,
        payload_id: PayloadId,
    ) -> RpcResult<EngineT::ExecutionPayloadEnvelopeV2> {
        Ok(self.inner.get_payload_v2(payload_id).await?)
    }

    async fn get_payload_v3(
        &self,
        payload_id: PayloadId,
    ) -> RpcResult<EngineT::ExecutionPayloadEnvelopeV3> {
        Ok(self.inner.get_payload_v3(payload_id).await?)
    }

    async fn get_payload_v4(
        &self,
        payload_id: PayloadId,
    ) -> RpcResult<EngineT::ExecutionPayloadEnvelopeV4> {
        Ok(self.inner.get_payload_v4(payload_id).await?)
    }

    async fn get_payload_bodies_by_hash_v1(
        &self,
        block_hashes: Vec<BlockHash>,
    ) -> RpcResult<ExecutionPayloadBodiesV1> {
        Ok(self
            .inner
            .get_payload_bodies_by_hash_v1(block_hashes)
            .await?)
    }

    async fn get_payload_bodies_by_range_v1(
        &self,
        start: U64,
        count: U64,
    ) -> RpcResult<ExecutionPayloadBodiesV1> {
        Ok(self
            .inner
            .get_payload_bodies_by_range_v1(start, count)
            .await?)
    }

    async fn signal_superchain_v1(&self, signal: SuperchainSignal) -> RpcResult<ProtocolVersion> {
        Ok(self.inner.signal_superchain_v1(signal).await?)
    }

    async fn get_client_version_v1(
        &self,
        client: ClientVersionV1,
    ) -> RpcResult<Vec<ClientVersionV1>> {
        Ok(self.inner.get_client_version_v1(client).await?)
    }

    async fn exchange_capabilities(&self, _capabilities: Vec<String>) -> RpcResult<Vec<String>> {
        Ok(self.inner.exchange_capabilities(_capabilities).await?)
    }
}

impl<Provider, EngineT, Pool, Validator, ChainSpec>
    OpEngineApiExt<Provider, EngineT, Pool, Validator, ChainSpec>
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    EngineT: EngineTypes<ExecutionData = OpExecutionData>,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EngineT>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    /// Handles a [`ForkchoiceState`] update by checking if the latest flashblock matches the
    /// `head_block_hash` of the `ForkchoiceState`. If it does, it clears the flashblocks state.
    ///
    /// It is up to the consumer of [`FlashblocksState`] to ensure that the block number of the latest
    /// flashblock is 1 + latest block number in the canonical chain.
    pub async fn handle_fork_choice_updated(&self, fork_choice_state: ForkchoiceState) {
        let confirmed = self
            .flashblocks_state
            .last()
            .await
            .map(|p| p.diff.block_hash == fork_choice_state.head_block_hash);

        if confirmed.unwrap_or(false) {
            self.flashblocks_state.clear().await;
        }
    }
}

impl<Provider, EngineT, Pool, Validator, ChainSpec> IntoEngineApiRpcModule
    for OpEngineApiExt<Provider, EngineT, Pool, Validator, ChainSpec>
where
    EngineT: EngineTypes,
    Self: OpEngineApiServer<EngineT>,
{
    fn into_rpc_module(self) -> RpcModule<()> {
        self.into_rpc().remove_context()
    }
}
