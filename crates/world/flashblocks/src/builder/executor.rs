use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use alloy_consensus::{Block, Eip658Value, Header, Receipt, Transaction, TxReceipt};
use alloy_eips::{Encodable2718, Typed2718};
use alloy_op_evm::block::receipt_builder::OpReceiptBuilder;
use alloy_op_evm::{OpBlockExecutionCtx, OpBlockExecutorFactory, OpEvm, OpEvmFactory};
use alloy_primitives::{address, b256, hex, Address, Bloom, Bytes, B256};
use clap::builder;
use op_alloy_consensus::{transaction, OpBlock, OpDepositReceipt, OpTxEnvelope, OpTxReceipt};
use reth::revm::State;
use reth_chainspec::{ChainSpec, EthChainSpec, EthereumHardforks};
use reth_evm::block::{
    BlockExecutorFactory, BlockExecutorFor, BlockValidationError, StateChangePostBlockSource,
    StateChangeSource, SystemCaller,
};
use reth_evm::eth::receipt_builder::ReceiptBuilderCtx;
use reth_evm::execute::{
    BasicBlockBuilder, BlockAssembler, BlockAssemblerInput, BlockBuilder, BlockBuilderOutcome,
    ExecutorTx,
};
use reth_evm::op_revm::transaction::deposit::DEPOSIT_TRANSACTION_TYPE;
use reth_evm::op_revm::OpHaltReason;
use reth_evm::state_change::{balance_increment_state, post_block_balance_increments};
use reth_evm::{
    block::{BlockExecutionError, BlockExecutor, CommitChanges, ExecutableTx},
    Database, FromRecoveredTx, FromTxWithEncoded, OnStateHook,
};
use reth_evm::{
    ConfigureEngineEvm, ConfigureEvm, Evm, EvmContextFor, EvmEnv, EvmEnvFor, EvmErrorFor,
    EvmFactory, EvmFactoryFor, ExecutionCtxFor,
};
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_forks::OpHardforks;
use reth_optimism_node::{OpBlockAssembler, OpRethReceiptBuilder};
use reth_optimism_primitives::{DepositReceipt, OpPrimitives, OpReceipt, OpTransactionSigned};
use reth_primitives::{transaction::SignedTransaction, SealedHeader};
use reth_primitives::{NodePrimitives, Recovered};
use reth_provider::{BlockExecutionResult, StateProvider};
use revm::context::result::{ExecutionResult, ResultAndState};
use revm::context::ContextTr;
use revm::database::BundleState;
use revm::inspector::NoOpInspector;
use revm::primitives::HashMap;
use revm::state::Bytecode;
use revm::{inspector, Context, DatabaseCommit, Inspector};
use rollup_boost::ed25519_dalek::ed25519::signature::rand_core::le;
use rollup_boost::Flashblocks;

use crate::builder::executor;

/// The address of the create2 deployer
const CREATE_2_DEPLOYER_ADDR: Address = address!("0x13b0D85CcB8bf860b6b79AF3029fCA081AE9beF2");

/// The codehash of the create2 deployer contract.
const CREATE_2_DEPLOYER_CODEHASH: B256 =
    b256!("0xb0550b5b431e30d38000efb7107aaa0ade03d48a7198a140edda9d27134468b2");

/// The raw bytecode of the create2 deployer contract.
const CREATE_2_DEPLOYER_BYTECODE: [u8; 1584] = hex!("6080604052600436106100435760003560e01c8063076c37b21461004f578063481286e61461007157806356299481146100ba57806366cfa057146100da57600080fd5b3661004a57005b600080fd5b34801561005b57600080fd5b5061006f61006a366004610327565b6100fa565b005b34801561007d57600080fd5b5061009161008c366004610327565b61014a565b60405173ffffffffffffffffffffffffffffffffffffffff909116815260200160405180910390f35b3480156100c657600080fd5b506100916100d5366004610349565b61015d565b3480156100e657600080fd5b5061006f6100f53660046103ca565b610172565b61014582826040518060200161010f9061031a565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe082820381018352601f90910116604052610183565b505050565b600061015683836102e7565b9392505050565b600061016a8484846102f0565b949350505050565b61017d838383610183565b50505050565b6000834710156101f4576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601d60248201527f437265617465323a20696e73756666696369656e742062616c616e636500000060448201526064015b60405180910390fd5b815160000361025f576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820181905260248201527f437265617465323a2062797465636f6465206c656e677468206973207a65726f60448201526064016101eb565b8282516020840186f5905073ffffffffffffffffffffffffffffffffffffffff8116610156576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601960248201527f437265617465323a204661696c6564206f6e206465706c6f790000000000000060448201526064016101eb565b60006101568383305b6000604051836040820152846020820152828152600b8101905060ff815360559020949350505050565b61014e806104ad83390190565b6000806040838503121561033a57600080fd5b50508035926020909101359150565b60008060006060848603121561035e57600080fd5b8335925060208401359150604084013573ffffffffffffffffffffffffffffffffffffffff8116811461039057600080fd5b809150509250925092565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b6000806000606084860312156103df57600080fd5b8335925060208401359150604084013567ffffffffffffffff8082111561040557600080fd5b818601915086601f83011261041957600080fd5b81358181111561042b5761042b61039b565b604051601f82017fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0908116603f011681019083821181831017156104715761047161039b565b8160405282815289602084870101111561048a57600080fd5b826020860160208301376000602084830101528095505050505050925092509256fe608060405234801561001057600080fd5b5061012e806100206000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c8063249cb3fa14602d575b600080fd5b603c603836600460b1565b604e565b60405190815260200160405180910390f35b60008281526020818152604080832073ffffffffffffffffffffffffffffffffffffffff8516845290915281205460ff16608857600060aa565b7fa2ef4600d742022d532d4747cb3547474667d6f13804902513b2ec01c848f4b45b9392505050565b6000806040838503121560c357600080fd5b82359150602083013573ffffffffffffffffffffffffffffffffffffffff8116811460ed57600080fd5b80915050925092905056fea26469706673582212205ffd4e6cede7d06a5daf93d48d0541fc68189eeb16608c1999a82063b666eb1164736f6c63430008130033a2646970667358221220fdc4a0fe96e3b21c108ca155438d37c9143fb01278a3c1d274948bad89c564ba64736f6c63430008130033");

/// A Block Executor for Optimism that can load pre state from previous flashblocks.
#[derive(Debug)]
pub struct FlashblocksBlockExecutor<Evm, R: OpReceiptBuilder> {
    /// OpChainSpec.
    spec: OpChainSpec,
    /// Receipt builder.
    receipt_builder: R,
    /// Context for block execution.
    ctx: OpBlockExecutionCtx,
    /// The EVM used by executor.
    evm: Evm,
    /// Receipts of executed transactions.
    receipts: Vec<R::Receipt>,
    /// Total gas used by executed transactions.
    gas_used: u64,
    /// Whether Regolith hardfork is active.
    is_regolith: bool,
    /// Utility to call system smart contracts.
    system_caller: SystemCaller<OpChainSpec>,
}

impl<'db, DB, E, R> FlashblocksBlockExecutor<E, R>
where
    DB: Database + 'db,
    E: Evm<
        Spec = OpChainSpec,
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
    >,
    R: OpReceiptBuilder<Transaction = OpTxEnvelope, Receipt = OpReceipt>,
{
    /// Creates a new [`OpBlockExecutor`].
    pub fn new(evm: E, ctx: OpBlockExecutionCtx, spec: OpChainSpec, receipt_builder: R) -> Self {
        Self {
            is_regolith: spec
                .is_regolith_active_at_timestamp(evm.block().timestamp.saturating_to()),
            evm,
            system_caller: SystemCaller::new(spec.clone()),
            spec,
            receipt_builder,
            receipts: Vec::new(),
            gas_used: 0,
            ctx,
        }
    }

    /// Extends the [`BundleState`] of the executor with a specified pre-image.
    ///
    /// This should be used _only_ when initializing the executor
    pub fn with_bundle_prestate(mut self, pre_state: BundleState) -> Self {
        self.evm_mut().db_mut().bundle_state.extend(pre_state);
        self
    }

    /// Extends the receipts to reflect the aggregated execution result
    pub fn with_receipts(mut self, receipts: Vec<R::Receipt>) -> Self {
        self.receipts.extend_from_slice(&receipts);
        self
    }

    /// Extends the gas used to reflect the aggregated execution result
    pub fn with_gas_used(mut self, gas_used: u64) -> Self {
        self.gas_used += gas_used;
        self
    }
}

impl<'db, DB, E, R> BlockExecutor for FlashblocksBlockExecutor<E, R>
where
    DB: Database + 'db,
    E: Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
        Spec = OpChainSpec,
    >,
    R: OpReceiptBuilder<Transaction: Transaction + Encodable2718, Receipt: TxReceipt>,
{
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;
    type Evm = E;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag = self
            .spec
            .is_spurious_dragon_active_at_block(self.evm.block().number.saturating_to());
        self.evm.db_mut().set_state_clear_flag(state_clear_flag);

        self.system_caller
            .apply_blockhashes_contract_call(self.ctx.parent_hash, &mut self.evm)?;
        self.system_caller
            .apply_beacon_root_contract_call(self.ctx.parent_beacon_block_root, &mut self.evm)?;

        // Ensure that the create2deployer is force-deployed at the canyon transition. Optimism
        // blocks will always have at least a single transaction in them (the L1 info transaction),
        // so we can safely assume that this will always be triggered upon the transition and that
        // the above check for empty blocks will never be hit on OP chains.
        //
        // If the canyon hardfork is active at the current timestamp, and it was not active at the
        // previous block timestamp (heuristically, block time is not perfectly constant at 2s), and the
        // chain is an optimism chain, then we need to force-deploy the create2 deployer contract.
        if self
            .spec
            .is_canyon_active_at_timestamp(self.evm.block().timestamp.saturating_to())
            && !self.spec.is_canyon_active_at_timestamp(
                self.evm
                    .block()
                    .timestamp
                    .saturating_to::<u64>()
                    .saturating_sub(2),
            )
        {
            // Load the create2 deployer account from the cache.
            let acc = self
                .evm
                .db_mut()
                .load_cache_account(CREATE_2_DEPLOYER_ADDR)
                .map_err(BlockExecutionError::other)?;

            // Update the account info with the create2 deployer codehash and bytecode.
            let mut acc_info = acc.account_info().unwrap_or_default();
            acc_info.code_hash = CREATE_2_DEPLOYER_CODEHASH;
            acc_info.code = Some(Bytecode::new_raw(Bytes::from_static(
                &CREATE_2_DEPLOYER_BYTECODE,
            )));

            // Convert the cache account back into a revm account and mark it as touched.
            let mut revm_acc: revm::state::Account = acc_info.into();
            revm_acc.mark_touch();

            // Commit the create2 deployer account to the database.
            self.evm_mut()
                .db_mut()
                .commit(HashMap::from_iter([(CREATE_2_DEPLOYER_ADDR, revm_acc)]));
            return Ok(());
        }

        Ok(())
    }

    fn execute_transaction_with_commit_condition(
        &mut self,
        tx: impl ExecutableTx<Self>,
        f: impl FnOnce(&ExecutionResult<<Self::Evm as Evm>::HaltReason>) -> CommitChanges,
    ) -> Result<Option<u64>, BlockExecutionError> {
        let is_deposit = tx.tx().ty() == DEPOSIT_TRANSACTION_TYPE;

        // The sum of the transaction’s gas limit, Tg, and the gas utilized in this block prior,
        // must be no greater than the block’s gasLimit.
        let block_available_gas = self.evm.block().gas_limit - self.gas_used;
        if tx.tx().gas_limit() > block_available_gas && (self.is_regolith || !is_deposit) {
            return Err(
                BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                    transaction_gas_limit: tx.tx().gas_limit(),
                    block_available_gas,
                }
                .into(),
            );
        }

        // Cache the depositor account prior to the state transition for the deposit nonce.
        //
        // Note that this *only* needs to be done post-regolith hardfork, as deposit nonces
        // were not introduced in Bedrock. In addition, regular transactions don't have deposit
        // nonces, so we don't need to touch the DB for those.
        let depositor = (self.is_regolith && is_deposit)
            .then(|| {
                self.evm
                    .db_mut()
                    .load_cache_account(*tx.signer())
                    .map(|acc| acc.account_info().unwrap_or_default())
            })
            .transpose()
            .map_err(BlockExecutionError::other)?;

        let hash = tx.tx().trie_hash();

        // Execute transaction.
        let ResultAndState { result, state } = self
            .evm
            .transact(&tx)
            .map_err(move |err| BlockExecutionError::evm(err, hash))?;

        if !f(&result).should_commit() {
            return Ok(None);
        }

        self.system_caller
            .on_state(StateChangeSource::Transaction(self.receipts.len()), &state);

        let gas_used = result.gas_used();

        // append gas used
        self.gas_used += gas_used;

        self.receipts.push(
            match self.receipt_builder.build_receipt(ReceiptBuilderCtx {
                tx: tx.tx(),
                result,
                cumulative_gas_used: self.gas_used,
                evm: &self.evm,
                state: &state,
            }) {
                Ok(receipt) => receipt,
                Err(ctx) => {
                    let receipt = alloy_consensus::Receipt {
                        // Success flag was added in `EIP-658: Embedding transaction status code
                        // in receipts`.
                        status: Eip658Value::Eip658(ctx.result.is_success()),
                        cumulative_gas_used: self.gas_used,
                        logs: ctx.result.into_logs(),
                    };

                    self.receipt_builder
                        .build_deposit_receipt(OpDepositReceipt {
                            inner: receipt,
                            deposit_nonce: depositor.map(|account| account.nonce),
                            // The deposit receipt version was introduced in Canyon to indicate an
                            // update to how receipt hashes should be computed
                            // when set. The state transition process ensures
                            // this is only set for post-Canyon deposit
                            // transactions.
                            deposit_receipt_version: (is_deposit
                                && self.spec.is_canyon_active_at_timestamp(
                                    self.evm.block().timestamp.saturating_to(),
                                ))
                            .then_some(1),
                        })
                }
            },
        );

        self.evm.db_mut().commit(state);

        Ok(Some(gas_used))
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<R::Receipt>), BlockExecutionError> {
        let balance_increments =
            post_block_balance_increments::<Header>(&self.spec, self.evm.block(), &[], None);
        // increment balances
        self.evm
            .db_mut()
            .increment_balances(balance_increments.clone())
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;
        // call state hook with changes due to balance increments.
        self.system_caller.try_on_state_with(|| {
            balance_increment_state(&balance_increments, self.evm.db_mut()).map(|state| {
                (
                    StateChangeSource::PostBlock(StateChangePostBlockSource::BalanceIncrements),
                    Cow::Owned(state),
                )
            })
        })?;

        let gas_used = self
            .receipts
            .last()
            .map(|r| r.cumulative_gas_used())
            .unwrap_or_default();
        Ok((
            self.evm,
            BlockExecutionResult {
                receipts: self.receipts,
                requests: Default::default(),
                gas_used,
            },
        ))
    }

    fn set_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        self.system_caller.with_state_hook(hook);
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }
}

/// Ethereum block executor factory.
#[derive(Debug, Clone)]
pub struct FlashblocksBlockExecutorFactory<Evm, R> {
    inner: OpBlockExecutorFactory<R, OpChainSpec, Evm>,
    pre_state: Option<BundleState>,
}

impl<Evm, R> FlashblocksBlockExecutorFactory<Evm, R> {
    /// Creates a new [`OpBlockExecutorFactory`] with the given spec, [`EvmFactory`], and
    /// [`OpReceiptBuilder`].
    pub const fn new(receipt_builder: R, spec: OpChainSpec, evm_factory: Evm) -> Self {
        Self {
            inner: OpBlockExecutorFactory::new(receipt_builder, spec, evm_factory),
            pre_state: None,
        }
    }

    /// Exposes the chain specification.
    pub const fn spec(&self) -> &OpChainSpec {
        self.inner.spec()
    }

    /// Exposes the EVM factory.
    pub const fn evm_factory(&self) -> &Evm {
        self.inner.evm_factory()
    }

    pub const fn take_bundle(&mut self) -> Option<BundleState> {
        self.pre_state.take()
    }

    /// Sets the pre-state for the block executor factory.
    pub fn set_pre_state(&mut self, pre_state: BundleState) {
        self.pre_state = Some(pre_state);
    }
}

impl<Evm, R> BlockExecutorFactory for FlashblocksBlockExecutorFactory<Evm, R>
where
    R: OpReceiptBuilder<Transaction = OpTxEnvelope, Receipt = OpReceipt> + Default + 'static,
    Evm: EvmFactory<
            Spec = OpChainSpec,
            Tx: FromRecoveredTx<op_alloy_consensus::OpTxEnvelope>
                    + FromTxWithEncoded<op_alloy_consensus::OpTxEnvelope>,
        > + 'static,
{
    type EvmFactory = Evm;
    type ExecutionCtx<'a> = OpBlockExecutionCtx;
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.inner.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: <Evm as EvmFactory>::Evm<&'a mut State<DB>, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: revm::Inspector<<Evm as EvmFactory>::Context<&'a mut State<DB>>> + 'a,
    {
        if let Some(pre_state) = &self.pre_state {
            return FlashblocksBlockExecutor::new(evm, ctx, self.spec().clone(), R::default())
                .with_bundle_prestate(pre_state.clone()); // TODO: Terrible clone here
        }

        FlashblocksBlockExecutor::new(evm, ctx, self.spec().clone(), R::default())
    }
}

/// Block builder for Optimism.
#[derive(Debug)]
pub struct FlashblocksBlockAssembler {
    inner: OpBlockAssembler<OpChainSpec>,
}

impl FlashblocksBlockAssembler {
    /// Creates a new [`OpBlockAssembler`].
    pub const fn new(chain_spec: Arc<OpChainSpec>) -> Self {
        Self {
            inner: OpBlockAssembler::new(chain_spec),
        }
    }
}

impl FlashblocksBlockAssembler {
    /// Builds a block for `input` without any bounds on header `H`.
    pub fn assemble_block<
        F: for<'a> BlockExecutorFactory<
            ExecutionCtx<'a> = OpBlockExecutionCtx,
            Transaction: SignedTransaction,
            Receipt: TxReceipt + DepositReceipt,
        >,
        H,
    >(
        &self,
        input: BlockAssemblerInput<'_, '_, F, H>,
    ) -> Result<Block<F::Transaction>, BlockExecutionError> {
        self.inner.assemble_block(input)
    }
}

impl Clone for FlashblocksBlockAssembler {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<F> BlockAssembler<F> for FlashblocksBlockAssembler
where
    F: for<'a> BlockExecutorFactory<
        ExecutionCtx<'a> = OpBlockExecutionCtx,
        Transaction: SignedTransaction,
        Receipt: TxReceipt + DepositReceipt,
    >,
{
    type Block = Block<F::Transaction>;

    fn assemble_block(
        &self,
        input: BlockAssemblerInput<'_, '_, F>,
    ) -> Result<Self::Block, BlockExecutionError> {
        self.assemble_block(input)
    }
}

/// A wrapper around the [`BasicBlockBuilder`] for flashblocks.
pub struct FlashblocksBlockBuilder<'a, F, DB, I = NoOpInspector, N: NodePrimitives = OpPrimitives> where F: BlockExecutorFactory, I: Inspector<EvmContextFor<F::EvmFactory>>, DB: Database + 'a {
    pub inner: BasicBlockBuilder<
        'a,
        F,
        FlashblocksBlockExecutor<
            OpEvm<&'a mut State<DB>, I>,
            OpRethReceiptBuilder: OpReceiptBuilder<
                Transaction = N::SignedTx,
                Receipt = N::Receipt,
             >,
        Builder,
        N,
    >
}

impl<'a, R, F, Builder, N, DB> FlashblocksBlockBuilder<'a, R, F, Builder, N, DB, NoOpInspector>
where
    F: BlockExecutorFactory<
        Transaction = N::SignedTx,
        Receipt = N::Receipt,
        EvmFactory: EvmFactory<
            Spec = OpChainSpec,
            Tx: FromRecoveredTx<N::SignedTx> + FromTxWithEncoded<N::SignedTx>,
            Evm<DB, NoOpInspector> = OpEvm<&'a mut State<DB>, NoOpInspector>,
        >,
    >,
    FlashblocksBlockExecutor<
        <<F as BlockExecutorFactory>::EvmFactory as EvmFactory>::Evm<
            &'a mut revm::database::State<DB>,
            NoOpInspector,
        >,
        R,
    >: BlockExecutor,
    R: OpReceiptBuilder<Transaction = N::SignedTx, Receipt = N::Receipt> + Default,
    DB: Database + 'a,
    Builder: BlockAssembler<F, Block = N::Block>,
    N: NodePrimitives<Block = OpBlock, SignedTx = OpTxEnvelope, Receipt = OpReceipt>,
{
    /// Creates a new [`FlashblocksBlockBuilder`] with the given executor factory, and assembler
    pub fn new(
        ctx: F::ExecutionCtx<'a>,
        parent: &'a SealedHeader<N::BlockHeader>,
        assembler: Builder,
        factory: F,
        db: &'a mut State<DB>,
        env: EvmEnv<<<F as BlockExecutorFactory>::EvmFactory as EvmFactory>::Spec>,
        spec: OpChainSpec,
    ) -> FlashblocksBlockBuilder<'a, R, F, Builder, N, DB, NoOpInspector> {
        // Prepare EVM.
        let evm = factory.evm_factory().create_evm(db, env);
        let executor = FlashblocksBlockExecutor::new(evm, ctx.clone(), spec, R::default());
        let transactions = Vec::new();

        Self {
            inner: BasicBlockBuilder {
                executor,
                assembler,
                ctx,
                parent,
                transactions,
            },
            _marker: PhantomData,
        }
    }

    /// Creates a new [`FlashblocksBlockBuilder`] with the given executor factory, assembler, and inspector.
    pub fn new_with_inspector<I>(
        ctx: F::ExecutionCtx<'a>,
        parent: &'a SealedHeader<N::BlockHeader>,
        assembler: Builder,
        factory: F,
        inspector: I,
        db: &'a mut State<DB>,
        env: EvmEnv<OpChainSpec>,
    ) -> FlashblocksBlockBuilder<'a, R, F, Builder, N, DB, I>
    where
        I: revm::Inspector<<F::EvmFactory as EvmFactory>::Context<&'a mut State<DB>>> + 'static,
        FlashblocksBlockExecutor<
            <<F as BlockExecutorFactory>::EvmFactory as EvmFactory>::Evm<
                &'a mut revm::database::State<DB>,
                I,
            >,
            R,
        >: BlockExecutor<
            Evm: Evm<Spec = OpChainSpec, DB = &'a mut State<DB>, Inspector = I>,
            Transaction = N::SignedTx,
            Receipt = N::Receipt,
        >,
    {
        // Prepare EVM.
        let evm = factory
            .evm_factory()
            .create_evm_with_inspector(db, env.clone(), inspector);

        let executor = FlashblocksBlockExecutor::new(evm, ctx, env.spec_id().clone(), R::default());
        let transactions = Vec::new();
        let ctx = executor.ctx.clone();
        FlashblocksBlockBuilder {
            inner: BasicBlockBuilder {
                executor,
                assembler,
                ctx,
                parent,
                transactions,
            },
            _marker: PhantomData,
        }
    }

    pub fn with_transactions(mut self, transactions: Vec<Recovered<N::SignedTx>>) -> Self {
        self.inner.transactions = transactions;
        self
    }

    pub fn with_executor<I>(
        self,
        executor: FlashblocksBlockExecutor<
            <<F as BlockExecutorFactory>::EvmFactory as EvmFactory>::Evm<&'a mut State<DB>, I>,
            R,
        >,
    ) -> FlashblocksBlockBuilder<'a, R, F, Builder, N, DB, I>
    where
        I: revm::Inspector<<F::EvmFactory as EvmFactory>::Context<&'a mut State<DB>>> + 'static,
        FlashblocksBlockExecutor<
            <<F as BlockExecutorFactory>::EvmFactory as EvmFactory>::Evm<
                &'a mut revm::database::State<DB>,
                I,
            >,
            R,
        >: BlockExecutor<
            Evm: Evm<Spec = OpChainSpec, DB = &'a mut State<DB>, Inspector = I>,
            Transaction = N::SignedTx,
            Receipt = N::Receipt,
        >,
    {
        FlashblocksBlockBuilder {
            inner: BasicBlockBuilder {
                ctx: self.inner.ctx,
                parent: self.inner.parent,
                executor,
                assembler: self.inner.assembler,
                transactions: self.inner.transactions,
            },
            _marker: PhantomData,
        }
    }

    pub fn with_receipts<I>(self, receipts: Vec<N::Receipt>) -> Self {
        let executor = self.inner.executor;
        FlashblocksBlockBuilder {
            inner: BasicBlockBuilder {
                ctx: self.inner.ctx,
                parent: self.inner.parent,
                executor: executor.with_receipts(receipts),
                assembler: self.inner.assembler,
                transactions: self.inner.transactions,
            },
            _marker: PhantomData,
        }
    }

    pub fn with_gas_used<I>(mut self, gas_used: u64) -> Self {
        let executor = self.inner.executor;
        FlashblocksBlockBuilder {
            inner: BasicBlockBuilder {
                ctx: self.inner.ctx,
                parent: self.inner.parent,
                executor: executor.with_gas_used(gas_used),
                assembler: self.inner.assembler,
                transactions: self.inner.transactions,
            },
            _marker: PhantomData,
        }
    }

    pub fn with_pre_state(self, pre_state: BundleState) -> Self {
        let executor = self.inner.executor;

        FlashblocksBlockBuilder {
            inner: BasicBlockBuilder {
                ctx: self.inner.ctx,
                parent: self.inner.parent,
                executor: executor.with_bundle_prestate(pre_state),
                assembler: self.inner.assembler,
                transactions: self.inner.transactions,
            },
            _marker: PhantomData,
        }
    }
}

impl<'a, R, F, Builder, N, DB, I> BlockBuilder
    for FlashblocksBlockBuilder<'a, R, F, Builder, N, DB, I>
where
    F: BlockExecutorFactory<
            Transaction = N::SignedTx,
            Receipt = N::Receipt,
            EvmFactory: EvmFactory<
                Spec = OpChainSpec,
                Tx: FromRecoveredTx<op_alloy_consensus::OpTxEnvelope>
                        + FromTxWithEncoded<op_alloy_consensus::OpTxEnvelope>,
                HaltReason = OpHaltReason,
            >,
        > + 'static,
    FlashblocksBlockExecutor<
        <<F as BlockExecutorFactory>::EvmFactory as EvmFactory>::Evm<
            &'a mut revm::database::State<DB>,
            I,
        >,
        R,
    >: BlockExecutor<
        Evm: Evm<
            Spec = <F::EvmFactory as EvmFactory>::Spec,
            HaltReason = <F::EvmFactory as EvmFactory>::HaltReason,
            DB = &'a mut State<DB>,
            Inspector = I,
            Tx: FromRecoveredTx<op_alloy_consensus::OpTxEnvelope>
                    + FromTxWithEncoded<op_alloy_consensus::OpTxEnvelope>,
        >,
        Transaction = N::SignedTx,
        Receipt = N::Receipt,
    >,
    R: OpReceiptBuilder<Transaction = N::SignedTx, Receipt = N::Receipt> + Default,
    DB: Database + 'a,
    Builder: BlockAssembler<F, Block = N::Block>,
    N: NodePrimitives<Block = OpBlock, SignedTx = OpTxEnvelope, Receipt = OpReceipt>,
    I: revm::Inspector<<F::EvmFactory as EvmFactory>::Context<&'a mut State<DB>>> + 'static,
{
    type Executor = FlashblocksBlockExecutor<
        <<F as BlockExecutorFactory>::EvmFactory as EvmFactory>::Evm<&'a mut State<DB>, I>,
        R,
    >;
    type Primitives = N;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        self.inner.apply_pre_execution_changes()
    }

    fn execute_transaction_with_commit_condition(
        &mut self,
        tx: impl ExecutorTx<Self::Executor>,
        f: impl FnOnce(
            &ExecutionResult<<<Self::Executor as BlockExecutor>::Evm as Evm>::HaltReason>,
        ) -> CommitChanges,
    ) -> Result<Option<u64>, BlockExecutionError> {
        self.inner.execute_transaction_with_commit_condition(tx, f)
    }

    fn finish(
        self,
        state: impl StateProvider,
    ) -> Result<BlockBuilderOutcome<N>, BlockExecutionError> {
        self.inner.finish(state)
    }

    fn executor_mut(&mut self) -> &mut Self::Executor {
        self.inner.executor_mut()
    }

    fn executor(&self) -> &Self::Executor {
        self.inner.executor()
    }

    fn into_executor(self) -> Self::Executor {
        self.inner.into_executor()
    }
}
