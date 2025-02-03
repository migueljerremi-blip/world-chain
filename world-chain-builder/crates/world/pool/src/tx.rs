use std::sync::Arc;

use alloy_consensus::{BlobTransactionSidecar, BlobTransactionValidationError};
use alloy_primitives::TxHash;
use alloy_rpc_types::erc4337::TransactionConditional;
use op_alloy_consensus::OpTypedTransaction;
use reth::transaction_pool::{
    error::{InvalidPoolTransactionError, PoolTransactionError},
    EthBlobTransactionSidecar, EthPoolTransaction, PoolTransaction, TransactionValidationOutcome,
};
use reth_optimism_node::txpool::OpPooledTransaction;
use reth_optimism_primitives::OpTransactionSigned;
use reth_primitives::{transaction::TransactionConversionError, Recovered};
use revm_primitives::{AccessList, Address, InvalidTransaction, KzgSettings, TxKind, B256, U256};
use thiserror::Error;
use world_chain_builder_pbh::payload::PbhValidationError;

pub trait WorldChainPoolTransaction: EthPoolTransaction {
    fn valid_pbh(&self) -> bool;
    fn set_valid_pbh(&mut self);
    fn conditional_options(&self) -> Option<&TransactionConditional>;
}

#[derive(Debug, Error)]
pub enum WorldChainPoolTransactionError {
    #[error("Conditional Validation Failed: {0}")]
    ConditionalValidationFailed(B256),
    #[error(transparent)]
    InvalidTransaction(#[from] InvalidTransaction),
    #[error(transparent)]
    PbhValidationError(#[from] PbhValidationError),
    #[error("Invalid calldata encoding")]
    InvalidCalldata,
    #[error("Missing PBH Payload")]
    MissingPbhPayload,
    #[error("InvalidSignatureAggregator")]
    InvalidSignatureAggregator,
    #[error("Malformed Call Trace")]
    MalformedCallTrace,
}

impl WorldChainPoolTransactionError {
    pub fn to_outcome<T: PoolTransaction>(self, tx: T) -> TransactionValidationOutcome<T> {
        TransactionValidationOutcome::Invalid(tx, self.into())
    }
}

impl From<WorldChainPoolTransactionError> for InvalidPoolTransactionError {
    fn from(val: WorldChainPoolTransactionError) -> Self {
        InvalidPoolTransactionError::Other(Box::new(val))
    }
}

//TODO: double check this?
impl PoolTransactionError for WorldChainPoolTransactionError {
    fn is_bad_transaction(&self) -> bool {
        // TODO: double check if invalid transaction should be penalized, we could also make this a match statement
        // If all errors should not be penalized, we can just return false
        false
    }
}

#[derive(Debug, Clone)]
pub struct WorldChainPooledTransaction {
    pub inner: OpPooledTransaction,
    pub valid_pbh: bool,
    pub conditional_options: Option<TransactionConditional>,
}

impl WorldChainPoolTransaction for WorldChainPooledTransaction {
    fn valid_pbh(&self) -> bool {
        self.valid_pbh
    }

    fn conditional_options(&self) -> Option<&TransactionConditional> {
        self.conditional_options.as_ref()
    }

    fn set_valid_pbh(&mut self) {
        self.valid_pbh = true;
    }
}

impl EthPoolTransaction for WorldChainPooledTransaction {
    fn take_blob(&mut self) -> EthBlobTransactionSidecar {
        self.inner.take_blob()
    }

    fn blob_count(&self) -> usize {
        self.inner.blob_count()
    }

    fn try_into_pooled_eip4844(
        self,
        sidecar: Arc<BlobTransactionSidecar>,
    ) -> Option<Recovered<Self::Pooled>> {
        self.inner.try_into_pooled_eip4844(sidecar)
    }

    fn try_from_eip4844(
        _tx: Recovered<Self::Consensus>,
        _sidecar: BlobTransactionSidecar,
    ) -> Option<Self> {
        None
    }

    fn validate_blob(
        &self,
        _sidecar: &BlobTransactionSidecar,
        _settings: &KzgSettings,
    ) -> Result<(), BlobTransactionValidationError> {
        Err(BlobTransactionValidationError::NotBlobTransaction(
            self.tx_type(),
        ))
    }

    fn authorization_count(&self) -> usize {
        match &self.inner.transaction.transaction {
            OpTypedTransaction::Eip7702(tx) => tx.authorization_list.len(),
            _ => 0,
        }
    }
}

impl From<OpPooledTransaction> for WorldChainPooledTransaction {
    fn from(tx: OpPooledTransaction) -> Self {
        Self {
            inner: tx,
            valid_pbh: false,
            conditional_options: None,
        }
    }
}

impl From<Recovered<op_alloy_consensus::OpPooledTransaction>> for WorldChainPooledTransaction {
    fn from(tx: Recovered<op_alloy_consensus::OpPooledTransaction>) -> Self {
        let inner = OpPooledTransaction::from(tx);

        Self {
            inner,
            valid_pbh: false,
            conditional_options: None,
        }
    }
}

impl TryFrom<Recovered<OpTransactionSigned>> for WorldChainPooledTransaction {
    type Error = TransactionConversionError;

    fn try_from(value: Recovered<OpTransactionSigned>) -> Result<Self, Self::Error> {
        let (tx, signer) = value.into_parts();
        let pooled: Recovered<op_alloy_consensus::OpPooledTransaction> =
            Recovered::new_unchecked(tx.try_into()?, signer);

        Ok(Self {
            inner: pooled.into(),
            valid_pbh: false,
            conditional_options: None,
        })
    }
}

impl From<WorldChainPooledTransaction> for Recovered<OpTransactionSigned> {
    fn from(val: WorldChainPooledTransaction) -> Self {
        val.inner.into()
    }
}

impl PoolTransaction for WorldChainPooledTransaction {
    type TryFromConsensusError = <Self as TryFrom<Recovered<Self::Consensus>>>::Error;
    type Consensus = OpTransactionSigned;
    type Pooled = op_alloy_consensus::OpPooledTransaction;

    fn clone_into_consensus(&self) -> Recovered<Self::Consensus> {
        self.inner.clone_into_consensus()
    }

    fn try_consensus_into_pooled(
        tx: Recovered<Self::Consensus>,
    ) -> Result<Recovered<Self::Pooled>, Self::TryFromConsensusError> {
        OpPooledTransaction::try_consensus_into_pooled(tx)
    }

    /// Returns hash of the transaction.
    fn hash(&self) -> &TxHash {
        self.inner.hash()
    }

    /// Returns the Sender of the transaction.
    fn sender(&self) -> Address {
        self.inner.sender()
    }

    /// Returns a reference to the Sender of the transaction.
    fn sender_ref(&self) -> &Address {
        self.inner.sender_ref()
    }

    /// Returns the nonce for this transaction.
    fn nonce(&self) -> u64 {
        self.inner.nonce()
    }

    /// Returns the cost that this transaction is allowed to consume:
    ///
    /// For EIP-1559 transactions: `max_fee_per_gas * gas_limit + tx_value`.
    /// For legacy transactions: `gas_price * gas_limit + tx_value`.
    /// For EIP-4844 blob transactions: `max_fee_per_gas * gas_limit + tx_value +
    /// max_blob_fee_per_gas * blob_gas_used`.
    fn cost(&self) -> &U256 {
        self.inner.cost()
    }

    /// Amount of gas that should be used in executing this transaction. This is paid up-front.
    fn gas_limit(&self) -> u64 {
        self.inner.gas_limit()
    }

    /// Returns the EIP-1559 Max base fee the caller is willing to pay.
    ///
    /// For legacy transactions this is `gas_price`.
    ///
    /// This is also commonly referred to as the "Gas Fee Cap" (`GasFeeCap`).
    fn max_fee_per_gas(&self) -> u128 {
        self.inner.max_fee_per_gas()
    }

    fn access_list(&self) -> Option<&AccessList> {
        self.inner.access_list()
    }

    /// Returns the EIP-1559 Priority fee the caller is paying to the block author.
    ///
    /// This will return `None` for non-EIP1559 transactions
    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.inner.max_priority_fee_per_gas()
    }

    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        self.inner.max_fee_per_blob_gas()
    }

    /// Returns the effective tip for this transaction.
    ///
    /// For EIP-1559 transactions: `min(max_fee_per_gas - base_fee, max_priority_fee_per_gas)`.
    /// For legacy transactions: `gas_price - base_fee`.
    fn effective_tip_per_gas(&self, base_fee: u64) -> Option<u128> {
        self.inner.effective_tip_per_gas(base_fee)
    }

    /// Returns the max priority fee per gas if the transaction is an EIP-1559 transaction, and
    /// otherwise returns the gas price.
    fn priority_fee_or_price(&self) -> u128 {
        self.inner.priority_fee_or_price()
    }

    /// Returns the transaction's [`TxKind`], which is the address of the recipient or
    /// [`TxKind::Create`] if the transaction is a contract creation.
    fn kind(&self) -> TxKind {
        self.inner.kind()
    }

    /// Returns true if the transaction is a contract creation.
    fn is_create(&self) -> bool {
        self.inner.is_create()
    }

    fn input(&self) -> &[u8] {
        self.inner.input()
    }

    /// Returns a measurement of the heap usage of this type and all its internals.
    fn size(&self) -> usize {
        self.inner.size()
    }

    /// Returns the transaction type
    fn tx_type(&self) -> u8 {
        self.inner.tx_type()
    }

    /// Returns the length of the rlp encoded object
    fn encoded_length(&self) -> usize {
        self.inner.encoded_length()
    }

    /// Returns `chain_id`
    fn chain_id(&self) -> Option<u64> {
        self.inner.chain_id()
    }
}
