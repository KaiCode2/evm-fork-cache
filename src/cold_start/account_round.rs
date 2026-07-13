//! Exact-hash account/code verification artifacts for background cold start.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, Bytes};
use alloy_provider::Provider;

use crate::cache::{AccountProof, AccountProofFetchFn, account_proof_fetcher};
use crate::errors::StorageFetchResult;

/// One canonical runtime-code hash claim to verify through `eth_getProof`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AccountCodeClaim {
    address: Address,
    expected_code_hash: B256,
}

impl AccountCodeClaim {
    /// Construct a code claim. Callers normally hash known runtime bytes
    /// locally and pass that hash here.
    pub const fn new(address: Address, expected_code_hash: B256) -> Self {
        Self {
            address,
            expected_code_hash,
        }
    }

    /// Claimed contract address.
    pub const fn address(self) -> Address {
        self.address
    }

    /// Expected `codeHash` committed by the account proof.
    pub const fn expected_code_hash(self) -> B256 {
        self.expected_code_hash
    }
}

/// Root-only account/code checks at one exact canonical block hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountProofRoundRequest {
    block_hash: B256,
    claims: Vec<AccountCodeClaim>,
}

impl AccountProofRoundRequest {
    /// Construct a code-verification round. Duplicate-address validation occurs
    /// before provider IO in [`AccountProofRoundFetcher::fetch`].
    pub fn new(block_hash: B256, claims: impl IntoIterator<Item = AccountCodeClaim>) -> Self {
        Self {
            block_hash,
            claims: claims.into_iter().collect(),
        }
    }

    /// Exact canonical block hash for every proof request.
    pub const fn block_hash(&self) -> B256 {
        self.block_hash
    }

    /// Code claims in caller order.
    pub fn claims(&self) -> &[AccountCodeClaim] {
        &self.claims
    }
}

/// Classified result of one exact-hash account/code claim.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccountProofOutcome {
    /// The proof's `codeHash` matches the worker's expected runtime-code hash.
    Verified {
        /// Verified account.
        address: Address,
        /// Exact-hash account fields and storage root.
        proof: AccountProof,
    },
    /// The proof exists but commits to another code hash (including absent and
    /// code-less account hashes).
    Mismatch {
        /// Contradicted account.
        address: Address,
        /// Worker-claimed runtime-code hash.
        expected: B256,
        /// Proof-observed code hash.
        actual: B256,
        /// Exact-hash account fields and storage root.
        proof: AccountProof,
    },
    /// The provider returned an explicit per-account failure.
    FetchFailed {
        /// Unverified account.
        address: Address,
        /// Provider error text.
        reason: String,
    },
}

impl AccountProofOutcome {
    /// Account associated with this outcome.
    pub const fn address(&self) -> Address {
        match self {
            Self::Verified { address, .. }
            | Self::Mismatch { address, .. }
            | Self::FetchFailed { address, .. } => *address,
        }
    }

    /// Borrow a verified proof, or `None` for mismatch/failure outcomes.
    pub const fn verified_proof(&self) -> Option<&AccountProof> {
        match self {
            Self::Verified { proof, .. } => Some(proof),
            Self::Mismatch { .. } | Self::FetchFailed { .. } => None,
        }
    }
}

/// Complete non-mutating account/code proof result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountProofRoundFetch {
    block_hash: B256,
    outcomes: Vec<AccountProofOutcome>,
}

/// Runtime code and exact-hash account fields ready for canonical cache commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedAccountValue {
    address: Address,
    proof: AccountProof,
    code: Bytes,
}

impl PreparedAccountValue {
    /// Pair known runtime bytes with the exact-hash proof that commits to them.
    /// Byte/hash validation remains at the cache-owner apply boundary.
    pub const fn new(address: Address, proof: AccountProof, code: Bytes) -> Self {
        Self {
            address,
            proof,
            code,
        }
    }

    /// Account whose runtime code was verified.
    pub const fn address(&self) -> Address {
        self.address
    }

    /// Exact-hash account proof fields.
    pub const fn proof(&self) -> &AccountProof {
        &self.proof
    }

    /// Runtime bytecode committed by [`AccountProof::code_hash`].
    pub const fn code(&self) -> &Bytes {
        &self.code
    }
}

/// Worker-produced verified accounts ready for one serialized cache commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedAccountPatch {
    block_hash: B256,
    verified_at_block: u64,
    values: Vec<PreparedAccountValue>,
}

impl PreparedAccountPatch {
    /// Construct a patch tied to one exact post-block point.
    pub fn new(
        block_hash: B256,
        verified_at_block: u64,
        values: impl IntoIterator<Item = PreparedAccountValue>,
    ) -> Self {
        Self {
            block_hash,
            verified_at_block,
            values: values.into_iter().collect(),
        }
    }

    /// Exact hash used to fetch every proof.
    pub const fn block_hash(&self) -> B256 {
        self.block_hash
    }

    /// Block height recorded on each durable verified-code mark.
    pub const fn verified_at_block(&self) -> u64 {
        self.verified_at_block
    }

    /// Verified accounts in worker order.
    pub fn values(&self) -> &[PreparedAccountValue] {
        &self.values
    }
}

impl AccountProofRoundFetch {
    /// Exact canonical hash used by the proof provider.
    pub const fn block_hash(&self) -> B256 {
        self.block_hash
    }

    /// One outcome per claim, in claim order.
    pub fn outcomes(&self) -> &[AccountProofOutcome] {
        &self.outcomes
    }

    /// Consume the result without cloning outcomes.
    pub fn into_outcomes(self) -> Vec<AccountProofOutcome> {
        self.outcomes
    }
}

/// Cloneable, thread-safe provider handle for exact-hash account/code proofs.
#[derive(Clone)]
pub struct AccountProofRoundFetcher {
    fetcher: AccountProofFetchFn,
}

impl fmt::Debug for AccountProofRoundFetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccountProofRoundFetcher")
            .finish_non_exhaustive()
    }
}

impl AccountProofRoundFetcher {
    /// Wrap an existing protocol-neutral account proof provider.
    pub fn new(fetcher: AccountProofFetchFn) -> Self {
        Self { fetcher }
    }

    /// Build a worker-owned proof fetcher without constructing an [`EvmCache`](crate::cache::EvmCache).
    pub fn from_provider<P>(provider: Arc<P>, max_concurrent_proofs: usize) -> Self
    where
        P: Provider<AnyNetwork> + 'static,
    {
        Self::new(account_proof_fetcher(provider, max_concurrent_proofs))
    }

    /// Verify every claim through a root-only proof pinned to the requested
    /// EIP-1898 canonical hash, without holding or mutating an EVM cache.
    pub fn fetch(
        &self,
        request: &AccountProofRoundRequest,
    ) -> Result<AccountProofRoundFetch, AccountProofRoundFetchError> {
        let mut requested = HashSet::with_capacity(request.claims.len());
        for claim in &request.claims {
            if !requested.insert(claim.address) {
                return Err(AccountProofRoundFetchError::DuplicateRequest {
                    address: claim.address,
                });
            }
        }

        let provider_requests = request
            .claims
            .iter()
            .map(|claim| (claim.address, Vec::new()))
            .collect();
        let response = (self.fetcher)(
            provider_requests,
            BlockId::from((request.block_hash, Some(true))),
        );

        let mut returned: HashMap<Address, StorageFetchResult<AccountProof>> =
            HashMap::with_capacity(response.len());
        for (address, proof) in response {
            if !requested.contains(&address) {
                return Err(AccountProofRoundFetchError::UnexpectedResult { address });
            }
            if returned.insert(address, proof).is_some() {
                return Err(AccountProofRoundFetchError::DuplicateResult { address });
            }
        }
        for address in requested {
            if !returned.contains_key(&address) {
                return Err(AccountProofRoundFetchError::MissingResult { address });
            }
        }

        let outcomes = request
            .claims
            .iter()
            .map(|claim| {
                match returned
                    .remove(&claim.address)
                    .expect("provider response completeness validated above")
                {
                    Ok(proof) if proof.code_hash == claim.expected_code_hash => {
                        AccountProofOutcome::Verified {
                            address: claim.address,
                            proof,
                        }
                    }
                    Ok(proof) => AccountProofOutcome::Mismatch {
                        address: claim.address,
                        expected: claim.expected_code_hash,
                        actual: proof.code_hash,
                        proof,
                    },
                    Err(error) => AccountProofOutcome::FetchFailed {
                        address: claim.address,
                        reason: error.to_string(),
                    },
                }
            })
            .collect();
        Ok(AccountProofRoundFetch {
            block_hash: request.block_hash,
            outcomes,
        })
    }
}

/// Invalid account-proof request or malformed provider response.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AccountProofRoundFetchError {
    /// More than one claim targeted the same account.
    #[error("duplicate account-proof request for {address}")]
    DuplicateRequest {
        /// Duplicated account.
        address: Address,
    },
    /// The provider returned an account that was not requested.
    #[error("account proof provider returned unexpected account {address}")]
    UnexpectedResult {
        /// Unexpected account.
        address: Address,
    },
    /// The provider returned one account more than once.
    #[error("account proof provider returned duplicate account {address}")]
    DuplicateResult {
        /// Duplicated account.
        address: Address,
    },
    /// The provider omitted a requested account.
    #[error("account proof provider omitted requested account {address}")]
    MissingResult {
        /// Omitted account.
        address: Address,
    },
}

/// Invalid worker-produced account/code patch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PreparedAccountPatchError {
    /// The actor cache is no longer pinned to the proof hash.
    #[error("prepared account patch baseline mismatch: prepared {prepared}, cache {cache:?}")]
    BaselineMismatch {
        /// Hash used by the background proof fetch.
        prepared: B256,
        /// Current cache hash, or `None` when number/tag-pinned.
        cache: Option<B256>,
    },
    /// The patch contains multiple values for one account.
    #[error("prepared account patch contains duplicate account {address}")]
    DuplicateAccount {
        /// Duplicated account.
        address: Address,
    },
    /// A verified-code value carried empty runtime bytes.
    #[error("prepared account patch contains empty runtime code for {address}")]
    EmptyCode {
        /// Invalid account.
        address: Address,
    },
    /// A root-only account proof unexpectedly carried storage slot payloads.
    #[error("prepared account proof for {address} unexpectedly contains {slots} storage slots")]
    UnexpectedProofSlots {
        /// Invalid account.
        address: Address,
        /// Unexpected slot count.
        slots: usize,
    },
    /// Runtime bytes do not match the proof's code hash.
    #[error("prepared runtime code hash mismatch for {address}: proof {expected}, bytes {actual}")]
    CodeHashMismatch {
        /// Invalid account.
        address: Address,
        /// Hash committed by the exact-hash proof.
        expected: B256,
        /// Hash of the prepared runtime bytes.
        actual: B256,
    },
    /// Canonical prepared state cannot overwrite deliberate local divergence.
    #[error("prepared canonical account patch cannot overwrite etched account {address}")]
    EtchedAccount {
        /// Etched account.
        address: Address,
    },
    /// A newer/different seed generation already owns the account.
    #[error(
        "prepared account seed conflict for {address}: existing {existing}, prepared {prepared}"
    )]
    SeedConflict {
        /// Conflicting account.
        address: Address,
        /// Existing marked-code hash.
        existing: B256,
        /// Worker-prepared verified hash.
        prepared: B256,
    },
}
