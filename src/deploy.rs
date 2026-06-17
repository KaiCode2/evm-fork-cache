//! Helpers for deploying Foundry artifacts into an [`EvmCache`].
//!
//! The main use case is local simulation against a fork where the caller wants
//! to replace code at an existing address while preserving that account's
//! storage, balance, and nonce. For contracts with immutables, the helper first
//! runs the creation bytecode with ABI-encoded constructor arguments, then
//! copies the resulting runtime bytecode to the target address.

use std::path::{Path, PathBuf};

use alloy_primitives::{Address, Bytes};
use alloy_sol_types::{SolType, SolValue, abi::TokenSeq};
use anyhow::{Context, Result, bail};
use tracing::{debug, info};

use crate::cache::{EvmCache, MissingTargetBehavior};

/// A Foundry JSON artifact with decoded creation bytecode.
#[derive(Debug, Clone)]
pub struct FoundryArtifact {
    /// Path the artifact was loaded from.
    pub path: PathBuf,
    /// Creation bytecode from `bytecode.object`.
    pub creation_code: Bytes,
}

impl FoundryArtifact {
    /// Load a Foundry JSON artifact from disk.
    ///
    /// Supports the standard Foundry shape:
    /// `{ "bytecode": { "object": "0x..." } }`.
    ///
    /// The legacy direct string shape `{ "bytecode": "0x..." }` is also
    /// accepted to make tests and generated artifacts easier to reuse.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, is not valid JSON, lacks a
    /// usable `bytecode`/`bytecode.object` field, or contains bytecode that is
    /// empty, not valid hex, or still has unresolved library placeholders (see
    /// [`load_foundry_creation_code`]).
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let creation_code = load_foundry_creation_code(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            creation_code,
        })
    }

    /// Build init code by appending ABI-encoded constructor arguments to the
    /// artifact's creation bytecode.
    pub fn init_code(&self, constructor_args: impl AsRef<[u8]>) -> Bytes {
        build_init_code(&self.creation_code, constructor_args)
    }

    /// Deploy this artifact into the forked EVM and return the temporary
    /// deployed address.
    ///
    /// `constructor_args` must already be ABI encoded. Use
    /// [`encode_constructor_args`] for ordinary Solidity constructor tuples.
    ///
    /// # Errors
    ///
    /// Returns an error if the `CREATE` transaction reverts or halts, or if the
    /// deployment otherwise fails to produce a deployed address (see
    /// [`EvmCache::deploy_contract`]).
    ///
    /// # Panics
    ///
    /// Like any method that may fetch missing state, this must run on a
    /// multi-thread tokio runtime; deploying on a current-thread runtime panics
    /// when the fork DB attempts a synchronous RPC fetch.
    pub fn deploy(
        &self,
        cache: &mut EvmCache,
        deployer: Address,
        constructor_args: impl AsRef<[u8]>,
    ) -> Result<Address> {
        let init_code = self.init_code(constructor_args);
        let deployed = cache
            .deploy_contract(deployer, init_code)
            .with_context(|| format!("deploying Foundry artifact {}", self.path.display()))?;
        debug!(
            artifact = %self.path.display(),
            %deployer,
            %deployed,
            "deployed Foundry artifact into EVM cache"
        );
        Ok(deployed)
    }

    /// Deploy this artifact and copy its runtime bytecode to `target`.
    ///
    /// This is equivalent to a simulation-friendly `vm.etch` for contracts with
    /// constructor-initialized immutables: the temporary deployment computes the
    /// final runtime bytecode, and `target` keeps its existing storage, balance,
    /// and nonce. `target` must already have non-empty runtime bytecode.
    ///
    /// On any error the cache is restored to its pre-deploy snapshot, so a
    /// failed etch leaves no partial deployment behind.
    ///
    /// # Errors
    ///
    /// Returns an error if `target` is missing or has no runtime bytecode, if
    /// the deployment reverts or halts (see [`Self::deploy`]), or if copying the
    /// runtime bytecode to `target` fails.
    ///
    /// # Panics
    ///
    /// Must run on a multi-thread tokio runtime; the underlying deployment
    /// panics on a current-thread runtime when the fork DB attempts a
    /// synchronous RPC fetch.
    pub fn etch(
        &self,
        cache: &mut EvmCache,
        target: Address,
        deployer: Address,
        constructor_args: impl AsRef<[u8]>,
    ) -> Result<EtchedContract> {
        self.etch_with_missing_target_behavior(
            cache,
            target,
            deployer,
            constructor_args,
            MissingTargetBehavior::Error,
        )
    }

    /// Deploy this artifact and copy its runtime bytecode to `target`, creating
    /// a default target account when it does not already exist.
    ///
    /// Use this only for synthetic simulation addresses where there is no
    /// storage, balance, or nonce to preserve.
    ///
    /// On any error the cache is restored to its pre-deploy snapshot, so a
    /// failed etch leaves no synthetic target account behind.
    ///
    /// # Errors
    ///
    /// Returns an error if the deployment reverts or halts (see
    /// [`Self::deploy`]), or if copying the runtime bytecode to `target` fails
    /// (for example when the deployed contract has empty runtime bytecode).
    ///
    /// # Panics
    ///
    /// Must run on a multi-thread tokio runtime; the underlying deployment
    /// panics on a current-thread runtime when the fork DB attempts a
    /// synchronous RPC fetch.
    pub fn etch_or_create(
        &self,
        cache: &mut EvmCache,
        target: Address,
        deployer: Address,
        constructor_args: impl AsRef<[u8]>,
    ) -> Result<EtchedContract> {
        self.etch_with_missing_target_behavior(
            cache,
            target,
            deployer,
            constructor_args,
            MissingTargetBehavior::Create,
        )
    }

    fn etch_with_missing_target_behavior(
        &self,
        cache: &mut EvmCache,
        target: Address,
        deployer: Address,
        constructor_args: impl AsRef<[u8]>,
        missing_target: MissingTargetBehavior,
    ) -> Result<EtchedContract> {
        let snapshot = cache.snapshot();
        let result = self.try_etch_with_missing_target_behavior(
            cache,
            target,
            deployer,
            constructor_args,
            missing_target,
        );

        if result.is_err() {
            cache.restore(snapshot);
        }

        result
    }

    fn try_etch_with_missing_target_behavior(
        &self,
        cache: &mut EvmCache,
        target: Address,
        deployer: Address,
        constructor_args: impl AsRef<[u8]>,
        missing_target: MissingTargetBehavior,
    ) -> Result<EtchedContract> {
        if matches!(missing_target, MissingTargetBehavior::Error) {
            cache
                .require_contract_target(target)
                .with_context(|| format!("validating target contract {}", target))?;
        }

        let deployed = self.deploy(cache, deployer, constructor_args)?;
        cache
            .override_account_code_with_missing_target(deployed, target, missing_target)
            .with_context(|| format!("etching runtime bytecode at {}", target))?;

        let code_size = cache
            .db_mut()
            .cache
            .accounts
            .get(&target)
            .and_then(|account| account.info.code.as_ref().map(|code| code.len()))
            .unwrap_or_default();

        info!(
            artifact = %self.path.display(),
            %deployed,
            %target,
            code_size,
            "etched Foundry artifact runtime bytecode"
        );

        Ok(EtchedContract {
            artifact_path: self.path.clone(),
            deployed_address: deployed,
            target_address: target,
            code_size,
        })
    }
}

/// Result of deploying an artifact and etching its runtime code at a target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtchedContract {
    /// Artifact path used for the deployment.
    pub artifact_path: PathBuf,
    /// Temporary address returned by the CREATE deployment.
    pub deployed_address: Address,
    /// Address whose runtime bytecode was replaced.
    pub target_address: Address,
    /// Runtime bytecode size at `target` after etching.
    pub code_size: usize,
}

/// ABI-encode constructor arguments.
///
/// Pass a tuple of alloy Solidity values matching the constructor parameter
/// list, e.g. `(owner, weth, vault)`. Single-argument constructors need a
/// trailing comma so the value is still a tuple: `(owner,)`. An empty tuple
/// `()` encodes to empty bytes, which is correct for argument-less
/// constructors.
///
/// The encoding mirrors Solidity constructor parameter encoding
/// (`abi.encode(arg0, arg1, ...)`): it uses [`SolValue::abi_encode_params`],
/// which lays the arguments out as a flat parameter list. This differs from
/// [`SolValue::abi_encode`], which would wrap a tuple in an extra layer
/// (matching `abi.encode((...))`) and produce the wrong bytes for a
/// constructor.
///
/// The trait bounds spell out "any alloy Solidity value tuple": `T: SolValue`
/// means each element implements the alloy Solidity-value trait, and the
/// `TokenSeq` bound on `T::SolType` requires the tuple's token to be a
/// sequence so it can be encoded as a parameter list. In practice you do not
/// construct these bounds yourself — they are satisfied automatically by
/// tuples of alloy primitives such as [`Address`], [`U256`](alloy_primitives::U256),
/// and `String`.
///
/// ```ignore
/// let args = evm_fork_cache::deploy::encode_constructor_args((owner, weth, vault));
/// ```
pub fn encode_constructor_args<T>(args: T) -> Bytes
where
    T: SolValue,
    for<'a> <T::SolType as SolType>::Token<'a>: TokenSeq<'a>,
{
    args.abi_encode_params().into()
}

/// Load creation bytecode from a Foundry artifact.
///
/// Reads the JSON at `path` and decodes the creation bytecode from
/// `bytecode.object` (or the legacy direct-string `bytecode` field).
///
/// # Errors
///
/// Returns an error when:
/// - the file cannot be read,
/// - the contents are not valid JSON,
/// - the JSON has no `bytecode` field, or `bytecode` has neither an `object`
///   string nor a direct string value,
/// - the bytecode hex is empty, still contains unresolved library
///   placeholders (`__$...$__`), or is otherwise not valid hex.
pub fn load_foundry_creation_code(path: impl AsRef<Path>) -> Result<Bytes> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read Foundry artifact at {}", path.display()))?;
    let json: serde_json::Value = serde_json::from_str(&content).with_context(|| {
        format!(
            "failed to parse Foundry artifact JSON at {}",
            path.display()
        )
    })?;

    let bytecode = json
        .get("bytecode")
        .ok_or_else(|| anyhow::anyhow!("artifact {} has no `bytecode` field", path.display()))?;

    let bytecode_hex = bytecode
        .get("object")
        .and_then(serde_json::Value::as_str)
        .or_else(|| bytecode.as_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "artifact {} has no `bytecode.object` string",
                path.display()
            )
        })?;

    decode_hex_bytecode(bytecode_hex)
        .with_context(|| format!("failed to decode bytecode in {}", path.display()))
}

/// Build init code from creation bytecode and ABI-encoded constructor args.
///
/// Init code is simply the contract's creation bytecode with the ABI-encoded
/// constructor arguments appended, matching how the EVM expects a `CREATE`
/// payload to be laid out. The `constructor_args` must already be ABI encoded;
/// use [`encode_constructor_args`] to produce them from an alloy Solidity
/// value tuple.
///
/// ```
/// use evm_fork_cache::deploy::build_init_code;
///
/// let init = build_init_code([0x60, 0x80], [0x01, 0x02, 0x03]);
/// assert_eq!(init.as_ref(), &[0x60, 0x80, 0x01, 0x02, 0x03]);
/// ```
pub fn build_init_code(
    creation_code: impl AsRef<[u8]>,
    constructor_args: impl AsRef<[u8]>,
) -> Bytes {
    let creation_code = creation_code.as_ref();
    let constructor_args = constructor_args.as_ref();
    let mut init_code = Vec::with_capacity(creation_code.len() + constructor_args.len());
    init_code.extend_from_slice(creation_code);
    init_code.extend_from_slice(constructor_args);
    Bytes::from(init_code)
}

/// Deploy a Foundry artifact into the forked EVM and return its temporary
/// deployed address.
///
/// # Errors
///
/// Returns an error if the artifact cannot be loaded (see
/// [`load_foundry_creation_code`]) or if the deployment reverts or halts (see
/// [`FoundryArtifact::deploy`]).
///
/// # Panics
///
/// Must run on a multi-thread tokio runtime; the deployment panics on a
/// current-thread runtime when the fork DB attempts a synchronous RPC fetch.
pub fn deploy_foundry_artifact(
    cache: &mut EvmCache,
    artifact_path: impl AsRef<Path>,
    deployer: Address,
    constructor_args: impl AsRef<[u8]>,
) -> Result<Address> {
    FoundryArtifact::load(artifact_path)?.deploy(cache, deployer, constructor_args)
}

/// Deploy a Foundry artifact and copy its runtime bytecode to `target`.
///
/// `target` must already have non-empty runtime bytecode. Its storage, balance,
/// and nonce are preserved. If `target` is missing or has no runtime bytecode,
/// this returns an error. Use [`etch_foundry_artifact_or_create`] for synthetic
/// simulation addresses.
///
/// # Errors
///
/// Returns an error if the artifact cannot be loaded (see
/// [`load_foundry_creation_code`]), if `target` is missing or has no runtime
/// bytecode, if the deployment reverts or halts, or if copying the runtime
/// bytecode to `target` fails (see [`FoundryArtifact::etch`]).
///
/// # Panics
///
/// Must run on a multi-thread tokio runtime; the deployment panics on a
/// current-thread runtime when the fork DB attempts a synchronous RPC fetch.
pub fn etch_foundry_artifact(
    cache: &mut EvmCache,
    target: Address,
    artifact_path: impl AsRef<Path>,
    deployer: Address,
    constructor_args: impl AsRef<[u8]>,
) -> Result<EtchedContract> {
    FoundryArtifact::load(artifact_path)?.etch(cache, target, deployer, constructor_args)
}

/// Deploy a Foundry artifact and copy its runtime bytecode to `target`, creating
/// a default target account when it does not already exist.
///
/// Prefer [`etch_foundry_artifact`] for forked/live contract addresses whose
/// storage, balance, or nonce should be preserved.
///
/// # Errors
///
/// Returns an error if the artifact cannot be loaded (see
/// [`load_foundry_creation_code`]), if the deployment reverts or halts, or if
/// copying the runtime bytecode to `target` fails, for example when the
/// deployed contract has empty runtime bytecode (see
/// [`FoundryArtifact::etch_or_create`]).
///
/// # Panics
///
/// Must run on a multi-thread tokio runtime; the deployment panics on a
/// current-thread runtime when the fork DB attempts a synchronous RPC fetch.
pub fn etch_foundry_artifact_or_create(
    cache: &mut EvmCache,
    target: Address,
    artifact_path: impl AsRef<Path>,
    deployer: Address,
    constructor_args: impl AsRef<[u8]>,
) -> Result<EtchedContract> {
    FoundryArtifact::load(artifact_path)?.etch_or_create(cache, target, deployer, constructor_args)
}

fn decode_hex_bytecode(bytecode_hex: &str) -> Result<Bytes> {
    let stripped = bytecode_hex.strip_prefix("0x").unwrap_or(bytecode_hex);
    if stripped.is_empty() {
        bail!("empty bytecode");
    }
    if stripped.contains("__") {
        bail!("bytecode contains unresolved library placeholders");
    }

    alloy_primitives::hex::decode(stripped)
        .map(Bytes::from)
        .context("invalid hex bytecode")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;
    use revm::state::{AccountInfo, Bytecode};
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn build_init_code_appends_constructor_args() {
        let init = build_init_code([0x60, 0x80], [0x01, 0x02, 0x03]);
        assert_eq!(init.as_ref(), &[0x60, 0x80, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn encode_constructor_args_uses_parameter_encoding_for_dynamic_args() {
        let args = (String::from("hello"), U256::from(7));
        let encoded = encode_constructor_args(args.clone());

        assert_eq!(encoded.as_ref(), args.abi_encode_params().as_slice());
        assert_ne!(encoded.as_ref(), args.abi_encode().as_slice());
    }

    #[test]
    fn encode_constructor_args_empty_tuple_is_empty() {
        assert!(encode_constructor_args(()).is_empty());
    }

    #[test]
    fn strict_override_rejects_known_empty_target() -> Result<()> {
        let mut cache = setup_cache();
        let source = Address::repeat_byte(0x11);
        let target = Address::repeat_byte(0x22);

        cache
            .db_mut()
            .insert_account_info(source, account_with_runtime(&[0x00], U256::ZERO, 1));
        cache
            .db_mut()
            .insert_account_info(target, AccountInfo::default());

        let err = cache.override_account_code(source, target).unwrap_err();
        assert!(err.to_string().contains("target account"));
        assert!(err.to_string().contains("no runtime bytecode"));

        let target_account = cache
            .db_mut()
            .cache
            .accounts
            .get(&target)
            .expect("target should still exist");
        assert!(
            target_account
                .info
                .code
                .as_ref()
                .is_none_or(|code| code.is_empty())
        );

        cache.override_or_create_account_code(source, target)?;
        let target_account = cache
            .db_mut()
            .cache
            .accounts
            .get(&target)
            .expect("explicit create should write target code");
        assert!(
            target_account
                .info
                .code
                .as_ref()
                .is_some_and(|code| !code.is_empty())
        );

        Ok(())
    }

    #[test]
    fn strict_etch_rejects_empty_target_before_deploying() -> Result<()> {
        let mut cache = setup_cache();
        let deployer = Address::ZERO;
        let target = Address::repeat_byte(0x33);
        let create_address = zero_nonce_create_address()?;
        let artifact = memory_artifact(non_empty_runtime_creation_code());

        cache
            .db_mut()
            .insert_account_info(deployer, AccountInfo::default());
        cache
            .db_mut()
            .insert_account_info(create_address, AccountInfo::default());
        cache
            .db_mut()
            .insert_account_info(target, AccountInfo::default());

        let err = artifact
            .etch(&mut cache, target, deployer, Bytes::new())
            .unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("validating target contract"));
        assert!(err.contains("no runtime bytecode"));

        assert_eq!(cached_nonce(&mut cache, deployer), Some(0));
        assert_eq!(cached_code_len(&mut cache, create_address), Some(0));

        Ok(())
    }

    #[test]
    fn etch_restores_cache_when_override_fails_after_deploy() -> Result<()> {
        let mut cache = setup_cache();
        let deployer = Address::ZERO;
        let target = Address::repeat_byte(0x44);
        let create_address = zero_nonce_create_address()?;
        let artifact = memory_artifact(empty_runtime_creation_code());

        cache
            .db_mut()
            .insert_account_info(deployer, AccountInfo::default());
        cache
            .db_mut()
            .insert_account_info(create_address, AccountInfo::default());

        let err = artifact
            .etch_or_create(&mut cache, target, deployer, Bytes::new())
            .unwrap_err();
        assert!(err.to_string().contains("etching runtime bytecode"));
        assert!(err.to_string().contains("bytecode"));

        assert_eq!(cached_nonce(&mut cache, deployer), Some(0));
        assert_eq!(cached_code_len(&mut cache, create_address), Some(0));
        assert!(
            !cache.db_mut().cache.accounts.contains_key(&target),
            "failed etch should not leave a synthetic target account behind"
        );

        Ok(())
    }

    #[test]
    fn decode_hex_bytecode_rejects_unlinked_libraries() {
        let err = decode_hex_bytecode("0x60__$abc$__").unwrap_err();
        assert!(err.to_string().contains("unresolved library"));
    }

    #[test]
    fn load_foundry_creation_code_reads_bytecode_object() -> Result<()> {
        let path = temp_artifact_path("foundry-bytecode-object");
        std::fs::write(&path, r#"{"bytecode":{"object":"0x60016002"}}"#)?;

        let code = load_foundry_creation_code(&path)?;

        std::fs::remove_file(&path).ok();
        assert_eq!(code.as_ref(), &[0x60, 0x01, 0x60, 0x02]);
        Ok(())
    }

    #[test]
    fn load_foundry_creation_code_accepts_direct_string_bytecode() -> Result<()> {
        let path = temp_artifact_path("foundry-bytecode-string");
        std::fs::write(&path, r#"{"bytecode":"0x6001"}"#)?;

        let code = load_foundry_creation_code(&path)?;

        std::fs::remove_file(&path).ok();
        assert_eq!(code.as_ref(), &[0x60, 0x01]);
        Ok(())
    }

    fn temp_artifact_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.json", std::process::id()))
    }

    fn setup_cache() -> EvmCache {
        use alloy_provider::{RootProvider, network::AnyNetwork};
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");

        rt.block_on(EvmCache::new(Arc::new(provider), None))
    }

    fn memory_artifact(creation_code: Bytes) -> FoundryArtifact {
        FoundryArtifact {
            path: PathBuf::from("memory-artifact.json"),
            creation_code,
        }
    }

    fn account_with_runtime(runtime: &[u8], balance: U256, nonce: u64) -> AccountInfo {
        let bytecode = Bytecode::new_raw(Bytes::copy_from_slice(runtime));
        let code_hash = bytecode.hash_slow();
        AccountInfo {
            balance,
            nonce,
            code: Some(bytecode),
            code_hash,
            account_id: None,
        }
    }

    fn non_empty_runtime_creation_code() -> Bytes {
        Bytes::from_static(&[
            0x60, 0x01, // PUSH1 1 byte runtime
            0x60, 0x0c, // PUSH1 runtime offset
            0x60, 0x00, // PUSH1 destination offset
            0x39, // CODECOPY
            0x60, 0x01, // PUSH1 1 byte runtime
            0x60, 0x00, // PUSH1 return offset
            0xf3, // RETURN
            0x00, // runtime: STOP
        ])
    }

    fn empty_runtime_creation_code() -> Bytes {
        Bytes::from_static(&[
            0x60, 0x00, // PUSH1 0 byte runtime
            0x60, 0x00, // PUSH1 return offset
            0xf3, // RETURN
        ])
    }

    fn zero_nonce_create_address() -> Result<Address> {
        "0xbd770416a3345f91e4b34576cb804a576fa48eb1"
            .parse()
            .context("zero nonce CREATE address should parse")
    }

    fn cached_nonce(cache: &mut EvmCache, address: Address) -> Option<u64> {
        cache
            .db_mut()
            .cache
            .accounts
            .get(&address)
            .map(|account| account.info.nonce)
    }

    fn cached_code_len(cache: &mut EvmCache, address: Address) -> Option<usize> {
        cache
            .db_mut()
            .cache
            .accounts
            .get(&address)
            .map(|account| account.info.code.as_ref().map_or(0, |code| code.len()))
    }
}
