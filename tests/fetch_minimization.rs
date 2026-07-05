//! Pins the Pillar 2 headline (data-fetch minimization) as a deterministic,
//! CI-guaranteed integer invariant: the crate fetches a shared hot working set
//! ONCE, and fanning N candidate simulations out over the frozen snapshot adds
//! ZERO further fetches — so a fork-per-candidate loop fetches `N x` as much.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::Result;
use evm_fork_cache::cache::{EvmCache, EvmOverlay, StorageBatchFetchFn};
use revm::context::result::ExecutionResult;
use revm::state::AccountInfo;

use common::{MOCK_ERC20_BALANCE_SLOT, MockERC20, mock_erc20_runtime, setup_cache};

const WORKING_SET: usize = 8;
const N_CANDIDATES: usize = 500;
const SEEDED_BALANCE: u64 = 1_000;

fn owner(i: usize) -> Address {
    let mut bytes = [0u8; 20];
    bytes[12..20].copy_from_slice(&(i as u64 + 1).to_be_bytes());
    Address::from(bytes)
}

fn balance_slot(owner: Address) -> U256 {
    U256::from_be_bytes(keccak256((owner, U256::from(MOCK_ERC20_BALANCE_SLOT)).abi_encode()).0)
}

fn counting_fetcher(counter: Arc<AtomicUsize>) -> StorageBatchFetchFn {
    Arc::new(move |requests: Vec<(Address, U256)>, _block: BlockId| {
        counter.fetch_add(requests.len(), Ordering::Relaxed);
        requests
            .into_iter()
            .map(|(addr, slot)| (addr, slot, Ok(U256::from(SEEDED_BALANCE))))
            .collect()
    })
}

async fn cache_with_counter(token: Address) -> Result<(EvmCache, Arc<AtomicUsize>)> {
    let mut cache = setup_cache().await?;
    let runtime = mock_erc20_runtime();
    let code_hash = runtime.hash_slow();
    cache.db_mut().insert_account_info(
        token,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 1,
            code: Some(runtime),
            code_hash,
            account_id: None,
        },
    );
    let counter = Arc::new(AtomicUsize::new(0));
    cache.set_storage_batch_fetcher(counting_fetcher(counter.clone()));
    Ok((cache, counter))
}

fn balance_of(overlay: &mut EvmOverlay, caller: Address, token: Address, account: Address) -> U256 {
    let calldata = Bytes::from(MockERC20::balanceOfCall { account }.abi_encode());
    match overlay.call_raw(caller, token, calldata).unwrap() {
        ExecutionResult::Success { output, .. } => {
            MockERC20::balanceOfCall::abi_decode_returns(&output.into_data()).unwrap()
        }
        other => panic!("balanceOf failed: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fan_out_reuses_one_warmup_fetch_across_all_candidates() -> Result<()> {
    let token = Address::repeat_byte(0x42);
    let working_set: Vec<(Address, U256)> = (0..WORKING_SET)
        .map(|i| (token, balance_slot(owner(i))))
        .collect();

    let (mut cache, counter) = cache_with_counter(token).await?;

    // Warm the working set once.
    cache.verify_slots(&working_set)?;
    assert_eq!(
        counter.load(Ordering::Relaxed),
        WORKING_SET,
        "warm-up fetches each working-set slot exactly once"
    );

    let snapshot = cache.snapshot();
    counter.store(0, Ordering::Relaxed);

    // Fan N candidates out; each reads the whole shared hot set from the snapshot.
    for c in 0..N_CANDIDATES {
        let mut overlay = EvmOverlay::new(snapshot.clone(), None);
        for i in 0..WORKING_SET {
            // Correctness: the warmed value is actually served (reuse works), and
            // the read does not fetch.
            assert_eq!(
                balance_of(&mut overlay, owner(c % WORKING_SET), token, owner(i)),
                U256::from(SEEDED_BALANCE)
            );
        }
    }

    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "the {N_CANDIDATES}-candidate fan-out adds zero fetches (overlays read the snapshot)"
    );

    // The fork-per-candidate baseline: one real cold cache fetches the whole
    // working set, so N of them fetch N x as much.
    let (mut cold, cold_counter) = cache_with_counter(token).await?;
    cold.verify_slots(&working_set)?;
    let per_candidate = cold_counter.load(Ordering::Relaxed);
    assert_eq!(per_candidate, WORKING_SET);
    assert_eq!(
        per_candidate * N_CANDIDATES,
        WORKING_SET * N_CANDIDATES,
        "vanilla fork-per-candidate re-fetches the working set every candidate"
    );

    Ok(())
}
