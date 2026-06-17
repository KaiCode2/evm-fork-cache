//! Offline integration tests for the Multicall3 helpers.
//!
//! The live `aggregate3` execution path requires the Multicall3 contract to be
//! deployed in the fork, which the RPC-gated `multicall_batch` example exercises.
//! These tests pin the network-free behavior: empty-batch short-circuits, the
//! result-decoding helpers, and the documented batch constants.

mod common;

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::Result;

use common::setup_cache;
use evm_fork_cache::multicall::{
    IMulticall3, MAX_BATCH_SIZE, MulticallBatch, decode_result, execute_batched, try_decode_result,
};

sol! {
    function getValue() external returns (uint256);
}

/// An empty batch returns empty results without invoking the EVM, on all three
/// entry points.
#[tokio::test(flavor = "multi_thread")]
async fn empty_batch_short_circuits() -> Result<()> {
    let mut cache = setup_cache().await?;

    let batch = MulticallBatch::new();
    assert!(batch.is_empty());
    assert!(batch.execute(&mut cache)?.is_empty());

    let (results, access) = batch.execute_tracked(&mut cache)?;
    assert!(results.is_empty());
    assert!(access.slots.is_empty() && access.accounts.is_empty());

    let batched = execute_batched(&mut cache, std::iter::empty::<(Address, Bytes, bool)>())?;
    assert!(batched.is_empty());

    Ok(())
}

/// `add` and `add_call` both append a call; length tracks the call count.
#[test]
fn batch_len_tracks_added_calls() {
    let target = Address::repeat_byte(0x11);
    let mut batch = MulticallBatch::with_capacity(2);
    assert_eq!(batch.len(), 0);

    batch.add(target, getValueCall {}.abi_encode().into(), true);
    batch.add_call(target, getValueCall {}, false);
    assert_eq!(batch.len(), 2);
    assert!(!batch.is_empty());
}

/// `decode_result` returns the typed value for a successful result and errors on
/// a failed one; `try_decode_result` mirrors this with `Option`.
#[test]
fn decode_result_honors_success_flag() {
    let ok = IMulticall3::Result {
        success: true,
        returnData: U256::from(42u64).abi_encode().into(),
    };
    let decoded = decode_result::<getValueCall>(&ok).expect("successful result decodes");
    assert_eq!(decoded, U256::from(42u64));
    assert_eq!(
        try_decode_result::<getValueCall>(&ok),
        Some(U256::from(42u64))
    );

    let failed = IMulticall3::Result {
        success: false,
        returnData: Bytes::new(),
    };
    assert!(
        decode_result::<getValueCall>(&failed).is_err(),
        "a failed call cannot be decoded"
    );
    assert_eq!(try_decode_result::<getValueCall>(&failed), None);
}

/// A successful result whose payload is undecodable errors (and yields `None`),
/// distinct from the `success == false` case.
#[test]
fn decode_result_rejects_garbage_payload() {
    let garbage = IMulticall3::Result {
        success: true,
        returnData: Bytes::from_static(&[0x01, 0x02, 0x03]),
    };
    assert!(decode_result::<getValueCall>(&garbage).is_err());
    assert_eq!(try_decode_result::<getValueCall>(&garbage), None);
}

#[test]
fn max_batch_size_constant() {
    assert_eq!(MAX_BATCH_SIZE, 200);
}
