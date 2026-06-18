//! Teach the revert decoder your own contract-defined custom errors.
//!
//! The core crate decodes only the two Solidity built-ins (`Error(string)` and
//! `Panic(uint256)`); every protocol- or app-specific selector is registered by
//! the application in one line. This example registers a handful of DeFi
//! adapter/orchestration errors — the kind a router or vault contract
//! defines — plus the IERC6093 standard `ERC20InsufficientBalance`, then decodes
//! sample revert blobs against them.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example custom_revert_errors
//! ```

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::{SolError, sol};
use evm_fork_cache::errors::{RevertDecoder, RevertReason};

sol! {
    // Application-specific errors. These live in your code, not in the crate —
    // define them once with `sol!` and register them on a decoder.
    #[derive(Debug)]
    error SwapFailed(address router, bytes data);
    #[derive(Debug)]
    error InvalidSimulationTarget();
    #[derive(Debug)]
    error NotCalm();
    // The IERC6093 standard error decodes through the very same mechanism.
    #[derive(Debug)]
    error ERC20InsufficientBalance(address sender, uint256 balance, uint256 needed);
}

fn main() {
    // Build a decoder once and reuse it across simulations (it is cheap to
    // clone and is `Send + Sync`).
    let decoder = RevertDecoder::new()
        .with_error::<SwapFailed>()
        .with_error::<InvalidSimulationTarget>()
        .with_error::<NotCalm>()
        .with_error::<ERC20InsufficientBalance>();
    println!("decoder knows {} custom errors\n", decoder.len());

    // A no-argument custom error: only the 4-byte selector is on the wire.
    decode(&decoder, "NotCalm", Bytes::from(NotCalm::SELECTOR.to_vec()));

    decode(
        &decoder,
        "InvalidSimulationTarget",
        Bytes::from(InvalidSimulationTarget::SELECTOR.to_vec()),
    );

    // A custom error carrying parameters — they are decoded and Debug-formatted.
    let swap_failed = SwapFailed {
        router: Address::repeat_byte(0x42),
        data: Bytes::from_static(b"router reverted"),
    };
    decode(
        &decoder,
        "SwapFailed",
        Bytes::from(swap_failed.abi_encode()),
    );

    let insufficient = ERC20InsufficientBalance {
        sender: Address::repeat_byte(0x11),
        balance: U256::from(5),
        needed: U256::from(100),
    };
    decode(
        &decoder,
        "ERC20InsufficientBalance",
        Bytes::from(insufficient.abi_encode()),
    );

    // An unregistered selector still decodes — as `Unknown` — so nothing is lost.
    decode(
        &decoder,
        "unregistered",
        Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
    );
}

fn decode(decoder: &RevertDecoder, label: &str, data: Bytes) {
    match decoder.decode(&data) {
        RevertReason::Custom(custom) => {
            println!("{label}: matched {}", custom.name);
            // A `Name()` signature has no arguments; only print params otherwise.
            if !custom.name.ends_with("()")
                && let Some(params) = &custom.params
            {
                println!("    params: {params}");
            }
        }
        RevertReason::Unknown { selector, .. } => {
            println!("{label}: unknown selector {selector}");
        }
        other => println!("{label}: {other}"),
    }
}
