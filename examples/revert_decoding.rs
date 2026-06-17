//! Decode raw EVM revert data into structured reasons.
//!
//! The decoder natively understands the two Solidity built-ins — `Error(string)`
//! (from `require`/`revert("msg")`) and `Panic(uint256)` (overflow, etc.) — and
//! classifies anything else as `Unknown`. See `custom_revert_errors.rs` for
//! teaching it your own contract-defined errors.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example revert_decoding
//! ```

use alloy_primitives::Bytes;
use alloy_sol_types::{SolError, sol};
use evm_fork_cache::errors::{RevertReason, SimulationError, decode_revert_reason};

sol! {
    #[derive(Debug)]
    error Error(string);
    #[derive(Debug)]
    error Panic(uint256);
    #[derive(Debug)]
    error CustomError(uint256 code);
}

fn main() {
    // 1. A standard `require(false, "insufficient output")` revert.
    let string_revert = Bytes::from(Error::abi_encode(&Error("insufficient output".to_string())));
    print_reason("require/revert string", &string_revert);

    // 2. A `Panic(0x11)` — arithmetic overflow.
    let panic = Bytes::from(Panic::abi_encode(&Panic(alloy_primitives::U256::from(
        0x11,
    ))));
    print_reason("arithmetic overflow panic", &panic);

    // 3. An unrecognized custom-error selector (not registered with a decoder).
    let custom = Bytes::from(CustomError::abi_encode(&CustomError {
        code: alloy_primitives::U256::from(7),
    }));
    print_reason("unregistered custom error", &custom);

    // 4. An empty revert (e.g. an out-of-gas `revert()` with no data).
    print_reason("empty revert", &Bytes::new());

    // `SimulationError` wraps the gas used and exposes typed accessors.
    let err = SimulationError::from_revert(21_000, string_revert);
    println!("\nSimulationError: {err}");
    println!("  revert_message(): {:?}", err.revert_message());
    println!("  panic_code():     {:?}", err.panic_code());
}

fn print_reason(label: &str, data: &Bytes) {
    let reason = decode_revert_reason(data);
    match &reason {
        RevertReason::Error(message) => println!("{label}: Error({message:?})"),
        RevertReason::Panic(code) => println!("{label}: Panic({code:#x})"),
        RevertReason::Empty => println!("{label}: <no revert data>"),
        RevertReason::Custom(custom) => println!("{label}: custom {}", custom.name),
        RevertReason::Unknown { selector, .. } => {
            println!("{label}: unknown selector {selector} (register it to decode)")
        }
    }
}
