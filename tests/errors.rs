//! Integration tests for the public error-handling surface.
//!
//! The inline unit tests in `src/errors.rs` cover revert *decoding*; these cover
//! the public `SimError` ergonomics a caller branches on (classification,
//! `Display`, `From` conversions), the decoder's `Send + Sync + Clone` sharing
//! across threads, and two edge cases flagged in `docs/KNOWN_ISSUES.md`: silent
//! selector collisions and out-of-range panic codes.

use std::sync::Arc;

use alloy_primitives::{Bytes, U256};
use alloy_sol_types::{Panic, SolError, sol};
use evm_fork_cache::errors::{
    PANIC_SELECTOR, RevertDecoder, RevertReason, SimError, SimHostError, SimulationError,
};

sol! {
    #[derive(Debug)]
    error Unauthorized(address caller);
}

#[test]
fn sim_error_classification() {
    let revert: SimError = SimulationError::from_revert(21_000, Bytes::new()).into();
    assert!(revert.is_revert());
    assert!(!revert.is_halt());
    assert!(revert.as_revert().is_some());

    let halt = SimError::Halt {
        reason: "OutOfGas".to_string(),
        gas_used: 1_000_000,
    };
    assert!(halt.is_halt());
    assert!(!halt.is_revert());
    assert!(halt.as_revert().is_none());

    let other: SimError = SimHostError::Database {
        details: "rpc exploded".to_string(),
    }
    .into();
    assert!(!other.is_revert());
    assert!(!other.is_halt());
    assert!(other.as_revert().is_none());
}

#[test]
fn sim_error_display_distinguishes_variants() {
    let revert: SimError = SimulationError::from_revert(0, Bytes::new()).into();
    assert!(revert.to_string().contains("reverted"), "{revert}");

    let halt = SimError::Halt {
        reason: "StackOverflow".to_string(),
        gas_used: 5,
    };
    let shown = halt.to_string();
    assert!(shown.contains("halted"), "{shown}");
    assert!(shown.contains("StackOverflow"), "{shown}");

    let other: SimError = SimHostError::Database {
        details: "boom".to_string(),
    }
    .into();
    assert_eq!(other.to_string(), "database operation failed: boom");
}

#[test]
fn decoder_is_shareable_across_threads() {
    // A configured decoder is Send + Sync + Clone, so it can back parallel sims.
    let decoder = Arc::new(RevertDecoder::new().with_error::<Unauthorized>());
    let data = Bytes::from(
        Unauthorized {
            caller: alloy_primitives::Address::repeat_byte(0xAB),
        }
        .abi_encode(),
    );

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let decoder = Arc::clone(&decoder);
            let data = data.clone();
            std::thread::spawn(move || matches!(decoder.decode(&data), RevertReason::Custom(_)))
        })
        .collect();

    for handle in handles {
        assert!(handle.join().expect("thread panicked"));
    }
}

#[test]
fn duplicate_selector_registration_keeps_first_and_try_register_reports_error() {
    let mut decoder = RevertDecoder::new();
    decoder
        .try_register_raw([0x11, 0x22, 0x33, 0x44], "First(uint256)", |_| {
            Some("first".to_string())
        })
        .expect("first registration succeeds");
    let err = decoder
        .try_register_raw([0x11, 0x22, 0x33, 0x44], "Second(uint256)", |_| {
            Some("second".to_string())
        })
        .expect_err("try_register_raw must reject duplicate selectors");
    assert!(
        err.to_string().contains("duplicate") || err.to_string().contains("selector"),
        "unexpected duplicate selector error: {err}"
    );
    decoder.register_raw([0x11, 0x22, 0x33, 0x44], "Second(uint256)", |_| {
        Some("second".to_string())
    });
    assert_eq!(
        decoder.len(),
        1,
        "duplicate registration must not add an entry"
    );

    let data = Bytes::from(vec![0x11, 0x22, 0x33, 0x44]);
    match decoder.decode(&data) {
        RevertReason::Custom(custom) => {
            assert_eq!(custom.name, "First(uint256)");
            assert_eq!(custom.params.as_deref(), Some("first"));
        }
        other => panic!("expected the original Custom error, got {other}"),
    }
}

#[test]
fn out_of_range_panic_code_falls_through_to_unknown() {
    // A Panic(uint256) whose code exceeds u64::MAX cannot be represented, so it
    // is reported as Unknown rather than Panic (KNOWN_ISSUES item 7).
    let data = Bytes::from(Panic { code: U256::MAX }.abi_encode());
    match RevertDecoder::new().decode(&data) {
        RevertReason::Unknown { selector, .. } => {
            assert_eq!(selector.as_slice(), &PANIC_SELECTOR);
        }
        other => panic!("expected Unknown for an out-of-range panic, got {other}"),
    }
}

#[test]
fn in_range_panic_code_decodes_to_panic() {
    // The companion to the overflow case: a normal single-byte panic code
    // decodes to Panic, confirming the selector itself is wired up correctly.
    let data = Bytes::from(
        Panic {
            code: U256::from(0x11u64),
        }
        .abi_encode(),
    );
    assert_eq!(
        RevertDecoder::new().decode(&data),
        RevertReason::Panic(0x11)
    );
}
