//! Derive CREATE3 deployment addresses off-chain.
//!
//! CREATE3 makes a deployed address depend only on `(factory, deployer, salt)`,
//! independent of the contract's init code — so you can predict an address
//! before sending any transaction. This example derives addresses for the
//! widely deployed universal CREATE3 factory.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example create3_addresses
//! ```

use alloy_primitives::{Address, B256, b256};
use evm_fork_cache::create3::{
    UNIVERSAL_CREATE3_FACTORY, derive_create3_address, derive_universal_create3_address,
};

fn main() {
    let deployer: Address = "0xC8bDb57Afa96E05DbE9d00a93Bf6863dfF634D59"
        .parse()
        .unwrap();

    let salt_a: B256 = b256!("1111111111111111111111111111111111111111111111111111111111111111");
    let salt_b: B256 = b256!("2222222222222222222222222222222222222222222222222222222222222222");

    println!("universal CREATE3 factory: {UNIVERSAL_CREATE3_FACTORY}");
    println!("deployer:                  {deployer}\n");

    let addr_a = derive_universal_create3_address(deployer, salt_a);
    let addr_b = derive_universal_create3_address(deployer, salt_b);
    println!("salt A -> {addr_a}");
    println!("salt B -> {addr_b}");
    assert_ne!(
        addr_a, addr_b,
        "different salts must yield different addresses"
    );

    // The derivation is a pure function of its inputs: re-deriving is stable.
    let again = derive_universal_create3_address(deployer, salt_a);
    assert_eq!(addr_a, again, "derivation must be deterministic");
    println!("\nre-deriving salt A is stable: {again}");

    // You can target any CREATE3 factory address, not just the universal one.
    let custom_factory = Address::repeat_byte(0xF0);
    let via_custom = derive_create3_address(custom_factory, deployer, salt_a);
    println!("\nsame salt via a custom factory ({custom_factory}):");
    println!("  -> {via_custom}");
    assert_ne!(
        addr_a, via_custom,
        "a different factory yields a different address"
    );
}
