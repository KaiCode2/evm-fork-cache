//! Discover hash-derived storage slots from a trace, then track holders.
//!
//! Demonstrates the v0.2.1 discovery API on [`EvmCache`]:
//!   * [`discover_erc20_balance_slot`] — layout-agnostic balance-slot discovery
//!     (Solidity / Vyper / Solady) from a single `balanceOf` simulation;
//!   * [`trace_hashed_slots`] — the general, ERC-20-agnostic resolver (used here
//!     on a nested allowance);
//!   * [`set_erc20_balance_with_slot_scan`] — now discover-first, so it forges
//!     balances on Vyper/Solady tokens the old Solidity-only scan could not;
//!   * [`track_erc20_balances`] — "discover once, then track these addresses",
//!     pinning each holder's slot into a [`FreshnessRegistry`].
//!
//! Runs fully offline, then (best-effort) forks real mainnet tokens if a public
//! RPC is reachable. Set `RPC_URL` to a reliable endpoint for a clean sweep.
//!
//! ```sh
//! cargo run --example discover_and_track
//! ```
//!
//! [`discover_erc20_balance_slot`]: evm_fork_cache::cache::EvmCache::discover_erc20_balance_slot
//! [`trace_hashed_slots`]: evm_fork_cache::cache::EvmCache::trace_hashed_slots
//! [`set_erc20_balance_with_slot_scan`]: evm_fork_cache::cache::EvmCache::set_erc20_balance_with_slot_scan
//! [`track_erc20_balances`]: evm_fork_cache::cache::EvmCache::track_erc20_balances

#[path = "support/mock.rs"]
mod mock;

use std::sync::Arc;

use alloy_primitives::{Address, Bytes, U256, address, keccak256};
use alloy_provider::ProviderBuilder;
use alloy_provider::network::AnyNetwork;
use alloy_sol_types::{SolCall, sol};
use anyhow::Result;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::{FreshnessRegistry, SlotLayout, Validity};
use revm::primitives::hardfork::SpecId;
use revm::state::{AccountInfo, Bytecode};
use tokio::runtime::Runtime;

const SOLADY_SEED: u32 = 0x87a2_11a2;

sol! {
    interface IErc20 {
        function balanceOf(address account) returns (uint256);
        function allowance(address owner, address spender) returns (uint256);
    }
}

// --- tiny bytecode builders for non-Solidity layouts (no compiler needed) ---

fn push1(v: &mut Vec<u8>, b: u8) {
    v.extend_from_slice(&[0x60, b]);
}
fn push4(v: &mut Vec<u8>, x: u32) {
    v.push(0x63);
    v.extend_from_slice(&x.to_be_bytes());
}

/// `balanceOf(addr)` = `sload(keccak256(slot ‖ addr))` — Vyper byte order.
fn vyper_runtime(slot: u8) -> Vec<u8> {
    let mut c = Vec::new();
    push1(&mut c, slot);
    push1(&mut c, 0x00);
    c.push(0x52);
    push1(&mut c, 0x04);
    c.push(0x35);
    push1(&mut c, 0x20);
    c.push(0x52);
    push1(&mut c, 0x40);
    push1(&mut c, 0x00);
    c.push(0x20);
    c.push(0x54);
    push1(&mut c, 0x00);
    c.push(0x52);
    push1(&mut c, 0x20);
    push1(&mut c, 0x00);
    c.push(0xf3);
    c
}

/// `balanceOf(addr)` = Solady's packed `sload(keccak256(0x0c, 0x20))`.
fn solady_runtime(seed: u32) -> Vec<u8> {
    let mut c = Vec::new();
    push4(&mut c, seed);
    push1(&mut c, 0x0c);
    c.push(0x52);
    push1(&mut c, 0x04);
    c.push(0x35);
    push1(&mut c, 0x00);
    c.push(0x52);
    push1(&mut c, 0x20);
    push1(&mut c, 0x0c);
    c.push(0x20);
    c.push(0x54);
    push1(&mut c, 0x00);
    c.push(0x52);
    push1(&mut c, 0x20);
    push1(&mut c, 0x00);
    c.push(0xf3);
    c
}

fn install_runtime(cache: &mut EvmCache, addr: Address, code: Vec<u8>) {
    let bytecode = Bytecode::new_raw(Bytes::from(code));
    let code_hash = bytecode.hash_slow();
    cache.db_mut().insert_account_info(
        addr,
        AccountInfo {
            code: Some(bytecode),
            code_hash,
            ..Default::default()
        },
    );
    cache
        .db_mut()
        .replace_account_storage(addr, Default::default())
        .expect("mark storage local");
}

// ---------------------------------------------------------------------------

async fn run_offline() -> Result<()> {
    println!("################  OFFLINE  ################");
    let mut cache = mock::offline_cache().await?;
    let sol_tok = Address::repeat_byte(0x11);
    let vyper_tok = Address::repeat_byte(0x22);
    let solady_tok = Address::repeat_byte(0x33);
    let alice = address!("00000000000000000000000000000000000000A1");
    let bob = address!("00000000000000000000000000000000000000B2");
    let carol = address!("00000000000000000000000000000000000000C3");

    for a in [Address::ZERO, alice, bob, carol] {
        mock::install_default_account(&mut cache, a);
    }
    mock::install_mock_erc20(&mut cache, sol_tok);
    install_runtime(&mut cache, vyper_tok, vyper_runtime(2));
    install_runtime(&mut cache, solady_tok, solady_runtime(SOLADY_SEED));
    cache.insert_mapping_storage_slot(sol_tok, U256::from(3u64), alice, U256::from(1000u64))?;

    // 1. Layout-agnostic balance-slot discovery across three token styles.
    println!("\n-- discover_erc20_balance_slot (one sim each) --");
    for (name, tok) in [
        ("Solidity", sol_tok),
        ("Vyper", vyper_tok),
        ("Solady", solady_tok),
    ] {
        match cache.discover_erc20_balance_slot(tok, alice)? {
            Some(a) => println!(
                "  {name:<8} base_slot={:<11} layout={:<28} slot={}",
                a.base_slot, a.layout, a.slot
            ),
            None => println!("  {name:<8} (no hashed balance read)"),
        }
    }

    // 2. General resolver on a nested mapping (allowance) — no ERC-20 assumptions.
    println!("\n-- trace_hashed_slots on allowance(alice, bob) --");
    let inner = keccak256(sol_encode(alice, U256::from(4u64)));
    let outer = keccak256(sol_encode(bob, U256::from_be_bytes(inner.0)));
    cache
        .db_mut()
        .insert_account_storage(sol_tok, U256::from_be_bytes(outer.0), U256::from(555u64))
        .unwrap();
    let calldata = Bytes::from(
        IErc20::allowanceCall {
            owner: alice,
            spender: bob,
        }
        .abi_encode(),
    );
    let known = [alice.into_word(), bob.into_word()];
    for a in cache.trace_hashed_slots(alice, sol_tok, calldata, &known)? {
        let keys: Vec<String> = a
            .keys
            .iter()
            .map(|k| format!("{}", Address::from_word(*k)))
            .collect();
        println!(
            "  base_slot={} depth={} layout={} keys=[{}]",
            a.base_slot,
            a.depth,
            a.layout,
            keys.join(", ")
        );
    }

    // 3. Forging balances now works on Vyper/Solady tokens (discover-first).
    println!("\n-- set_erc20_balance_with_slot_scan (max_slot=0 → must use discovery) --");
    for (name, tok) in [
        ("Solidity", sol_tok),
        ("Vyper", vyper_tok),
        ("Solady", solady_tok),
    ] {
        let ok = cache.set_erc20_balance_with_slot_scan(tok, bob, U256::from(9_999u64), 0)?;
        let got: U256 = cache.call_sol(tok, IErc20::balanceOfCall { account: bob })?;
        println!("  {name:<8} set ok={ok}  balanceOf(bob)={got}");
    }

    // 4. "Discover once, then track these addresses" → pin into a FreshnessRegistry.
    println!("\n-- track_erc20_balances + pin into FreshnessRegistry --");
    let mut fresh = FreshnessRegistry::new();
    if let Some((tracked, pairs)) = cache.track_erc20_balances(sol_tok, [alice, bob, carol])? {
        println!(
            "  layout = {} @ base slot {}",
            tracked.layout, tracked.base_slot
        );
        for (holder, slot) in &pairs {
            fresh.set_slot(sol_tok, U256::from_be_bytes(slot.0), Validity::Volatile);
            println!("    tracking {holder} at slot {slot}");
        }
        // Reuse the descriptor for a brand-new holder with no re-simulation.
        let dave = address!("00000000000000000000000000000000000000D4");
        let dave_slot = tracked.slot_for(dave.into_word()).unwrap();
        println!("  reuse for {dave} → slot {dave_slot} (no new sim)");
    }
    let _ = SlotLayout::Opaque; // (SlotLayout is part of the public surface)

    Ok(())
}

// ---------------------------------------------------------------------------

struct Token {
    name: &'static str,
    addr: Address,
    holder: Address,
}

async fn run_mainnet() -> Result<()> {
    let rpc = std::env::var("RPC_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "https://ethereum-rpc.publicnode.com".to_string());
    println!("\n################  MAINNET (fork via {rpc})  ################");

    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .connect_http(rpc.parse()?);
    let mut cache = EvmCache::builder(Arc::new(provider))
        .latest_block()
        .spec(SpecId::CANCUN)
        .build()
        .await;

    let permit2 = address!("000000000022D473030F116dDEE9F6B43aC78BA3");
    let tokens = [
        Token {
            name: "USDC",
            addr: address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            holder: address!("28C6c06298d514Db089934071355E5743bf21d60"),
        },
        Token {
            name: "WETH",
            addr: address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
            holder: address!("8EB8a3b98659Cce290402893d0123abb75E3ab28"),
        },
        Token {
            name: "USDT",
            addr: address!("dAC17F958D2ee523a2206206994597C13D831ec7"),
            holder: address!("28C6c06298d514Db089934071355E5743bf21d60"),
        },
        Token {
            name: "3CRV(Vyper)",
            addr: address!("6c3F90f043a72FA612cbac8115EE7e52BDe6E490"),
            holder: address!("d632f22692FaC7611d2AA1C0D552930D43CAEd3B"),
        },
    ];

    for t in &tokens {
        match cache.discover_erc20_balance_slot(t.addr, t.holder) {
            Ok(Some(a)) => println!(
                "  {:<12} balance  base_slot={:<3} layout={:<28} value={}",
                t.name, a.base_slot, a.layout, a.value
            ),
            Ok(None) => println!(
                "  {:<12} balance  (no hashed read — try a reliable RPC_URL)",
                t.name
            ),
            Err(e) => println!("  {:<12} balance  SKIPPED ({e})", t.name),
        }
        let cd = Bytes::from(
            IErc20::allowanceCall {
                owner: t.holder,
                spender: permit2,
            }
            .abi_encode(),
        );
        let known = [t.holder.into_word(), permit2.into_word()];
        if let Ok(accesses) = cache.trace_hashed_slots(t.holder, t.addr, cd, &known)
            && let Some(a) = accesses.iter().find(|a| a.layout == SlotLayout::Nested)
        {
            println!(
                "  {:<12} allowance base_slot={} depth={}",
                t.name, a.base_slot, a.depth
            );
        }
    }
    Ok(())
}

fn sol_encode(addr: Address, slot: U256) -> [u8; 64] {
    let mut out = [0u8; 64];
    out[0..32].copy_from_slice(addr.into_word().as_slice());
    out[32..64].copy_from_slice(&slot.to_be_bytes::<32>());
    out
}

fn main() -> Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(run_offline())?;
    if let Err(e) = rt.block_on(run_mainnet()) {
        println!("\n[mainnet suite skipped: {e}]");
    }
    Ok(())
}
