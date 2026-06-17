//! Apply a batch of targeted [`StateUpdate`]s and inspect the returned
//! [`StateDiff`] (Phase 3, Pillar B.1) — fully offline.
//!
//! Builds a mocked-provider cache, seeds a little state, then applies a mixed
//! batch — a `Slot` write, an `Account` balance patch, and a `Purge { Slots }` —
//! through the single [`EvmCache::apply_update`] / `apply_updates` primitive, and
//! prints what each apply actually changed (slot deltas, account deltas, purge
//! records). It then shows a *relative* `SlotDelta` balance bump on a hot slot and
//! a cold-slot `SlotDelta` surfaced (not applied) via `diff.skipped`. No network
//! is touched.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example state_update_apply
//! ```

use alloy_primitives::{Address, U256};
use anyhow::Result;
use evm_fork_cache::{PurgeScope, SlotDelta, StateUpdate};

#[path = "support/mock.rs"]
mod mock;

use mock::{install_default_account, install_mock_erc20, offline_cache};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let pool = Address::repeat_byte(0x11);
    let holder = Address::repeat_byte(0x22);

    let mut cache = offline_cache().await?;
    // A token-like account with overlay storage (so the slot write heals both
    // layers) plus an EOA-style account to patch a balance onto.
    install_mock_erc20(&mut cache, pool);
    install_default_account(&mut cache, holder);

    // Seed some backend storage on the pool so the purge has something to remove
    // and the slot write has a recorded `old` value.
    cache.inject_storage_batch(&[
        (pool, U256::from(0), U256::from(100)), // e.g. a reserve slot
        (pool, U256::from(7), U256::from(1)),   // a tick/aux slot we'll purge
        (pool, U256::from(8), U256::from(2)),   // another slot we'll purge
    ]);

    println!("Applying a mixed batch of state updates...\n");

    let diff = cache.apply_updates(&[
        // 1. Authoritative slot write (e.g. an event-derived reserve update).
        StateUpdate::slot(pool, U256::from(0), U256::from(250)),
        // 2. Partial account patch: set only the balance, leave nonce/code.
        StateUpdate::balance(holder, U256::from(1_000_000)),
        // 3. Drop two stale storage slots so the next read re-fetches them.
        StateUpdate::purge(pool, PurgeScope::Slots(vec![U256::from(7), U256::from(8)])),
    ]);

    println!("StateDiff: {} changed entr(ies)\n", diff.len());

    println!("Slot changes ({}):", diff.slots.len());
    for change in &diff.slots {
        println!(
            "  {} slot {} : {} -> {}",
            change.address, change.slot, change.old, change.new
        );
    }

    println!("\nAccount changes ({}):", diff.accounts.len());
    for change in &diff.accounts {
        println!("  {}", change.address);
        if let Some((old, new)) = change.balance {
            println!("    balance: {old} -> {new}");
        }
        if let Some((old, new)) = change.nonce {
            println!("    nonce:   {old} -> {new}");
        }
        if let Some((old, new)) = change.code_hash {
            println!("    code:    {old} -> {new}");
        }
    }

    println!("\nPurge records ({}):", diff.purged.len());
    for rec in &diff.purged {
        println!(
            "  {} scope={:?} slots_removed={} account_removed={}",
            rec.address, rec.scope, rec.slots_removed, rec.account_removed
        );
    }

    // Re-applying the same slot value is a no-op — idempotence is observable.
    let again = cache.apply_update(&StateUpdate::slot(pool, U256::from(0), U256::from(250)));
    println!(
        "\nRe-applying the same slot value -> empty diff: {}",
        again.is_empty()
    );

    // --- Relative (read-modify-write) updates -------------------------------
    //
    // A caller indexing ERC-20 `Transfer` logs only learns the *delta*
    // (`amount`), not the resulting balance. `SlotDelta` reads the current value
    // and applies a saturating mutation, write-through.
    println!("\n--- Relative SlotDelta updates ---");

    // A hot (seeded) balance slot: +750 relative to the current value.
    let hot_slot = U256::from(0); // we set this to 250 above
    let rel = cache.apply_update(&StateUpdate::slot_delta(
        pool,
        hot_slot,
        SlotDelta::Add(U256::from(750)),
    ));
    for change in &rel.slots {
        println!(
            "  hot  : slot {} {} -> {} (Add 750)",
            change.slot, change.old, change.new
        );
    }

    // A cold slot the cache never fetched: applying `0 ± amount` would corrupt an
    // unknown value, so the delta is NOT applied — it is surfaced for the caller
    // to fetch+seed the true value and retry.
    let cold_slot = U256::from(4_242);
    let cold = cache.apply_update(&StateUpdate::slot_delta(
        pool,
        cold_slot,
        SlotDelta::Add(U256::from(100)),
    ));
    println!(
        "  cold : applied {} change(s), skipped {} (left for the caller to seed)",
        cold.slots.len(),
        cold.skipped.len()
    );
    for skip in &cold.skipped {
        println!(
            "    skipped: {} slot {} delta={:?}",
            skip.address, skip.slot, skip.delta
        );
    }

    // --- Relative native-balance updates (BalanceDelta) ---------------------
    //
    // The same cold-aware read-modify-write rule applies to an account's native
    // ETH balance: a `BalanceDelta` on a *present* account bumps its balance; on a
    // *cold* account (absent from both layers) it is dropped and surfaced.
    println!("\n--- Relative BalanceDelta updates ---");

    // `holder` was installed above (present), so a +500_000 delta applies.
    let bal = cache.apply_update(&StateUpdate::balance_delta(
        holder,
        SlotDelta::Add(U256::from(500_000)),
    ));
    for change in &bal.accounts {
        if let Some((old, new)) = change.balance {
            println!(
                "  hot  : {} balance {} -> {} (Add 500_000)",
                change.address, old, new
            );
        }
    }

    // A cold account the cache never loaded: the balance is unknown, so the delta
    // is NOT applied (no default account is materialized to mask the real one) —
    // it is surfaced in `diff.skipped_balances`.
    let unknown = Address::repeat_byte(0x99);
    let cold_bal = cache.apply_update(&StateUpdate::balance_delta(
        unknown,
        SlotDelta::Add(U256::from(1_000)),
    ));
    // A cold-skipped relative update produces no change, so it is invisible to the
    // changes-only `is_empty()`/`len()` check — callers MUST inspect `has_skipped()`.
    println!(
        "  cold : has_skipped={} skipped_len={} (changes-only len={})",
        cold_bal.has_skipped(),
        cold_bal.skipped_len(),
        cold_bal.len(),
    );
    for skip in &cold_bal.skipped_balances {
        println!(
            "    skipped balance: {} delta={:?}",
            skip.address, skip.delta
        );
    }

    Ok(())
}
