//! CREATE3 deployment-address derivation.
//!
//! CREATE3 makes a contract's deployed address depend only on the deploying
//! factory and a salt, independent of the contract's init code. This module
//! reproduces that derivation off-chain: it computes the CREATE2 proxy address
//! the factory would create from `(deployer, salt)`, then the CREATE address
//! the proxy deploys to. This lets callers predict an address before a
//! transaction is sent.

use alloy_primitives::{Address, B256, address, b256, keccak256};

/// Address of the widely deployed universal CREATE3 factory (the CreateX /
/// CREATE3 factory implementation).
///
/// This is the canonical cross-chain address at which the CreateX-style
/// CREATE3 factory has been deterministically deployed on many EVM networks.
/// The derivation in this module assumes the factory at this address uses the
/// CREATE3 proxy init code whose hash is `CREATE3_PROXY_INITCODE_HASH`.
///
/// Callers must verify the factory is actually deployed at this address on
/// their target chain before relying on a derived address: if the factory is
/// absent (or a chain hosts a different factory implementation), the derived
/// address will not correspond to any real deployment.
pub const UNIVERSAL_CREATE3_FACTORY: Address = address!("93FEC2C00BfE902F733B57c5a6CeeD7CD1384AE1");

// CREATE3 proxy initcode used by the universal factory implementation.
// keccak256(0x67363d3d37363d34f03d5260086018f3)
const CREATE3_PROXY_INITCODE_HASH: B256 =
    b256!("21c35dbe1b344a2488cf3321d6ce542f8e9f305544ff09e4993a62319a497c1f");

/// Derive CREATE3 deployment address for the universal factory implementation.
///
/// CREATE3 deploys in two hops: the factory first `CREATE2`-deploys a tiny
/// fixed proxy, then that proxy `CREATE`s the actual contract as its first
/// (nonce-1) deployment. Because both hops use only the factory, the salt, and
/// a fixed proxy init code, the final address depends solely on `factory`,
/// `deployer`, and `salt` — it is **independent of the deployed contract's
/// bytecode**. Two different contracts deployed with the same inputs land at
/// the same address.
///
/// Formula:
/// 1. `mixedSalt = keccak256(abi.encodePacked(deployer, salt))` — binds the
///    salt to the logical deployer.
/// 2. `proxy = create2(factory, mixedSalt, CREATE3_PROXY_INITCODE_HASH)` —
///    the CREATE2 address of the proxy. `CREATE3_PROXY_INITCODE_HASH` is the
///    keccak256 of the fixed proxy init code, so the proxy address is fully
///    determined by the factory and mixed salt.
/// 3. `deployed = address(keccak256(rlp([proxy, 1])))` — the CREATE address of
///    the proxy's first deployment (nonce 1). The RLP framing bytes encode the
///    short list `[proxy, 1]`: `0xd6` is the RLP list header for the 22-byte
///    payload that follows, `0x94` introduces the 20-byte `proxy` address, and
///    `0x01` is the RLP encoding of the proxy's nonce (1), since a fresh
///    contract account's first `CREATE` uses nonce 1.
///
/// The address is returned as the low 20 bytes of each keccak256 hash, matching
/// the EVM's address-from-hash convention.
///
/// `factory` lets you derive against a non-canonical factory deployment; for
/// the canonical address use [`derive_universal_create3_address`].
///
/// ```
/// use evm_fork_cache::create3::derive_create3_address;
/// use alloy_primitives::{Address, B256, address, b256};
///
/// let factory: Address = address!("93FEC2C00BfE902F733B57c5a6CeeD7CD1384AE1");
/// let deployer: Address = address!("00000000000000000000000000000000000000aa");
/// let salt: B256 =
///     b256!("1111111111111111111111111111111111111111111111111111111111111111");
///
/// // The derivation is a pure function of (factory, deployer, salt): identical
/// // inputs always yield the same address.
/// let a = derive_create3_address(factory, deployer, salt);
/// let b = derive_create3_address(factory, deployer, salt);
/// assert_eq!(a, b);
///
/// // Changing the salt changes the derived address.
/// let other_salt: B256 =
///     b256!("2222222222222222222222222222222222222222222222222222222222222222");
/// assert_ne!(a, derive_create3_address(factory, deployer, other_salt));
/// ```
pub fn derive_create3_address(factory: Address, deployer: Address, salt: B256) -> Address {
    let mut mixed_salt_input = [0u8; 52];
    mixed_salt_input[..20].copy_from_slice(deployer.as_slice());
    mixed_salt_input[20..].copy_from_slice(salt.as_slice());
    let mixed_salt = keccak256(mixed_salt_input);

    let mut create2_preimage = [0u8; 85];
    create2_preimage[0] = 0xff;
    create2_preimage[1..21].copy_from_slice(factory.as_slice());
    create2_preimage[21..53].copy_from_slice(mixed_salt.as_slice());
    create2_preimage[53..85].copy_from_slice(CREATE3_PROXY_INITCODE_HASH.as_slice());
    let proxy_hash = keccak256(create2_preimage);
    let proxy = Address::from_slice(&proxy_hash.as_slice()[12..]);

    let mut create_preimage = [0u8; 23];
    create_preimage[0] = 0xd6;
    create_preimage[1] = 0x94;
    create_preimage[2..22].copy_from_slice(proxy.as_slice());
    create_preimage[22] = 0x01;
    let deployed_hash = keccak256(create_preimage);

    Address::from_slice(&deployed_hash.as_slice()[12..])
}

/// Derive CREATE3 deployment address via the universal factory.
///
/// Convenience wrapper around [`derive_create3_address`] that uses
/// [`UNIVERSAL_CREATE3_FACTORY`] as the factory. As with the general form, the
/// result depends only on `deployer` and `salt`, not on the deployed bytecode,
/// and is only meaningful on chains where that factory is actually deployed.
///
/// ```
/// use evm_fork_cache::create3::derive_universal_create3_address;
/// use alloy_primitives::{Address, B256, address, b256};
///
/// let deployer: Address = address!("00000000000000000000000000000000000000aa");
/// let salt: B256 =
///     b256!("1111111111111111111111111111111111111111111111111111111111111111");
///
/// // Deterministic: the same (deployer, salt) always derive the same address.
/// assert_eq!(
///     derive_universal_create3_address(deployer, salt),
///     derive_universal_create3_address(deployer, salt),
/// );
/// ```
pub fn derive_universal_create3_address(deployer: Address, salt: B256) -> Address {
    derive_create3_address(UNIVERSAL_CREATE3_FACTORY, deployer, salt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_universal_create3_address_matches_factory_reference_vector() {
        let deployer: Address = address!("C8bDb57Afa96E05DbE9d00a93Bf6863dfF634D59");
        let salt: B256 = b256!("3e423a81e6ff85145e727e92fd89e4775e1fb188ed74b9f1f6e3679b7af66626");

        let expected: Address = address!("eFef040ed447a25cF61277990DE61a429BF8F3e4");
        let derived = derive_universal_create3_address(deployer, salt);

        assert_eq!(derived, expected);
    }

    /// The derivation must be a pure function of its inputs: identical inputs
    /// always yield the same address, and changing the salt changes the address.
    #[test]
    fn test_derive_is_deterministic_and_salt_sensitive() {
        let deployer: Address = address!("00000000000000000000000000000000000000aa");
        let salt_a: B256 =
            b256!("1111111111111111111111111111111111111111111111111111111111111111");
        let salt_b: B256 =
            b256!("2222222222222222222222222222222222222222222222222222222222222222");

        // Same inputs -> same output.
        assert_eq!(
            derive_universal_create3_address(deployer, salt_a),
            derive_universal_create3_address(deployer, salt_a),
        );

        // Different salt -> different address.
        assert_ne!(
            derive_universal_create3_address(deployer, salt_a),
            derive_universal_create3_address(deployer, salt_b),
        );

        // Different deployer (with the same salt) -> different address.
        let other_deployer: Address = address!("00000000000000000000000000000000000000bb");
        assert_ne!(
            derive_universal_create3_address(deployer, salt_a),
            derive_universal_create3_address(other_deployer, salt_a),
        );
    }
}
