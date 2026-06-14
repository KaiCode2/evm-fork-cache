//! CREATE3 deployment-address derivation.
//!
//! CREATE3 makes a contract's deployed address depend only on the deploying
//! factory and a salt, independent of the contract's init code. This module
//! reproduces that derivation off-chain: it computes the CREATE2 proxy address
//! the factory would create from `(deployer, salt)`, then the CREATE address
//! the proxy deploys to. This lets callers predict an address before a
//! transaction is sent.

use alloy_primitives::{Address, B256, address, b256, keccak256};

/// A widely deployed universal CREATE3 factory implementation.
pub const UNIVERSAL_CREATE3_FACTORY: Address = address!("93FEC2C00BfE902F733B57c5a6CeeD7CD1384AE1");

// CREATE3 proxy initcode used by the universal factory implementation.
// keccak256(0x67363d3d37363d34f03d5260086018f3)
const CREATE3_PROXY_INITCODE_HASH: B256 =
    b256!("21c35dbe1b344a2488cf3321d6ce542f8e9f305544ff09e4993a62319a497c1f");

/// Derive CREATE3 deployment address for the universal factory implementation.
///
/// Formula:
/// 1) mixedSalt = keccak256(abi.encodePacked(deployer, salt))
/// 2) proxy = create2(factory, mixedSalt, CREATE3_PROXY_INITCODE_HASH)
/// 3) deployed = address(keccak256(rlp([proxy, 1])))
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
