// SPDX-License-Identifier: MIT OR Apache-2.0
pragma solidity ^0.8.28;

/// @title MockERC20
/// @notice Minimal ERC20 used as a test fixture for `evm-fork-cache`. The
///         compiled bytecode is checked in as `mock_erc20_runtime.hex`
///         (runtime) and `mock_erc20_creation.hex` (creation) so the tests and
///         examples run without a Solidity toolchain. This source documents the
///         exact contract those bytecode blobs were compiled from.
/// @dev Storage layout (relied on by the tests):
///        slot 0: name
///        slot 1: symbol
///        slot 2: totalSupply
///        slot 3: balanceOf  (mapping(address => uint256))
///        slot 4: allowance  (mapping(address => mapping(address => uint256)))
///      `decimals` is immutable and therefore not stored. The balance of
///      `owner` lives at `keccak256(abi.encode(owner, uint256(3)))`.
contract MockERC20 {
    event Transfer(address indexed from, address indexed to, uint256 value);

    string public name;
    string public symbol;
    uint8 public immutable decimals;

    uint256 public totalSupply;
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;

    constructor(string memory _name, string memory _symbol, uint8 _decimals) {
        name = _name;
        symbol = _symbol;
        decimals = _decimals;
    }

    function transfer(address to, uint256 amount) public returns (bool) {
        _transfer(msg.sender, to, amount);
        return true;
    }

    function approve(address spender, uint256 amount) public returns (bool) {
        allowance[msg.sender][spender] = amount;
        return true;
    }

    function transferFrom(address from, address to, uint256 amount) public returns (bool) {
        uint256 allowed = allowance[from][msg.sender];
        require(allowed >= amount, "allowance");
        allowance[from][msg.sender] = allowed - amount;
        _transfer(from, to, amount);
        return true;
    }

    function _transfer(address from, address to, uint256 amount) internal {
        require(balanceOf[from] >= amount, "balance");
        balanceOf[from] -= amount;
        balanceOf[to] += amount;
        emit Transfer(from, to, amount);
    }

    function _mint(address to, uint256 amount) external {
        totalSupply += amount;
        balanceOf[to] += amount;
    }
}
