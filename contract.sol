// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;
import "@openzeppelin/contracts/interfaces/IERC20.sol";

contract KaiFan_AlphaBot {
    address public immutable OKX_ROUTER =
        0x9b9efa5Efa731EA9Bbb0369E91fA17Abf249CFD4;
    address public immutable OKX_TOKEN_APROVAL =
        0x2c34A2Fb1d0b4f55de51E1d0bDEfaDDce6b7cDD6;
    address public immutable NATIVE_TOKEN =
        0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE;
    address public owner;
    mapping(address => bool) public botEOA;
    mapping(uint8 => address) public builderEOA;

    constructor() {
        owner = msg.sender;
    }

    modifier onlyOwner() {
        require(owner == msg.sender);
        _;
    }

    modifier onlyBot() {
        require(msg.sender == owner || botEOA[msg.sender]);
        _;
    }

    function changeOwner(address _owner) public onlyOwner {
        owner = _owner;
    }

    function setBuilderStatus(uint8 _id, address _builder) public onlyOwner {
        builderEOA[_id] = _builder;
    }

    function setBotStatus(address _bot, bool _isEnable) public onlyOwner {
        botEOA[_bot] = _isEnable;
    }

    function setApprovalToken(address _token) public onlyOwner {
        IERC20(_token).approve(OKX_TOKEN_APROVAL, type(uint256).max);
    }

    function setRevokeToken(address _token) public onlyOwner {
        IERC20(_token).approve(OKX_TOKEN_APROVAL, 0);
    }

    function call(
        address _user,
        address _fromToken,
        address _toToken,
        uint256 _amount,
        uint8 _builderEOA,
        bytes calldata _data
    ) public payable onlyBot {
        IERC20(_fromToken).transferFrom(_user, address(this), _amount);
        (bool success, ) = OKX_ROUTER.call(_data);
        require(success, "Failed to call ROUTER");
        if (_toToken != NATIVE_TOKEN) {
            IERC20 _erc20 = IERC20(_toToken);
            _erc20.transfer(_user, _erc20.balanceOf(address(this)));
        } else {
            _user.call{value: (address(this).balance - msg.value)}("");
        }
        if (_builderEOA != 255 && msg.value != 0) {
            builderEOA[_builderEOA].call{value: msg.value}("");
        }
    }

    receive() external payable {}
}