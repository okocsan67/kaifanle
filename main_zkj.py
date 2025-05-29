import time

from web3 import AsyncWeb3, WebSocketProvider, Web3, HTTPProvider, AsyncHTTPProvider
from eth_abi import decode
from web3.middleware import ExtraDataToPOAMiddleware
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
from web3.utils.subscriptions import (
    LogsSubscription, LogsSubscriptionContext
)
import aiohttp
import hmac
import hashlib
import base64
import json
from datetime import datetime
from urllib.parse import urlencode

import threading
from typing import List
from eth_utils import keccak
import sys
import os
import pymysql
import socket
import aiomysql
from pathlib import Path


rpc_url = ""  # 替换为有效 BSC WebSocket URL

#blockrazor注册一个，用来发交易的
blockrazor_auth = ''

token_name = "zkj"  # token名 对应数据库的代币名称
bot_id = 1  #不用动
toTokenReferrerWalletAddress = ""  #你接收手续费的地址

db_config = {
    'host': 'xx.xx.xx.xx',
    'user': 'root',
    'port': 3306,
    'password': 'xxxxxxxxx',
    'db': 'alphabot',
    'charset': 'utf8mb4'
}

GAS_LIMIT = 2000000
MEV_GAS_PRICE = Web3.to_wei('0.1', 'gwei')  #发送交易的gas price
BRIBE_AMOUNT = Web3.to_wei('0.00001', 'ether') #贿赂给服务商的费用，快速上链
CHAINID = 56
SLIPPAGE = "0.001"  #默认滑点 千1
FEE_PERCENT = "0.001" #默认手续费 万1

# 创建一个容量为 1000 的队列
max_queue_size = 1000
task_queue = asyncio.Queue(maxsize=max_queue_size)

###################以下配置没特殊情况不用改##########################


# 确保 logs/ 目录存在
log_dir = os.path.join(os.getcwd(), 'logs')
os.makedirs(log_dir, exist_ok=True)  # 如果目录不存在则创建


api_config = {
}

# 连接数据库
connection = pymysql.connect(**db_config)

# 全局异步连接池
pool = None

# 合约地址
sell_contract_address = None
token_contract_address = None


# ABI
sell_contract_abi = [{"inputs": [{"internalType": "address","name": "_user","type": "address"},{"internalType": "address","name": "_fromToken","type": "address"},{"internalType": "address","name": "_toToken","type": "address"},{"internalType": "uint256","name": "_amount","type": "uint256"},{"internalType": "uint8","name": "_builderEOA","type": "uint8"},{"internalType": "bytes","name": "_data","type": "bytes"}],"name": "call","outputs": [],"stateMutability": "payable","type": "function"},{"inputs": [{"internalType": "address","name": "_owner","type": "address"}],"name": "changeOwner","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "address","name": "_token","type": "address"}],"name": "setApprovalToken","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "address","name": "_bot","type": "address"},{"internalType": "bool","name": "_isEnable","type": "bool"}],"name": "setBotStatus","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "uint8","name": "_id","type": "uint8"},{"internalType": "address","name": "_builder","type": "address"}],"name": "setBuilderStatus","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "address","name": "_token","type": "address"}],"name": "setRevokeToken","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [],"stateMutability": "nonpayable","type": "constructor"},{"stateMutability": "payable","type": "receive"},{"inputs": [{"internalType": "address","name": "","type": "address"}],"name": "botEOA","outputs": [{"internalType": "bool","name": "","type": "bool"}],"stateMutability": "view","type": "function"},{"inputs": [{"internalType": "uint8","name": "","type": "uint8"}],"name": "builderEOA","outputs": [{"internalType": "address","name": "","type": "address"}],"stateMutability": "view","type": "function"},{"inputs": [],"name": "NATIVE_TOKEN","outputs": [{"internalType": "address","name": "","type": "address"}],"stateMutability": "view","type": "function"},{"inputs": [],"name": "OKX_ROUTER","outputs": [{"internalType": "address","name": "","type": "address"}],"stateMutability": "view","type": "function"},{"inputs": [],"name": "OKX_TOKEN_APROVAL","outputs": [{"internalType": "address","name": "","type": "address"}],"stateMutability": "view","type": "function"},{"inputs": [],"name": "owner","outputs": [{"internalType": "address","name": "","type": "address"}],"stateMutability": "view","type": "function"}]
token_contract_abi = [{"inputs":[{"internalType":"address","name":"admin","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"AccessControlBadConfirmation","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"bytes32","name":"neededRole","type":"bytes32"}],"name":"AccessControlUnauthorizedAccount","type":"error"},{"inputs":[],"name":"ECDSAInvalidSignature","type":"error"},{"inputs":[{"internalType":"uint256","name":"length","type":"uint256"}],"name":"ECDSAInvalidSignatureLength","type":"error"},{"inputs":[{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"ECDSAInvalidSignatureS","type":"error"},{"inputs":[{"internalType":"uint256","name":"increasedSupply","type":"uint256"},{"internalType":"uint256","name":"cap","type":"uint256"}],"name":"ERC20ExceededCap","type":"error"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"allowance","type":"uint256"},{"internalType":"uint256","name":"needed","type":"uint256"}],"name":"ERC20InsufficientAllowance","type":"error"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"uint256","name":"balance","type":"uint256"},{"internalType":"uint256","name":"needed","type":"uint256"}],"name":"ERC20InsufficientBalance","type":"error"},{"inputs":[{"internalType":"address","name":"approver","type":"address"}],"name":"ERC20InvalidApprover","type":"error"},{"inputs":[{"internalType":"uint256","name":"cap","type":"uint256"}],"name":"ERC20InvalidCap","type":"error"},{"inputs":[{"internalType":"address","name":"receiver","type":"address"}],"name":"ERC20InvalidReceiver","type":"error"},{"inputs":[{"internalType":"address","name":"sender","type":"address"}],"name":"ERC20InvalidSender","type":"error"},{"inputs":[{"internalType":"address","name":"spender","type":"address"}],"name":"ERC20InvalidSpender","type":"error"},{"inputs":[{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"ERC2612ExpiredSignature","type":"error"},{"inputs":[{"internalType":"address","name":"signer","type":"address"},{"internalType":"address","name":"owner","type":"address"}],"name":"ERC2612InvalidSigner","type":"error"},{"inputs":[],"name":"EnforcedPause","type":"error"},{"inputs":[],"name":"ExpectedPause","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"uint256","name":"currentNonce","type":"uint256"}],"name":"InvalidAccountNonce","type":"error"},{"inputs":[],"name":"InvalidShortString","type":"error"},{"inputs":[{"internalType":"string","name":"str","type":"string"}],"name":"StringTooLong","type":"error"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"address","name":"spender","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[],"name":"EIP712DomainChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"account","type":"address"}],"name":"Paused","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"role","type":"bytes32"},{"indexed":True,"internalType":"bytes32","name":"previousAdminRole","type":"bytes32"},{"indexed":True,"internalType":"bytes32","name":"newAdminRole","type":"bytes32"}],"name":"RoleAdminChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"role","type":"bytes32"},{"indexed":True,"internalType":"address","name":"account","type":"address"},{"indexed":True,"internalType":"address","name":"sender","type":"address"}],"name":"RoleGranted","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"role","type":"bytes32"},{"indexed":True,"internalType":"address","name":"account","type":"address"},{"indexed":True,"internalType":"address","name":"sender","type":"address"}],"name":"RoleRevoked","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"account","type":"address"}],"name":"Unpaused","type":"event"},{"inputs":[],"name":"DEFAULT_ADMIN_ROLE","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MINTER_ROLE","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"burnFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"cap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"eip712Domain","outputs":[{"internalType":"bytes1","name":"fields","type":"bytes1"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"version","type":"string"},{"internalType":"uint256","name":"chainId","type":"uint256"},{"internalType":"address","name":"verifyingContract","type":"address"},{"internalType":"bytes32","name":"salt","type":"bytes32"},{"internalType":"uint256[]","name":"extensions","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"role","type":"bytes32"}],"name":"getRoleAdmin","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"role","type":"bytes32"},{"internalType":"address","name":"account","type":"address"}],"name":"grantRole","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"role","type":"bytes32"},{"internalType":"address","name":"account","type":"address"}],"name":"hasRole","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"mint","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"pause","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"paused","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"role","type":"bytes32"},{"internalType":"address","name":"callerConfirmation","type":"address"}],"name":"renounceRole","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"role","type":"bytes32"},{"internalType":"address","name":"account","type":"address"}],"name":"revokeRole","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"unpause","outputs":[],"stateMutability":"nonpayable","type":"function"}]

# 创建合约实例
sell_contract = None
token_contract = None

# 函数签名和 MethodID
swap_functions = {
    "0xe5e8894b": {
        "name": "proxySwapV2",
        "types": ["address", "uint256", "uint256", "uint256", "uint256", "bytes"],
        "param_names": ["router", "fromTokenWithFee", "fromAmt", "toTokenWithFee", "minReturnAmt", "callData"]
    },
    "0xa03de6a9": {
        "name": "callOneInch",
        "types": ["uint256", "uint256", "uint256", "bytes"],
        "param_names": ["fromTokenWithFee", "fromAmt", "toTokenWithFee", "callData"]
    }
}

# ANSI 颜色代码
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[36m',  # 青色
        'INFO': '\033[32m',  # 绿色
        'WARNING': '\033[33m',  # 黄色
        'ERROR': '\033[31m',  # 红色
        'CRITICAL': '\033[35m',  # 紫色
        'RESET': '\033[0m'
    }

    def format(self, record):
        levelname = record.levelname
        message = super().format(record)
        return f"{self.COLORS.get(levelname, '')}{message}{self.COLORS['RESET']}"


# 自定义 Filter，只允许 ERROR 级别的日志
class ErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelname == 'ERROR'


# 捕获 print 和 sys.stdout/stderr
class PrintToLogger:
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message.strip() and not message.startswith('2025-'):  # 避免重复记录已格式化的日志
            self.logger.log(self.level, message.rstrip())

    def flush(self):
        pass


# 配置根日志记录器，捕获所有日志（包括第三方库）
logging.getLogger('').setLevel(logging.INFO)  # 设置根日志级别为 DEBUG
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 1. 配置按天切割的日志文件（所有日志）
log_file = os.path.join(log_dir, f'{token_name}.log')  # 日志文件路径改为 logs/app.log
all_handler = TimedRotatingFileHandler(
    log_file,
    when="midnight",
    interval=1,
    backupCount=7
)
all_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logging.getLogger('').addHandler(all_handler)

# 2. 配置单独的 ERROR 日志文件
error_log_file = os.path.join(log_dir, f'{token_name}.error.log')  # 日志文件路径改为 logs/error.log
error_handler = TimedRotatingFileHandler(
    error_log_file,
    when="midnight",
    interval=1,
    backupCount=7
)
error_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
error_handler.addFilter(ErrorFilter())
logging.getLogger('').addHandler(error_handler)

# 3. 配置控制台输出（带颜色）
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColoredFormatter('%(asctime)s %(levelname)s: %(message)s'))
logging.getLogger('').addHandler(console_handler)

# 4. 重定向 print 到 logging
sys.stdout = PrintToLogger(logging.getLogger(''), logging.INFO)

# 5. 重定向 stderr（捕获第三方库的 stderr 输出）
sys.stderr = PrintToLogger(logging.getLogger(''), logging.ERROR)


async def init_pool(db_config):
    global pool
    pool = await aiomysql.create_pool(**db_config)
    logger.info(f"init db success")


async def close_pool():
    global pool
    if pool is not None:
        pool.close()
        await pool.wait_closed()


target_wallet_addr_list: List[str] = []

# config.json文件更新到本地变量中
def update_wallet_addresses(token: str, config_file: str = "config.json", interval: int = 5):
    def update_config_with_approved_wallets(db_conn, config_file='config.json'):
        try:
            with db_conn.cursor() as cursor:
                sql = """
                    SELECT user_addr 
                    FROM tb_bot_approve 
                    WHERE token_addr = %s AND approve_amount > 0
                """
                cursor.execute(sql, (token_contract_address,))
                results = cursor.fetchall()

                # 提取 wallet_addr 列表
                wallet_addresses = [row[0] for row in results]

                # 读取现有 config.json
                config_path = Path(config_file)
                if config_path.exists():
                    with config_path.open('r', encoding='utf-8') as f:
                        config = json.load(f)
                else:
                    config = {"tokens": {}}

                # 更新 config 中的 wallet_addresses
                config["tokens"][token] = {"wallet_addresses": wallet_addresses}

                # 写入 config.json
                with config_path.open('w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)

                # logger.info(f"Updated config.json with {len(wallet_addresses)} wallet addresses for token {token}")
        except Exception as e:
            logger.error(f"Error updating config: {e}")
            raise e
        finally:
            db_conn.close()

    def load_wallet_addresses(token: str, config_file: str = "config.json") -> List[str]:

        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                tokens = config.get("tokens", {})
                if token not in tokens:
                    logger.error(f"Token {token} not found in config file")
                    return []
                addresses = tokens[token].get("wallet_addresses", [])
                return addresses
        except FileNotFoundError:
            logger.error(f"Config file {config_file} not found")
            return []
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON format in {config_file}")
            return []
        except Exception as e:
            logger.error(f"Error reading config file: {e}")
            return []

    global target_wallet_addr_list
    while True:
        try:
            db_conn = pymysql.connect(**db_config)
            update_config_with_approved_wallets(db_conn, config_file)
            target_wallet_addr_list = load_wallet_addresses(token, config_file)
            #logger.info(f"当前监控的钱包地址:{target_wallet_addr_list}")
        except Exception as e:
            logger.error(f"Error in update thread for {token}: {e}")
        time.sleep(interval)

try:
    with connection.cursor() as cursor:
        # api_key
        sql = f"SELECT api_key, secret_key, passphrase FROM tb_okx_api_key where bot_id = {bot_id} and token = '{token_name}' "
        cursor.execute(sql)

        result = cursor.fetchone()

        api_config['api_key'] = result[0]
        api_config['secret_key'] = result[1]
        api_config['passphrase'] = result[2]
        logger.info(f"api_config:{api_config}")

        # sell contract info
        sql = f"SELECT contract_addr from tb_contract_info where token_name = 'sell_contract' "
        cursor.execute(sql)

        result = cursor.fetchone()

        sell_contract_address = result[0]
        logger.info(f"sell_contract_addr:{sell_contract_address}")

        # contract info
        sql = f"SELECT contract_addr from tb_contract_info where token_name = '{token_name}' "
        cursor.execute(sql)

        result = cursor.fetchone()

        token_contract_address = result[0]
        logger.info(f"token_name:{token_name} token_addr:{token_contract_address}")


except Exception as e:
    logger.error(f"init api_key or contract info error: {e}")
    exit(0)

finally:
    connection.close()


async def log_handler(
        handler_context: LogsSubscriptionContext
) -> None:
    try:
        # reference the w3 instance
        w3 = handler_context.async_w3

        log = handler_context.result

        #logger.info(f"{log}")

        # 买入代币事件
        to_addr = '0x' + log['topics'][2].hex()[-40:]

        if to_addr.lower() in [addr.lower() for addr in target_wallet_addr_list]:
            amount = w3.to_int(log['data'])
            logger.info(
                f"[producer] wallet:{to_addr} buy {token_name} amount:{amount} hash:{log['transactionHash'].hex()}")
            await add_to_queue(log['transactionHash'].hex() + ',' + str(amount))
    except Exception as e:
        logger.error(f"[producer] unknown error:{e}")


async def acquire_wallet(consumer_name, base_hash, max_retries=20, retry_delay=0.1):
    global pool
    if pool is None:
        raise Exception("Connection pool not initialized")

    async with pool.acquire() as connection:
        for attempt in range(max_retries):
            try:
                async with connection.cursor() as cursor:
                    # 开启事务
                    await connection.begin()

                    sql_select = """
                        SELECT wallet_addr, private_key 
                        FROM tb_bot_wallet 
                        WHERE `using` = 0 
                        LIMIT 1 FOR UPDATE
                    """
                    await cursor.execute(sql_select)
                    result = await cursor.fetchone()

                    if not result:
                        raise Exception("No available wallet")

                    wallet_addr, private_key = result

                    sql_update = """
                        UPDATE tb_bot_wallet 
                        SET `using` = 1
                        WHERE wallet_addr = %s
                    """
                    await cursor.execute(sql_update, (wallet_addr,))
                    await connection.commit()

                    return wallet_addr, private_key

            except aiomysql.OperationalError as e:
                await connection.rollback()
                if "Deadlock" in str(e) and attempt < max_retries - 1:
                    logger.warning(
                        f"[{consumer_name}] base_hash:{base_hash} Deadlock detected, retrying {attempt + 1}/{max_retries}")
                    await asyncio.sleep(retry_delay)
                    continue
                raise e
            except Exception as e:
                await connection.rollback()
                raise e

    raise Exception(f"[{consumer_name}] base_hash:{base_hash} Max retries reached, unable to acquire wallet")


async def release_wallet(wallet_addr, consumer_name, base_hash):
    global pool
    if pool is None:
        raise Exception("Connection pool not initialized")

    async with pool.acquire() as connection:
        try:
            async with connection.cursor() as cursor:
                # 释放钱包
                sql = """
                    UPDATE tb_bot_wallet 
                    SET `using` = 0
                    WHERE wallet_addr = %s
                """
                await cursor.execute(sql, (wallet_addr,))
                await connection.commit()

        except Exception as e:
            logger.error(f"[{consumer_name}] base_hash:{base_hash} Error releasing wallet: {e}")
            raise e


# 异步生产者：向队列添加数据
async def add_to_queue(tx_hash: str, type=0):
    try:
        if type == 0:
            await task_queue.put(tx_hash)  # 异步添加数据
            logger.info(f"[producer] add to sell queue: {tx_hash}")
    except asyncio.QueueFull:
        logger.error(f"[producer] sell queue was full")
    except Exception as e:
        logger.error(f"[producer] add to queue unknown error:{e}")


async def sub_manager(w3):
    logger.info(f"[producer] running...")

    global sell_contract, token_contract
    if sell_contract is None:
        sell_contract = w3.eth.contract(address=w3.to_checksum_address(sell_contract_address), abi=sell_contract_abi)
    if token_contract is None:
        token_contract = w3.eth.contract(address=w3.to_checksum_address(token_contract_address), abi=token_contract_abi)

    # -- subscribe to event(s) --
    await w3.subscription_manager.subscribe(
        [
            LogsSubscription(
                label=f"{token_name} transfers",  # optional label
                address=w3.to_checksum_address(token_contract_address),
                topics=[token_contract.events.Transfer().topic],
                handler=log_handler,
                # optional `handler_context` args to help parse a response
                handler_context={"transfer_event": token_contract.events.Transfer()},
            ),
        ]
    )
    # -- listen for events --
    await w3.subscription_manager.handle_subscriptions()


async def parse_address_and_fee(token_with_fee: int) -> str:
    token = hex(token_with_fee & ((1 << 160) - 1))[2:].zfill(40)
    token = f"0x{token}"
    return token


async def request_okx_calldata(w3, params, consumer_name, base_hash=None):
    try:
        get_request_path = "/api/v5/dex/aggregator/swap"
        get_params = {
            "chainId": 56,
            "amount": str(params['amount']),
            "toTokenAddress": params['from_token_addr'],
            "fromTokenAddress": params['to_token_addr'],
            "slippage": "0.001",
            "userWalletAddress": w3.to_checksum_address(sell_contract_address),
            "feePercent": "0.001",
            "toTokenReferrerWalletAddress": toTokenReferrerWalletAddress,
            "swapReceiverAddress": w3.to_checksum_address(sell_contract_address),
        }
        response_json = await send_get_request(get_request_path, get_params)
        # logger.info(f"[consumer] {params['from']} okx 返回结果:{response_json}")
        call_data = response_json['data'][0]['tx']['data']

        to_token_amount = response_json['data'][0]['routerResult']['toTokenAmount']
        from_amt = params['fromAmt']

        # 最大允许滑点
        if w3.to_int(text=to_token_amount) <= int(from_amt * 0.995):
            logger.error(
                f"[{consumer_name}] base_hash:{base_hash} request okx SLIPPAGE not allowed! params:{params} okx response:{response_json}")
            return None

        # 将字符串转换为 bytes
        if call_data.startswith("0x"):
            call_data_bytes = w3.to_bytes(hexstr=call_data[2:])
        else:
            call_data_bytes = w3.to_bytes(hexstr=call_data)
        return call_data_bytes
    except Exception as e:
        logger.error(f"[{consumer_name}] base_hash:{base_hash} request okx failed: {e}")
        return None


# MEV 贿赂: 0-bnb48, 1-blockrazor, 255 不贿赂
async def create_mev_transaction(w3, wallet_addr, private_key, nonce, params, consumer_name, builder_eoa, call_data,
                                 base_hash=None):
    """创建并签名一笔交易"""
    try:
        # 构建交易
        txn = await sell_contract.functions.call(
            w3.to_checksum_address(params['from']),
            w3.to_checksum_address(params['to_token_addr']),
            w3.to_checksum_address(params['from_token_addr']),
            params['amount'],
            builder_eoa,
            call_data
        ).build_transaction({
            'from': w3.to_checksum_address(wallet_addr),
            'gas': GAS_LIMIT,  # 调整gas limit
            'gasPrice': MEV_GAS_PRICE,
            'value': BRIBE_AMOUNT,
            'nonce': nonce,
            'chainId': CHAINID  # BSC主网chainId
        })

        # 签名交易
        signed_tx = w3.eth.account.sign_transaction(txn, private_key)
        return signed_tx, call_data
    except Exception as e:
        logger.error(f"[{consumer_name}] base_hash:{base_hash} create or sign transaction failed: {e}")
        return None


# 解析交易的 input 数据
async def decode_from_token_with_fee(tx, consumer_name, base_hash=None):
    try:
        input_data = tx["input"]

        # 检查 MethodID
        method_id = '0x' + input_data.hex().lower()[:8]
        if method_id not in swap_functions:
            return None

        # 获取函数定义
        func = swap_functions[method_id]
        func_name = func["name"]
        param_types = func["types"]
        param_names = func["param_names"]

        # 解码 input 数据（跳过 MethodID 的前 4 字节）
        decoded_params = decode(param_types, input_data[4:])
        # 构建参数字典
        params = {param_names[i]: decoded_params[i] for i in range(len(param_names))}

        return params
    except Exception as e:
        logger.error(f"[{consumer_name}] base_hash:{base_hash} decode base tx failed: {e}")


async def send_get_request(request_path, params):
    async def pre_hash(timestamp, method, request_path, params):
        """生成预签名消息"""
        query_string = ""
        if method == "GET" and params:
            query_string = "?" + urlencode(params)
        elif method == "POST" and params:
            query_string = json.dumps(params)
        return f"{timestamp}{method}{request_path}{query_string}"

    async def sign(message, secret_key):
        """使用HMAC-SHA256进行签名"""
        secret_bytes = secret_key.encode('utf-8')
        message_bytes = message.encode('utf-8')
        hmac_obj = hmac.new(secret_bytes, message_bytes, hashlib.sha256)
        return base64.b64encode(hmac_obj.digest()).decode('utf-8')

    async def create_signature(method, request_path, params):
        """创建签名和时间戳"""
        now = datetime.utcnow()
        iso_str = now.isoformat(timespec='milliseconds') + 'Z'
        timestamp = iso_str[:-5] + 'Z'
        message = await pre_hash(timestamp, method, request_path, params)
        signature = await sign(message, api_config["secret_key"])
        return {"signature": signature, "timestamp": timestamp}

    """发送异步GET请求"""
    sig_info = await create_signature("GET", request_path, params)

    # 构造请求头
    headers = {
        "OK-ACCESS-KEY": api_config["api_key"],
        "OK-ACCESS-SIGN": sig_info["signature"],
        "OK-ACCESS-TIMESTAMP": sig_info["timestamp"],
        "OK-ACCESS-PASSPHRASE": api_config["passphrase"],
    }

    # 使用 aiohttp 发送异步请求
    url = f"https://web3.okx.com{request_path}"
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get(url, headers=headers, params=params) as response:
            response_json = await response.json()
            return response_json


async def deal_consumer_task(w3, queue_data):
    base_hash = None
    wallet_addr = None

    max_attempts = 3
    attempt = 1

    while attempt <= max_attempts:
        try:
            base_hash = queue_data.split(',')[0]
            amount = queue_data.split(',')[1]

            wallet_addr, private_key = await acquire_wallet("consumer", base_hash)
            logger.info(f"[consumer] base_hash:{base_hash} Acquired wallet: {wallet_addr}")

            tx = await w3.eth.get_transaction(base_hash)
            params = await decode_from_token_with_fee(tx, "consumer", base_hash)

            if params:

                from_token_addr = await parse_address_and_fee(w3.to_int(params['fromTokenWithFee']))
                to_token_addr = await parse_address_and_fee(w3.to_int(params['toTokenWithFee']))
                params['from_token_addr'] = from_token_addr
                params['to_token_addr'] = to_token_addr
                params['from'] = tx['from']
                params['amount'] = w3.to_int(text=amount)

                if int(params['from_token_addr'], 16) == 0:
                    params['from_token_addr'] = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                if int(params['to_token_addr'], 16) == 0:
                    params['to_token_addr'] = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

                # logger.info(f"[consumer] {params['from']} traction params:{params}")

                # 创建并签名自己的交易
                nonce = await w3.eth.get_transaction_count(w3.to_checksum_address(wallet_addr))
                okx_calldata = await request_okx_calldata(w3, params, "consumer", base_hash)

                mev_48_tx, _ = await create_mev_transaction(w3,
                                                            wallet_addr,
                                                            private_key,
                                                            nonce, params,
                                                            "consumer",
                                                            0,
                                                            okx_calldata,
                                                            base_hash)

                mev_blockrazor_tx, _ = await create_mev_transaction(w3,
                                                                    wallet_addr,
                                                                    private_key,
                                                                    nonce,
                                                                    params,
                                                                    "consumer",
                                                                    1,
                                                                    okx_calldata,
                                                                    base_hash)

                if mev_48_tx and mev_blockrazor_tx and okx_calldata:
                    # 发送签名交易

                    mev_48_tx_hash = '0x' + keccak(mev_48_tx.raw_transaction).hex()
                    mev_blockrazor_tx_hash = '0x' + keccak(mev_blockrazor_tx.raw_transaction).hex()

                    # 异步发送到 mev节点
                    if mev_48_tx:
                        asyncio.create_task(send_48_bundle('0x' + mev_48_tx.raw_transaction.hex(),
                                                           "consumer",
                                                           base_hash))
                    if mev_blockrazor_tx:
                        asyncio.create_task(send_blockrazor_bundle('0x' + mev_blockrazor_tx.raw_transaction.hex(),
                                                                   "consumer",
                                                                   base_hash))

                    wait_tasks = {
                        'mev_48_custom': asyncio.create_task(
                            w3.eth.wait_for_transaction_receipt(mev_48_tx_hash, timeout=15)),
                        'mev_blockrazor_custom': asyncio.create_task(
                            w3.eth.wait_for_transaction_receipt(mev_blockrazor_tx_hash, timeout=15))
                    }

                    done_tasks, pending_tasks = await asyncio.wait(wait_tasks.values(),
                                                                   return_when=asyncio.FIRST_COMPLETED)

                    # 取消未完成的任务
                    for task in pending_tasks:
                        task.cancel()

                    for done_task in done_tasks:
                        receipt = done_task.result()
                        logger.info(
                            f"[consumer] base_hash:{base_hash} sell transaction is: {receipt['transactionHash'].hex()}")
                        break
                else:
                    if attempt >= max_attempts:
                        error_msg = f"[consumer] base_hash:{base_hash} cannot sign or create transaction"
                        logger.error(error_msg)
                        raise Exception(error_msg)
                    raise Exception(f"cannot sign or create transaction，")
            return
        except Exception as e:
            if attempt >= max_attempts:
                logger.error(f"[consumer] base_hash:{base_hash} consumer unknown error: {e}")
            attempt += 1
            await release_wallet(wallet_addr, "consumer", base_hash)
        finally:
            if wallet_addr:
                await asyncio.sleep(2)
                await release_wallet(wallet_addr, "consumer", base_hash)
                logger.info(f"[consumer] base_hash:{base_hash} Released wallet: {wallet_addr}")


# 异步消费者：从队列中拉取数据并处理
async def consumer(w3):
    logger.info("[consumer] running...")
    global task_queue

    while True:
        try:
            # 异步获取交易哈希，超时 1 秒
            queue_data = await asyncio.wait_for(task_queue.get(), timeout=1.0)
            logger.info(f"[consumer] doing: {queue_data}")

            # 启动处理任务
            asyncio.create_task(deal_consumer_task(w3, queue_data))

        except asyncio.TimeoutError:
            # 队列为空时继续循环
            await asyncio.sleep(0.01)
        except Exception as e:
            logger.error(f"[consumer] unknown error: {e}")


async def send_48_bundle(raw_tx: str, consumer_name, base_hash=None) -> None:
    PUISSANT_URL = "https://puissant-builder.48.club/"

    max_attempts = 3
    attempt = 1

    while attempt <= max_attempts:
        try:
            # 构造 bundle
            bundle = {
                "txs": [raw_tx],
            }

            # JSON-RPC 请求
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_sendBundle",
                "params": [bundle]
            }

            # logger.info(f"payload:{json.dumps(payload)}")

            # 发送 POST 请求到 Puissant Builder
            headers = {'Content-Type': 'application/json'}
            async with aiohttp.ClientSession() as session:
                async with session.post(PUISSANT_URL, json=payload, headers=headers) as response:
                    response_data = await response.json()
                    # logger.info(f"48 Bundle 发送响应: {response_data}")

                    # 检查响应
                    if response.status == 200 and 'result' in response_data:
                        # logger.info(f"Bundle 发送成功，结果: {response_data['result']}")
                        return response_data['result']
                    else:
                        if attempt >= max_attempts:
                            logger.error(
                                f"[{consumer_name}] base_hash:{base_hash} 48Bundle send failed after {max_attempts} attempts: {response_data}")
                            raise Exception(f"Failed response: {response_data}")
                        raise Exception(f"Attempt {attempt} failed: {response_data}")
        except Exception as e:
            if attempt >= max_attempts:
                logger.error(
                    f"[{consumer_name}] base_hash:{base_hash} send 48 bundle failed after {max_attempts} attempts: {e}")
            attempt += 1
            await asyncio.sleep(0.1)


async def send_blockrazor_bundle(raw_tx: str, consumer_name, base_hash=None) -> None:
    BUILDER_URL = "https://rpc.blockrazor.builders/"
    max_attempts = 3
    attempt = 1

    while attempt <= max_attempts:
        try:
            # 构造 bundle
            bundle = {
                "txs": [raw_tx],
            }

            # JSON-RPC 请求
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_sendBundle",
                "params": [bundle]
            }

            # logger.info(f"payload:{json.dumps(payload)}")

            # 发送 POST 请求到 Puissant Builder
            headers = {
                'Content-Type': 'application/json',
                'Authorization': blockrazor_auth
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(BUILDER_URL, json=payload, headers=headers) as response:
                    response_data = await response.json()
                    # logger.info(f"48 Bundle 发送响应: {response_data}")

                    # 检查响应
                    if response.status == 200 and 'result' in response_data:
                        # logger.info(f"Bundle 发送成功，结果: {response_data['result']}")
                        return response_data['result']
                    else:
                        if attempt >= max_attempts:
                            logger.error(
                                f"[{consumer_name}] base_hash:{base_hash} blockrazor send failed after {max_attempts} attempts: {response_data}")
                            raise Exception(f"Failed response: {response_data}")
                        raise Exception(f"Attempt {attempt} failed: {response_data}")
        except Exception as e:
            if attempt >= max_attempts:
                logger.error(
                    f"[{consumer_name}] base_hash:{base_hash} send blockrazor bundle failed after {max_attempts} attempts: {e}")
            attempt += 1
            await asyncio.sleep(0.1)


async def main():
    # 初始化连接池
    await init_pool(db_config)

    # -- initialize provider --
    w3 = await AsyncWeb3(WebSocketProvider(rpc_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    # 启动消费者任务
    consumer_task = asyncio.create_task(consumer(w3))

    # 启动订阅者任务
    sub_task = asyncio.create_task(sub_manager(w3))

    try:
        await asyncio.gather(sub_task, consumer_task, return_exceptions=True)
    except asyncio.CancelledError:
        logger.info(f"all task canceled!")
    except KeyboardInterrupt:
        logger.info("trying cancel task...")
        sub_task.cancel()
        consumer_task.cancel()
        try:
            await asyncio.gather(sub_task, consumer_task, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("all task canceled!")
    finally:
        # 确保 WebSocket 连接关闭
        if await w3.provider.is_connected():
            await w3.provider.disconnect()
        # 关闭连接池
        await close_pool()


if __name__ == "__main__":
    thread = threading.Thread(target=update_wallet_addresses, args=(token_name, 'config.json', 5), daemon=True)
    thread.start()
    asyncio.run(main())