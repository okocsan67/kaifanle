from web3 import AsyncWeb3
from web3 import WebSocketProvider
import asyncio
from web3.middleware import ExtraDataToPOAMiddleware
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
from web3.utils.subscriptions import (
    LogsSubscription,LogsSubscriptionContext
)
from datetime import datetime
import aiomysql
import socket

# 确保 logs/ 目录存在
log_dir = os.path.join(os.getcwd(), 'logs')
os.makedirs(log_dir, exist_ok=True)  # 如果目录不存在则创建

# 配置参数
RPC_URL = "wss://bsc-rpc.publicnode.com"
TOKENS = [
    {"token_name":"b2", "address":"0x783c3f003f172c6Ac5AC700218a357d2D66Ee2a2"},
    {"token_name":"zkj", "address":"0xc71b5f631354be6853efe9c3ab6b9590f8302e81"},
    {"token_name":"koge", "address":"0xe6DF05CE8C8301223373CF5B969AFCb1498c5528"}
] #需要监听哪些token


#数据库配置
db_config = {
    'host': 'xx.xx.xx.xx',
    'user': 'root',
    'port': 3306,
    'password': '',
    'db': 'alphabot',
    'charset': 'utf8mb4'
}

TARGET_SPENDER = "" #填写自己的合约 用来监听哪个代币给此合约授权了

#######################以下配置没有特殊需求不用改############################


# 全局连接池
pool = None

# ANSI 颜色代码
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[36m',  # 青色
        'INFO': '\033[32m',   # 绿色
        'WARNING': '\033[33m',# 黄色
        'ERROR': '\033[31m',  # 红色
        'CRITICAL': '\033[35m', # 紫色
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
log_file = os.path.join(log_dir, f'token_approve.log')  # 日志文件路径改为 logs/app.log
all_handler = TimedRotatingFileHandler(
    log_file,
    when="midnight",
    interval=1,
    backupCount=7
)
all_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logging.getLogger('').addHandler(all_handler)

# 2. 配置单独的 ERROR 日志文件
error_log_file = os.path.join(log_dir, f'token_approve.error.log')  # 日志文件路径改为 logs/error.log
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


# ERC20 ABI
ERC20_ABI = [{
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "owner", "type": "address"},
        {"indexed": True, "name": "spender", "type": "address"},
        {"indexed": False, "name": "value", "type": "uint256"}
    ],
    "name": "Approval",
    "type": "event"
}]


async def init_pool():
    global pool
    pool = await aiomysql.create_pool(**db_config)
    logger.info(f"init db success")

async def close_pool():
    global pool
    if pool is not None:
        pool.close()
        await pool.wait_closed()

async def upsert_approval(user_addr: str, token_addr: str, approve_amount: str):

    global pool
    if pool is None:
        raise Exception("Connection pool not initialized")

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 检查记录是否存在
            select_query = """
                SELECT id FROM `alphabot`.`tb_bot_approve`
                WHERE user_addr = %s AND token_addr = %s
            """
            await cursor.execute(select_query, (user_addr, token_addr))
            result = await cursor.fetchone()

            if result:
                # 更新现有记录
                update_query = """
                    UPDATE `alphabot`.`tb_bot_approve`
                    SET approve_amount = %s, update_time = %s
                    WHERE user_addr = %s AND token_addr = %s
                """
                await cursor.execute(update_query, (approve_amount, current_time, user_addr, token_addr))
            else:
                # 插入新记录
                insert_query = """
                    INSERT INTO `alphabot`.`tb_bot_approve` (
                        user_addr, token_addr, approve_amount, create_time, update_time
                    ) VALUES (%s, %s, %s, %s, %s)
                """
                await cursor.execute(insert_query, (user_addr, token_addr, approve_amount, current_time, current_time))

            await conn.commit()

async def approve_log_handler(
    handler_context: LogsSubscriptionContext
) -> None:

    try:
        # reference the w3 instance
        #w3 = handler_context.async_w3

        log = handler_context.result

        #logger.info(f"{log}")

        #授权事件

        spender_addr = '0x' + log['topics'][2].hex()[-40:]

        if spender_addr.lower() in TARGET_SPENDER.lower():
            owner_addr = '0x' + log['topics'][1].hex()[-40:]
            approve_amount = int.from_bytes(log['data'], byteorder='big')
            token_address = log['address']

            if approve_amount == 0:
                logger.info(f"user cancel_approve:{owner_addr} hash:{log['transactionHash'].hex()}")
                await upsert_approval(user_addr=owner_addr,
                                      token_addr=token_address,
                                      approve_amount=str(approve_amount))
            else:
                logger.info(f"user add_approve:{owner_addr} hash:{log['transactionHash'].hex()}")
                await upsert_approval(user_addr=owner_addr,
                                      token_addr=token_address,
                                      approve_amount=str(approve_amount))
    except Exception as e:
        logger.error(f"[producer] unknown error:{e}")


async def main():
    logger.info("[token-approve] running...")
    await init_pool()

    w3 = None
    while True:  # 重连循环
        try:
            w3 = await AsyncWeb3(WebSocketProvider(RPC_URL))
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

            # 验证连接
            if not await w3.is_connected():
                raise ConnectionError("无法连接RPC节点")

            subscriptions = [
                LogsSubscription(
                    label=f"{token['token_name']} Approval event",
                    address=w3.to_checksum_address(token['address']),
                    topics=[w3.eth.contract(
                        address=w3.to_checksum_address(token['address']),
                        abi=ERC20_ABI
                    ).events.Approval().topic],
                    handler=approve_log_handler,
                    handler_context={
                        "approve_event": w3.eth.contract(
                            address=w3.to_checksum_address(token['address']),
                            abi=ERC20_ABI
                        ).events.Approval()
                    },
                )
                for token in TOKENS
            ]

            await w3.subscription_manager.subscribe(subscriptions)
            logger.info("订阅已启动，监听事件...")
            await w3.subscription_manager.handle_subscriptions()

        except ConnectionError as e:
            logger.error(f"连接错误: {3}")
            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"未知错误: {e}")
            await asyncio.sleep(3)
        finally:
            try:
                if 'w3' in locals() and w3.is_connected():
                    await w3.close()
                    logger.info("WebSocket连接已关闭")
                # 关闭连接池
                await close_pool()
            except:
                pass

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("监控已手动停止")
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
    except Exception as e:
        logger.error(f"主程序错误: {e}")
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()