"""
Binance U 本位合约数据收集器
输出格式与 hftbacktest 官方 Rust collector 完全兼容：
  每行：{纳秒时间戳} {原始 JSON}\n
  文件：{path}/{symbol_小写}_{YYYYMMDD}.gz，按天自动滚动

订阅的数据流：
  - {symbol}@trade        逐笔成交
  - {symbol}@bookTicker   最优买卖盘
  - {symbol}@depth@0ms    增量深度更新（最快频率）

当检测到深度序列号断层时，自动拉取 REST 快照并注入数据流，
确保后续 hftbacktest 能正确重建订单簿。
"""

import asyncio
import gzip
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── 配置 ──────────────────────────────────────────────────────────────────────

SYMBOLS = ["BTCUSDT", "ETHUSDT"]  # 需要收集的合约

# 输出目录，按需修改（使用绝对路径或相对路径均可）
OUTPUT_DIR = "./data"

# Binance USDM Futures 端点
WS_BASE    = "wss://fstream.binance.com/stream?streams="
DEPTH_URL  = "https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"

# 深度快照限流：官方限制约 100 次/分钟，此处保守设为 80 次/分钟
SNAPSHOT_INTERVAL = 60 / 80  # 秒

# ── 文件写入（按天滚动，gzip 压缩）──────────────────────────────────────────

class RotatingGzipWriter:
    """
    与 Rust 版 RotatingFile 行为一致：
    - 文件名格式：{path}/{symbol}_{YYYYMMDD}.gz
    - 每行格式：{纳秒时间戳} {原始 JSON 字符串}\n
    - 日期变化时自动关闭旧文件，开新文件
    """

    def __init__(self, base_dir: str, symbol: str):
        self.base_dir = Path(base_dir)
        self.symbol   = symbol.lower()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: str = ""
        self._file = None

    def _open(self, date_str: str):
        """打开新的日期文件"""
        if self._file:
            self._file.close()
        path = self.base_dir / f"{self.symbol}_{date_str}.gz"
        self._file = gzip.open(path, "at", encoding="utf-8")
        self._current_date = date_str
        logger.info(f"已开启新文件：{path}")

    def write(self, recv_ns: int, raw_json: str):
        """写入一条记录，recv_ns 为纳秒级 UTC 时间戳"""
        date_str = datetime.fromtimestamp(recv_ns / 1e9, tz=timezone.utc).strftime("%Y%m%d")
        if date_str != self._current_date:
            self._open(date_str)
        # 格式与 Rust 版完全一致："{纳秒时间戳} {JSON}\n"
        self._file.write(f"{recv_ns} {raw_json}\n")

    def flush(self):
        if self._file:
            self._file.flush()

    def close(self):
        if self._file:
            self._file.close()
            self._file = None


# ── REST 快照拉取 ─────────────────────────────────────────────────────────────

async def fetch_depth_snapshot(session: aiohttp.ClientSession, symbol: str) -> str | None:
    """
    拉取当前深度快照（1000档），返回原始 JSON 字符串。
    快照会被当作普通事件写入文件，hftbacktest 的 Data Preparation
    脚本能识别快照格式（有 lastUpdateId 字段）并正确处理。
    """
    url = DEPTH_URL.format(symbol=symbol.upper())
    try:
        async with session.get(url) as resp:
            text = await resp.text()
            return text
    except Exception as e:
        logger.error(f"拉取 {symbol} 快照失败：{e}")
        return None


# ── 主收集循环 ────────────────────────────────────────────────────────────────

async def collect(symbols: list[str], output_dir: str):
    """
    连接 WebSocket，持续收集数据并写入文件。
    自动处理：
      - Ping/Pong 保活
      - 断线重连（指数退避）
      - 深度序列号断层检测 → 自动拉取快照补全
    """
    # 每个 symbol 初始化一个写入器
    writers = {sym.lower(): RotatingGzipWriter(output_dir, sym) for sym in symbols}

    # 记录每个 symbol 最后一次 depthUpdate 的 u（finalUpdateId），用于断层检测
    prev_u: dict[str, int] = {}

    # 限流：防止快照请求过于频繁
    last_snapshot_time: dict[str, float] = {}

    # 订阅的数据流列表
    streams = []
    for sym in symbols:
        s = sym.lower()
        streams += [
            f"{s}@trade",       # 逐笔成交
            f"{s}@bookTicker",  # 最优买卖盘
            f"{s}@depth@0ms",   # 增量深度（最快频率）
        ]
    ws_url = WS_BASE + "/".join(streams)

    error_count = 0

    async with aiohttp.ClientSession() as http_session:
        while True:
            try:
                logger.info(f"正在连接：{ws_url[:80]}...")
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,   # 每 20s 发一次 ping 保活
                    ping_timeout=60,    # 60s 未收到 pong 则断开
                    max_size=10 * 1024 * 1024,  # 最大消息 10MB
                ) as ws:
                    logger.info("WebSocket 已连接")
                    error_count = 0

                    async for raw_msg in ws:
                        # 记录收到消息的纳秒时间戳（与 Rust 版一致）
                        recv_ns = time.time_ns()

                        # 解析外层结构，获取 symbol 和事件类型
                        try:
                            msg = json.loads(raw_msg)
                        except json.JSONDecodeError:
                            continue

                        data = msg.get("data", {})
                        symbol = data.get("s", "").lower()
                        event  = data.get("e", "")

                        if not symbol or symbol not in writers:
                            continue

                        # ── 深度断层检测 ──────────────────────────────────────
                        if event == "depthUpdate":
                            u  = data.get("u")   # 本次 finalUpdateId
                            pu = data.get("pu")  # 上次 finalUpdateId（应与我们记录的一致）

                            expected = prev_u.get(symbol)
                            if expected is not None and pu != expected:
                                # 序列号对不上，说明有数据丢失，需要拉快照重建订单簿
                                logger.warning(
                                    f"{symbol} 深度断层：期望 pu={expected}，实际 pu={pu}，拉取快照..."
                                )
                                now = time.time()
                                if now - last_snapshot_time.get(symbol, 0) >= SNAPSHOT_INTERVAL:
                                    last_snapshot_time[symbol] = now
                                    snap = await fetch_depth_snapshot(http_session, symbol)
                                    if snap:
                                        snap_ns = time.time_ns()
                                        writers[symbol].write(snap_ns, snap)

                            if u is not None:
                                prev_u[symbol] = u  # 更新最新的 finalUpdateId

                        # ── 写入文件 ──────────────────────────────────────────
                        writers[symbol].write(recv_ns, raw_msg)

                        # 每 1000 条刷一次（避免数据积压在内存缓冲区）
                        if recv_ns % 1000 == 0:
                            writers[symbol].flush()

            except (websockets.ConnectionClosed, ConnectionError) as e:
                error_count += 1
                logger.warning(f"连接断开（第 {error_count} 次）：{e}")
            except Exception as e:
                error_count += 1
                logger.error(f"未知错误（第 {error_count} 次）：{e}")
            finally:
                # 刷新所有文件缓冲
                for w in writers.values():
                    w.flush()

            # 指数退避重连
            if error_count <= 3:
                delay = 1
            elif error_count <= 10:
                delay = 3
            else:
                delay = 10
            logger.info(f"{delay}s 后重连...")
            await asyncio.sleep(delay)


# ── 入口 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info(f"开始收集：{SYMBOLS}，输出目录：{OUTPUT_DIR}")
    try:
        asyncio.run(collect(SYMBOLS, OUTPUT_DIR))
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，正常退出")
