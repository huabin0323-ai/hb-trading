"""行情采集模块 — Binance WebSocket 实时数据 + SQLite 存储"""

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from websocket import WebSocketApp, WebSocketConnectionClosedException

from config import DATA_DIR, SYMBOLS, TIMEFRAMES, LOOKBACK_CANDLES

logger = logging.getLogger(__name__)

DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "market.db"

# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    """建表，幂等"""
    conn = _get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS klines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            close_time INTEGER NOT NULL,
            UNIQUE(symbol, timeframe, open_time)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_sym_tf ON klines(symbol, timeframe)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_open_time ON klines(open_time)")
    conn.commit()
    conn.close()


def insert_klines(symbol: str, timeframe: str, candles: list[dict]) -> int:
    """批量插入K线，跳过重复。返回实际插入条数"""
    conn = _get_conn()
    count = 0
    for c in candles:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO klines
                   (symbol, timeframe, open_time, open, high, low, close, volume, close_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    symbol, timeframe,
                    c["open_time"], c["open"], c["high"], c["low"],
                    c["close"], c["volume"], c["close_time"],
                ),
            )
            if conn.total_changes > count:
                count += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return count


def get_klines(
    symbol: str, timeframe: str, limit: int = 500, since: Optional[int] = None
) -> pd.DataFrame:
    """查询K线，返回DataFrame"""
    conn = _get_conn()
    if since:
        rows = conn.execute(
            """SELECT open_time, open, high, low, close, volume
               FROM klines WHERE symbol=? AND timeframe=? AND open_time>=?
               ORDER BY open_time ASC LIMIT ?""",
            (symbol, timeframe, since, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT open_time, open, high, low, close, volume
               FROM klines WHERE symbol=? AND timeframe=?
               ORDER BY open_time DESC LIMIT ?""",
            (symbol, timeframe, limit),
        ).fetchall()
        rows.reverse()
    conn.close()

    if not rows:
        return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows, columns=["open_time", "open", "high", "low", "close", "volume"])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("open_time", inplace=True)
    df.sort_index(inplace=True)
    return df


# ---------------------------------------------------------------------------
# Binance REST API
# ---------------------------------------------------------------------------

BASE_URL = "https://api.binance.com/api/v3"


def fetch_historical(symbol: str, timeframe: str, limit: int = LOOKBACK_CANDLES) -> pd.DataFrame:
    """拉取历史K线（REST API），写入SQLite并返回DataFrame"""
    params = {"symbol": symbol.replace("/", ""), "interval": timeframe, "limit": limit}
    try:
        resp = requests.get(f"{BASE_URL}/klines", params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"拉取历史数据失败 {symbol} {timeframe}: {e}")
        return pd.DataFrame()

    raw = resp.json()
    candles = []
    for k in raw:
        candles.append({
            "open_time": k[0],
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": k[6],
        })

    n = insert_klines(symbol, timeframe, candles)
    logger.info(f"历史数据 {symbol} {timeframe}: 拉取 {len(candles)} 条, 新增 {n} 条")
    return get_klines(symbol, timeframe, limit)


def fill_history() -> None:
    """启动时拉取所有币种×所有时间框架的历史数据"""
    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            fetch_historical(symbol, tf)


# ---------------------------------------------------------------------------
# Binance WebSocket
# ---------------------------------------------------------------------------

STREAM_BASE = "wss://stream.binance.com:9443/ws"


def _on_message(ws: WebSocketApp, raw: str) -> None:
    """WebSocket 消息回调：解析K线写入SQLite"""
    try:
        msg = json.loads(raw)
        kline = msg.get("k", {})
        if not kline or not kline.get("x"):
            return  # K线未完结，跳过

        candle = {
            "open_time": kline["t"],
            "open": float(kline["o"]),
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "close": float(kline["c"]),
            "volume": float(kline["v"]),
            "close_time": kline["T"],
        }
        symbol = msg["s"].replace("USDT", "/USDT")
        insert_klines(symbol, "5m", [candle])
        logger.debug(f"WS 写入: {symbol} 5m close={candle['close']}")
    except Exception:
        logger.exception("WebSocket 消息处理异常")


def _on_error(ws: WebSocketApp, err: str) -> None:
    logger.error(f"WebSocket 错误: {err}")


def _on_close(ws: WebSocketApp, status: int, msg: str) -> None:
    logger.warning(f"WebSocket 断开: status={status} msg={msg}")


def _on_open(ws: WebSocketApp) -> None:
    logger.info("WebSocket 已连接")


def _build_stream_url() -> str:
    """构建组合流地址: wss://stream.binance.com:9443/ws/btcusdt@kline_5m/ethusdt@kline_5m/..."""
    streams = []
    for s in SYMBOLS:
        pair = s.replace("/", "").lower()
        streams.append(f"{pair}@kline_5m")
    return f"{STREAM_BASE}/{'/'.join(streams)}"


class Collector:
    """行情采集器，优先WebSocket，不可用时回退到REST轮询"""

    def __init__(self) -> None:
        self.ws: Optional[WebSocketApp] = None
        self.thread: Optional[threading.Thread] = None
        self._running = False
        self._reconnect_delay = 3
        self._use_ws = True  # 是否尝试WebSocket

    def start(self) -> None:
        """启动后台采集线程"""
        if self._running:
            return
        self._running = True
        self.thread = threading.Thread(target=self._run_forever, daemon=True)
        self.thread.start()
        logger.info("采集器已启动")

    def stop(self) -> None:
        """停止采集"""
        self._running = False
        if self.ws:
            self.ws.close()
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("采集器已停止")

    def _run_forever(self) -> None:
        """主循环：尝试WebSocket，失败则用REST轮询"""
        url = _build_stream_url()
        ws_failed = False

        while self._running:
            if self._use_ws and not ws_failed:
                self.ws = WebSocketApp(
                    url,
                    on_open=_on_open,
                    on_message=_on_message,
                    on_error=_on_error,
                    on_close=_on_close,
                )
                try:
                    self.ws.run_forever(ping_interval=60, ping_timeout=10)
                except Exception:
                    logger.exception("WebSocket 异常")

            # WebSocket 断开 → 回退到 REST 轮询
            if self._running:
                logger.info("回退到 REST 轮询模式（每60s拉一次）")
                self._poll_rest_loop()

            if not self._running:
                break

    def _poll_rest_loop(self) -> None:
        """REST轮询：每分钟拉一次5m K线"""
        while self._running:
            for symbol in SYMBOLS:
                try:
                    fetch_historical(symbol, "5m", limit=5)
                except Exception:
                    logger.exception(f"REST轮询失败 {symbol}")
            time.sleep(60)


# 全局单例
_collector: Optional[Collector] = None


def get_collector() -> Collector:
    global _collector
    if _collector is None:
        _collector = Collector()
    return _collector


# ---------------------------------------------------------------------------
# 便捷入口
# ---------------------------------------------------------------------------

def startup() -> None:
    """系统启动：初始化数据库 → 拉历史数据 → 启动WebSocket"""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    init_db()
    logger.info("数据库初始化完成")
    fill_history()
    get_collector().start()


if __name__ == "__main__":
    startup()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        get_collector().stop()
        logger.info("已退出")
