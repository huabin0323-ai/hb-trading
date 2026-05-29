"""行情采集模块 — Binance REST API + 资金费率 + 数据质量监控

针对国内网络环境优化：直接用 requests 调 Binance API（已验证通过），
避免 ccxt 的连接问题。SQLite 存储，后台轮询，自动质量检查。
"""

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from config import (
    DATA_DIR, SYMBOLS, TIMEFRAMES, TIMEFRAME_MAIN, LOOKBACK_CANDLES,
    REST_POLL_INTERVAL, MAX_MISSING_MINUTES, PRICE_CHANGE_WARN_PCT,
    VOLUME_SPIKE_MULTIPLIER, DATA_FRESHNESS_SEC, OHLCV_COLUMNS,
    FUNDING_POSITIVE_THRESHOLD, FUNDING_NEGATIVE_THRESHOLD,
    LOG_FORMAT, LOG_LEVEL,
)

logger = logging.getLogger("collector")

DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "market.db"

# Binance API 端点
BINANCE_REST = "https://api.binance.com"
BINANCE_FAPI = "https://fapi.binance.com"   # 合约API（资金费率/未平仓合约）

# 请求超时与重试
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2  # 秒


# ======================================================================
# HTTP 工具
# ======================================================================

def _http_get(url: str, params: dict = None, base: str = BINANCE_REST) -> Optional[dict]:
    """带重试和超时的 GET 请求，返回 JSON"""
    full_url = f"{base}{url}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(full_url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.Timeout:
            logger.warning(f"超时 (attempt {attempt+1}/{MAX_RETRIES}): {url}")
        except requests.RequestException as e:
            logger.warning(f"请求失败 (attempt {attempt+1}/{MAX_RETRIES}): {e}")
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY * (attempt + 1))
    logger.error(f"请求最终失败: {url}")
    return None


# ======================================================================
# SQLite 数据层
# ======================================================================

class Database:
    """线程安全 SQLite"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._local = threading.local()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._get_conn().execute(sql, params)

    def commit(self) -> None:
        with self._lock:
            self._get_conn().commit()

    def init_tables(self) -> None:
        self.execute("""
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
        self.execute("""
            CREATE TABLE IF NOT EXISTS funding_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                rate REAL NOT NULL,
                next_funding_time INTEGER,
                mark_price REAL,
                UNIQUE(symbol, timestamp)
            )
        """)
        self.execute("""
            CREATE TABLE IF NOT EXISTS open_interest (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                oi_value REAL NOT NULL,
                UNIQUE(symbol, timestamp)
            )
        """)
        self.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                check_type TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT
            )
        """)
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_klines_sym_tf ON klines(symbol, timeframe)",
            "CREATE INDEX IF NOT EXISTS idx_klines_open_time ON klines(open_time)",
            "CREATE INDEX IF NOT EXISTS idx_funding_ts ON funding_rates(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_oi_ts ON open_interest(timestamp)",
        ]:
            self.execute(idx_sql)
        self.commit()

    def insert_klines(self, symbol: str, timeframe: str, candles: list[dict]) -> int:
        count = 0
        for c in candles:
            self.execute(
                """INSERT OR IGNORE INTO klines
                   (symbol, timeframe, open_time, open, high, low, close, volume, close_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (symbol, timeframe, c["open_time"], c["open"], c["high"],
                 c["low"], c["close"], c["volume"], c["close_time"]),
            )
            count += 1
        self.commit()
        return count

    def get_klines(self, symbol: str, timeframe: str,
                   limit: int = 500, since: Optional[int] = None) -> pd.DataFrame:
        cond = "WHERE symbol=? AND timeframe=?"
        params = [symbol, timeframe]
        if since is not None:
            cond += " AND open_time>=?"
            params.append(int(since))
        rows = self.execute(
            f"""SELECT open_time, open, high, low, close, volume
                FROM klines {cond}
                ORDER BY open_time DESC LIMIT ?""",
            tuple(params) + (limit,),
        ).fetchall()
        rows.reverse()
        if not rows:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        df = pd.DataFrame(rows, columns=["open_time"] + OHLCV_COLUMNS)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df.set_index("open_time", inplace=True)
        df.sort_index(inplace=True)
        return df

    def get_latest_open_time(self, symbol: str, timeframe: str) -> int:
        row = self.execute(
            "SELECT MAX(open_time) FROM klines WHERE symbol=? AND timeframe=?",
            (symbol, timeframe),
        ).fetchone()
        return row[0] if row[0] else 0

    def insert_funding_rate(self, symbol: str, timestamp: int, rate: float,
                            next_time: int = 0, mark_price: float = 0.0) -> None:
        self.execute(
            """INSERT OR IGNORE INTO funding_rates
               (symbol, timestamp, rate, next_funding_time, mark_price)
               VALUES (?, ?, ?, ?, ?)""",
            (symbol, timestamp, rate, next_time, mark_price),
        )
        self.commit()

    def get_funding_rates(self, symbol: str, limit: int = 50) -> pd.DataFrame:
        rows = self.execute(
            """SELECT timestamp, rate, mark_price FROM funding_rates
               WHERE symbol=? ORDER BY timestamp DESC LIMIT ?""",
            (symbol, limit),
        ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["timestamp", "rate", "mark_price"])
        rows.reverse()
        df = pd.DataFrame(rows, columns=["timestamp", "rate", "mark_price"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df

    def insert_open_interest(self, symbol: str, timestamp: int, oi_value: float) -> None:
        self.execute(
            "INSERT OR IGNORE INTO open_interest (symbol, timestamp, oi_value) VALUES (?, ?, ?)",
            (symbol, timestamp, oi_value),
        )
        self.commit()

    def get_open_interest(self, symbol: str, limit: int = 50) -> pd.DataFrame:
        rows = self.execute(
            "SELECT timestamp, oi_value FROM open_interest WHERE symbol=? ORDER BY timestamp DESC LIMIT ?",
            (symbol, limit),
        ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["timestamp", "oi_value"])
        rows.reverse()
        df = pd.DataFrame(rows, columns=["timestamp", "oi_value"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df

    def log_quality(self, ts: int, symbol: str, check_type: str,
                    status: str, detail: str = "") -> None:
        self.execute(
            "INSERT INTO data_quality_log (timestamp, symbol, check_type, status, detail) VALUES (?, ?, ?, ?, ?)",
            (ts, symbol, check_type, status, detail),
        )
        self.commit()

    def total_klines(self, symbol: str = None, timeframe: str = None) -> int:
        conds = []
        params = []
        if symbol:
            conds.append("symbol=?")
            params.append(symbol)
        if timeframe:
            conds.append("timeframe=?")
            params.append(timeframe)
        where = " AND ".join(conds) if conds else "1=1"
        row = self.execute(f"SELECT COUNT(*) FROM klines WHERE {where}", tuple(params)).fetchone()
        return row[0]


# ======================================================================
# K线采集
# ======================================================================

def _binance_symbol(s: str) -> str:
    """BTC/USDT → BTCUSDT"""
    return s.replace("/", "")


def fetch_historical(symbol: str, timeframe: str,
                     limit: int = LOOKBACK_CANDLES) -> pd.DataFrame:
    """拉取历史K线，写入DB，返回DataFrame"""
    raw = _http_get("/api/v3/klines", {
        "symbol": _binance_symbol(symbol),
        "interval": timeframe,
        "limit": limit,
    })
    if raw is None:
        return pd.DataFrame()

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

    db = Database()
    n = db.insert_klines(symbol, timeframe, candles)
    logger.info(f"历史K线 {symbol} {timeframe}: 拉取 {len(candles)} 新增 {n}")
    return db.get_klines(symbol, timeframe, limit)


def fetch_incremental(symbol: str, timeframe: str) -> int:
    """增量拉取：从最新K线时间到现在，只拉新增的"""
    db = Database()
    since = db.get_latest_open_time(symbol, timeframe)
    if since == 0:
        return 0
    raw = _http_get("/api/v3/klines", {
        "symbol": _binance_symbol(symbol),
        "interval": timeframe,
        "startTime": since + 1,
        "limit": 500,
    })
    if raw is None or len(raw) == 0:
        return 0

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
    return db.insert_klines(symbol, timeframe, candles)


def fill_all_history() -> dict:
    """启动时填充所有历史数据，返回统计"""
    stats = {}
    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            df = fetch_historical(symbol, tf)
            stats[f"{symbol}:{tf}"] = len(df)
    return stats


class KlineCollector:
    """后台K线轮询采集器"""

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._poll_count = 0

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info(f"K线轮询启动 ({REST_POLL_INTERVAL}s 间隔)")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _poll_loop(self) -> None:
        while self._running:
            for symbol in SYMBOLS:
                try:
                    n = fetch_incremental(symbol, TIMEFRAME_MAIN)
                    if n > 0:
                        logger.debug(f"{symbol}: +{n} 新K线")
                except Exception:
                    logger.exception(f"轮询异常 {symbol}")
            self._poll_count += 1
            time.sleep(REST_POLL_INTERVAL)


# ======================================================================
# 衍生品数据（资金费率 + 未平仓合约）
# ======================================================================

def fetch_funding_rate(symbol: str) -> Optional[dict]:
    """从 Binance Futures API 拉取最新资金费率"""
    data = _http_get("/fapi/v1/premiumIndex", {"symbol": _binance_symbol(symbol)},
                     base=BINANCE_FAPI)
    if data is None:
        return None

    rate = float(data.get("lastFundingRate", 0))
    next_time = int(data.get("nextFundingTime", 0))
    mark_price = float(data.get("markPrice", 0))
    ts = int(time.time() * 1000)

    db = Database()
    db.insert_funding_rate(symbol, ts, rate, next_time, mark_price)
    logger.debug(f"资金费率 {symbol}: {rate:.6f} mark={mark_price:.2f}")
    return {"rate": rate, "next_time": next_time, "mark_price": mark_price}


def fetch_open_interest(symbol: str) -> Optional[dict]:
    """从 Binance Futures API 拉取未平仓合约"""
    data = _http_get("/fapi/v1/openInterest", {"symbol": _binance_symbol(symbol)},
                     base=BINANCE_FAPI)
    if data is None:
        return None

    value = float(data.get("openInterest", 0))
    ts = int(data.get("time", time.time() * 1000))

    db = Database()
    db.insert_open_interest(symbol, ts, value)
    logger.debug(f"未平仓 {symbol}: {value:.0f}")
    return {"value": value, "timestamp": ts}


def funding_signal(symbol: str) -> dict:
    """资金费率情绪分析"""
    db = Database()
    df = db.get_funding_rates(symbol, limit=3)
    if df.empty:
        return {"crowded": None, "rate": 0, "message": "无数据"}

    last = df.iloc[-1]["rate"]
    if last > FUNDING_POSITIVE_THRESHOLD:
        return {"crowded": "long", "rate": last,
                "message": f"多头拥挤（费率 {last:.4f}），注意回调风险"}
    elif last < FUNDING_NEGATIVE_THRESHOLD:
        return {"crowded": "short", "rate": last,
                "message": f"空头拥挤（费率 {last:.4f}），注意轧空风险"}
    return {"crowded": None, "rate": last, "message": "中性"}


class DerivativesCollector:
    """后台衍生品数据轮询"""

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def fetch_all(self) -> None:
        for s in SYMBOLS:
            fetch_funding_rate(s)
            fetch_open_interest(s)

    def start(self, interval: int = 300) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, args=(interval,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _poll_loop(self, interval: int) -> None:
        while self._running:
            try:
                self.fetch_all()
            except Exception:
                logger.exception("衍生品轮询异常")
            time.sleep(interval)


# ======================================================================
# 数据质量监控
# ======================================================================

def _tf_minutes(tf: str) -> int:
    return {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}.get(tf, 5)


class DataQualityMonitor:
    """数据质量后台检查"""

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def check_all(self) -> list[dict]:
        """全面检查，返回告警列表"""
        db = Database()
        now = int(time.time() * 1000)
        alerts = []

        for symbol in SYMBOLS:
            # 缺口检测
            df = db.get_klines(symbol, TIMEFRAME_MAIN, limit=200)
            if len(df) >= 2:
                expected = pd.Timedelta(minutes=_tf_minutes(TIMEFRAME_MAIN))
                for i in range(1, len(df)):
                    actual = df.index[i] - df.index[i - 1]
                    if actual > expected * MAX_MISSING_MINUTES:
                        msg = f"{df.index[i-1]} ~ {df.index[i]} 缺口 {actual}"
                        db.log_quality(now, symbol, "gap", "warn", msg)
                        alerts.append({"type": "gap", "symbol": symbol, "detail": msg})

            # 异常价格
            for i in range(len(df)):
                if df["open"].iloc[i] > 0:
                    chg = abs(df["close"].iloc[i] / df["open"].iloc[i] - 1) * 100
                    if chg > PRICE_CHANGE_WARN_PCT:
                        msg = f"{df.index[i]} 涨跌幅 {chg:.1f}%"
                        db.log_quality(now, symbol, "anomaly", "warn", msg)
                        alerts.append({"type": "anomaly", "symbol": symbol, "detail": msg})

            # 新鲜度
            if not df.empty:
                age = (pd.Timestamp.now(tz="utc") - df.index[-1]).total_seconds()
                if age > DATA_FRESHNESS_SEC:
                    msg = f"数据延迟 {age:.0f}s"
                    db.log_quality(now, symbol, "freshness", "warn", msg)
                    alerts.append({"type": "freshness", "symbol": symbol, "detail": msg})

        return alerts

    def start(self, interval: int = 300) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, args=(interval,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _monitor_loop(self, interval: int) -> None:
        while self._running:
            try:
                alerts = self.check_all()
                if alerts:
                    logger.warning(f"数据质量告警: {len(alerts)} 条")
            except Exception:
                logger.exception("质量监控异常")
            time.sleep(interval)


# ======================================================================
# 全局单例
# ======================================================================

_db: Optional[Database] = None
_kline_collector: Optional[KlineCollector] = None
_derivatives_collector: Optional[DerivativesCollector] = None
_quality_monitor: Optional[DataQualityMonitor] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
        _db.init_tables()
    return _db


def get_kline_collector() -> KlineCollector:
    global _kline_collector
    if _kline_collector is None:
        _kline_collector = KlineCollector()
    return _kline_collector


def get_derivatives_collector() -> DerivativesCollector:
    global _derivatives_collector
    if _derivatives_collector is None:
        _derivatives_collector = DerivativesCollector()
    return _derivatives_collector


def get_quality_monitor() -> DataQualityMonitor:
    global _quality_monitor
    if _quality_monitor is None:
        _quality_monitor = DataQualityMonitor()
    return _quality_monitor


# ======================================================================
# 入口
# ======================================================================

def startup() -> None:
    """一键启动所有采集子系统"""
    logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)

    get_db()
    logger.info("数据库初始化完成")

    logger.info("填充历史K线...")
    stats = fill_all_history()
    total = sum(stats.values())
    logger.info(f"历史数据填充完成: {total} 条 ({len(stats)} 组)")

    get_kline_collector().start()
    logger.info(f"K线轮询启动 ({REST_POLL_INTERVAL}s)")

    deriv = get_derivatives_collector()
    deriv.fetch_all()
    deriv.start()
    logger.info("衍生品数据采集启动 (300s)")

    get_quality_monitor().start()
    logger.info("数据质量监控启动 (300s)")

    logger.info("=== 行情采集系统就绪 ===")


def shutdown() -> None:
    for c in [get_kline_collector(), get_derivatives_collector(), get_quality_monitor()]:
        c.stop()


def health_report() -> dict:
    """返回系统健康状态"""
    db = get_db()
    latest = {}
    for s in SYMBOLS:
        df = db.get_klines(s, TIMEFRAME_MAIN, limit=1)
        if not df.empty:
            latest[s] = f"{df['close'].iloc[-1]:.2f} ({df.index[-1]})"
    return {
        "total_klines": db.total_klines(),
        "latest_prices": latest,
        "poll_count": get_kline_collector()._poll_count,
    }


if __name__ == "__main__":
    startup()
    try:
        while True:
            time.sleep(60)
            rpt = health_report()
            logger.info(f"心跳: {rpt['total_klines']} 条 | {rpt['latest_prices']}")
    except KeyboardInterrupt:
        shutdown()
        logger.info("已退出")
