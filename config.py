"""项目配置"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# 路径
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"

# Binance
SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
TIMEFRAME = "5m"       # 主交易时间框架（阿布推荐 5分钟）
TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"]  # 多时间框架
LOOKBACK_CANDLES = 500  # 每次拉取的K线数量

# 情绪
FEAR_GREED_API = "https://api.alternative.me/fng/"
CRYPTOPANIC_API = "https://cryptopanic.com/api/v1/posts/"

# 信号
SIGNAL_THRESHOLD_BUY = 70    # 综合评分 ≥ 70 偏多
SIGNAL_THRESHOLD_SELL = 30   # 综合评分 ≤ 30 偏空

# 回测
INITIAL_CAPITAL = 10000      # 回测初始资金 (USDT)
COMMISSION = 0.001           # 手续费 0.1%
