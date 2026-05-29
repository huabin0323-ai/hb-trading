# hb-trading

加密货币半自动交易系统 — 实时盯盘、多源情绪分析、阿布价格行为学信号、回测验证。

## 模块

| 模块 | 文件 | 功能 |
|------|------|------|
| 行情采集 | `src/collector.py` | Binance WebSocket 实时数据 + SQLite 存储 |
| 情绪聚合 | `src/sentiment.py` | Fear & Greed + 新闻情绪 → 0-100 分数 |
| 价格行为 | `src/price_action.py` | Al Brooks 市场结构/信号识别 |
| 信号引擎 | `src/signal_engine.py` | 技术面 + 情绪面 综合打分 |
| 宏观感知 | `src/macros.py` | 全球宏观风险仪表盘 |
| 回测 | `src/backtest.py` | 历史回测验证策略 |
| 面板 | `dashboard.py` | Streamlit 可视化主控台 |

## 快速开始

```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

## 策略概要

趋势跟踪 + Al Brooks 价格行为学（市场结构/二次入场/楔形/80%规则）+ 情绪复合评分，半自动交易。
