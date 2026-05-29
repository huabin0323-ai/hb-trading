# hb-trading 开发路线图

## 分阶段计划

---

### 阶段 1：行情采集管道

**目标**：数据能进库，能查，WebSocket 不断线

**文件**：`src/collector.py`

**做什么**：
- Binance WebSocket 订阅 BTC/USDT、ETH/USDT、SOL/USDT 的 5m K线
- 断线自动重连，新K线写入 SQLite
- 同时拉取历史K线（多时间框架：5m/15m/1h/4h/1d）填满 LOOKBACK_CANDLES
- 提供查询函数：`get_ohlcv(symbol, timeframe, limit)`

**验证方式**：
```bash
python -c "
from src.collector import fetch_historical, watch_live
# 1. 拉历史K线
df = fetch_historical('BTC/USDT', '5m', 100)
print(f'拉取成功: {len(df)} 条')
print(df.tail(3))
# 2. 确认数据格式: open/high/low/close/volume 全是正数，时间无重复
"
```
**完成标准**：历史数据拉取无报错 + WebSocket 运行 60 秒收到 3 条以上新数据

---

### 阶段 2：价格行为引擎

**目标**：裸K结构识别正确，输出人类可读的市场状态

**文件**：`src/price_action.py`

**做什么**：
- 识别 swing highs/lows（摆动点）
- 判定市场状态：上升趋势 / 下降趋势 / 交易区间 / 窄通道 / 宽通道
- 检测信号K线（强趋势K、Pin Bar、Inside Bar、Outside Bar）
- 检测入场信号（H1/H2/H3 序列）
- 检测楔形（3推+收敛）
- 检测失败突破（80%规则）
- 输出 0-100 技术面评分

**验证方式**：
```bash
python -c "
from src.collector import fetch_historical
from src.price_action import analyze_structure, detect_signals

df = fetch_historical('BTC/USDT', '5m', 200)
state = analyze_structure(df)
signals = detect_signals(df, state)
print(f'市场状态: {state}')
print(f'信号列表: {signals[-5:]}')
# 拿某一天的BTC数据，肉眼对比K线图验证状态判定
"
```
**完成标准**：拿 2024 年 BTC 一段明显趋势+一段震荡的数据，状态判定与实际一致

---

### 阶段 3：情绪聚合器

**目标**：输出一个合理的 0-100 情绪分，恐慌时偏低、贪婪时偏高

**文件**：`src/sentiment.py`

**做什么**：
- 接 Fear & Greed Index 公开 API（免费，无需 key）
- 接 CryptoPanic 新闻 API（免费层 3 条/次够用）
- 简单的新闻标题关键词情感（"crash/surge/ban/adopt" 等词频统计）
- 综合：恐惧贪婪 60% + 新闻情绪 40% → 0-100 分数

**验证方式**：
```bash
python -c "
from src.sentiment import get_fear_greed, get_news_sentiment, aggregate
fg = get_fear_greed()
print(f'恐惧贪婪: {fg}')
news = get_news_sentiment()
print(f'新闻情绪: {news}')
score = aggregate(fg, news)
print(f'综合情绪分: {score}/100')
"
```
**完成标准**：3 次调用返回不同但合理的分数（极端行情时分数应明显偏离 50）

---

### 阶段 4：综合信号引擎

**目标**：技术面 + 情绪面 → 一个分数 + 一句话，逻辑一致

**文件**：`src/signal_engine.py`

**做什么**：
- 加权：技术面 60% + 情绪面 40%
- 输出综合评分 + 方向建议（偏多/偏空/中性）
- 输出各因子拆解（为什么是这个分）
- H2/L2 信号 + 情绪共振 → 加分
- 情绪极端但技术面无信号 → 减分

**验证方式**：
```bash
python -c "
from src.collector import fetch_historical
from src.price_action import analyze_structure
from src.sentiment import aggregate as sentiment_score
from src.signal_engine import compute_signal

df = fetch_historical('BTC/USDT', '5m', 200)
state = analyze_structure(df)
sentiment = sentiment_score(...)
signal = compute_signal(state, sentiment)
print(f'评分: {signal.score}/100')
print(f'方向: {signal.direction}')
print(f'因子拆解: {signal.breakdown}')
"
```
**完成标准**：趋势上涨+情绪贪婪 时评分 > 70，趋势下跌+情绪恐慌 时评分 < 30

---

### 阶段 5：宏观风险感知

**目标**：识别是否有重大风险事件，影响仓位建议

**文件**：`src/macros.py`

**做什么**：
- 爬取/接入几个宏观数据源（美联储利率、CPI 日期、VIX 等价指标）
- 新闻关键词扫描（war/tariff/sanction/crackdown/default/hack 等）
- 输出宏观风险等级：低/中/高
- 高风险时自动调低仓位建议系数

**验证方式**：手动跑，看输出是否和当前全球局势一致
**完成标准**：出现重大新闻时（如关税战），风险等级明显升高

---

### 阶段 6：回测引擎

**目标**：用历史数据验证策略，得到可量化的胜率/回撤

**文件**：`src/backtest.py`

**做什么**：
- 基于 `backtesting.py` 库，接入我们的信号逻辑
- 测试参数：初始资金 $10000，手续费 0.1%
- 输出：总收益率、夏普比率、最大回撤、胜率、盈亏比
- 对比基准：纯持有 BTC 的收益

**验证方式**：
```bash
python -c "
from src.backtest import run_backtest
result = run_backtest('BTC/USDT', '5m', days=90)
print(f'收益率: {result.return_pct:.1f}%')
print(f'胜率: {result.win_rate:.1f}%')
print(f'最大回撤: {result.max_drawdown:.1f}%')
print(f'夏普比率: {result.sharpe:.2f}')
"
```
**完成标准**：90 天回测跑通，夏普 > 0.5（否则说明策略需要调整），回撤不超过 30%

---

### 阶段 7：Streamlit 面板

**目标**：一个浏览器页面，看懂一切，不用碰命令行

**文件**：`dashboard.py`

**做什么**：
- 实时 K线图（plotly，5分钟刷新）
- 市场状态指示器（趋势/区间/通道 + 方向）
- 综合信号仪表盘（评分环 + 因子拆解）
- 情绪仪表盘（恐惧贪婪指数 + 新闻列表）
- 宏观风险等级
- 历史信号列表
- 回测结果展示

**验证方式**：
```bash
streamlit run dashboard.py
# 浏览器打开，肉眼确认每个面板有数据
```
**完成标准**：所有面板正常显示数据，5 分钟内无报错

---

### 阶段 8：模拟交易

**目标**：虚拟盘跑一个星期，验证信号实战价值

**文件**：`src/paper_trader.py`

**做什么**：
- 监听实时信号，满足阈值自动记录虚拟交易
- 记录：入场价、出场价、持仓时长、盈亏
- 每晚生成当日报告

**验证方式**：跑一周 → 导出虚拟交易记录 → 算盈亏
**完成标准**：连续 1 周正收益或小亏损（-5%以内），则策略通过验证，可考虑小资金实盘

---

## 总结图

```
阶段1 ──→ 阶段2 ──→ 阶段4 ──→ 阶段6 ──→ 阶段7 ──→ 阶段8
(数据)    (技术)    (信号)    (回测)    (面板)    (模拟)
              ↘         ↗
              阶段3 ──→ 阶段5
             (情绪)    (宏观)
```

每一阶段完成后，下一阶段才能开始。任一阶段验证不通过，停下来修，不带着 bug 往前走。
