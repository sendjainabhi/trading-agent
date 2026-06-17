# AlphaQuant — AI Trading Assistant

AlphaQuant is a Spring Boot + Spring AI trading assistant that connects to live market data, runs multi-timeframe technical analysis, and delivers plain-English trade recommendations through a chat interface. Ask it to analyse a specific stock, scan the market for top setups, or check what is moving in pre-market — it fetches real data, calculates the signals, and tells you exactly what to do and why.

---

## Architecture

```
Browser (index.html)
    │
    ├── POST /api/chat/stream   → TradingAgentService  → LLM (Ollama / qwen3:30b)
    │                                                        │
    │                                                   Spring AI tool calls
    │                                                        │
    ├── GET  /api/price/{sym}   → TradingAgentController     ├── stockPriceFunction
    ├── GET  /api/search?q=     → TradingAgentController     ├── historicalTrendFunction
    └── GET  /api/backtest      → BacktestController         ├── generalMarketScannerFunction
                                                             └── preMarketScannerFunction
                                                                      │
                                               ┌──────────────────────┼─────────────────────┐
                                          Alpaca WS              Alpaca REST           Yahoo Finance
                                        (live quotes)          (OHLCV bars)           (price/search)
                                                                      │
                                                               Finnhub REST
                                                              (news sentiment)
```

### Components

| File | Responsibility |
|---|---|
| `TradingAgentService.java` | Intent detection, prompt enrichment, LLM streaming |
| `McpServerConfig.java` | 4 Spring AI tool functions + full MTF analysis engine |
| `TradingAgentController.java` | REST endpoints: chat stream, price refresh, symbol search |
| `BacktestController.java` | Walk-forward backtesting endpoint |
| `BacktestService.java` | SMA20 + RSI14 scoring engine over historical daily bars |
| `AlpacaStreamService.java` | Alpaca WebSocket — live trade/quote cache (30 s TTL) |
| `MarketClockService.java` | Session detection (Pre/Regular/Post/Closed) via Alpaca clock API |

---

## Features

### Live Market Analysis
- **Single-stock deep-dive** — full multi-timeframe analysis on any US equity/ETF
- **Market scanner** — fetches Yahoo Finance most-active tickers, runs MTF on each, returns top 5 by absolute confluence score
- **Pre-market scanner** — scans a 30-stock watchlist for gap plays and pre-market patterns (Gap & Go, Gap & Fade, Consolidating)

### Multi-Timeframe Scoring Engine
Every analysis fetches 4 timeframes **in parallel** via Alpaca REST:

| Timeframe | Indicators | Weight |
|---|---|---|
| Daily (45 d) | SMA20 deviation + RSI14 | 36–63% depending on horizon |
| 1-Hour (10 d) | MACD (EMA12–EMA26) + RSI9 + price vs EMA26 | 27% |
| 15-Min (5 d) | SMA9/21 cross + RSI9 + 3-bar momentum | 18% |
| 5-Min (5 d) | VWAP (intraday) + 3-bar momentum | 0–9% |

Scores are combined into a **`total_confluence_score`** (−100 to +100), then amplified or dampened by today's volume vs the 30-day average.

### Trade Signals
- **Confluence score** drives the trade verdict (5 states: execute long, wait for VWAP dip, execute short, fade bounce, sit out)
- **ATR-based stops**: 1× ATR intraday → 2× ATR for multi-week positions
- **Implied volatility** blended from ATM options (55%) and 20-day realised volatility (45%)
- **Expected move ranges**: tomorrow, next week, and any custom window (e.g. "in 4 weeks")
- **Buy/sell checklist**: 6 independent signals scored per direction (SMA20, RSI, MACD, VWAP, hourly trend, volume confirmation)

### Buy Strength Ratings
| Score | Label |
|---|---|
| Net buy ≥ 4/6 | Strong Buy |
| Net buy ≥ 2/6 | Buy |
| −1 to +1 | Watch |
| Net sell ≥ 2/6 | Sell Signal |
| Net sell ≥ 4/6 | Strong Sell |

### Backtesting
`GET /api/backtest?symbol=AAPL&days=252&holdDays=5&threshold=20`

Replays the same SMA20 + RSI14 scoring engine over historical daily bars using a strict walk-forward approach. Reports: total trades, win rate, avg return, compounded total return, max drawdown, Sharpe ratio, and a trade log of the last 20 trades.

### Chat UI
- **Symbol autocomplete** — dynamic dropdown powered by Yahoo Finance search as you type (↑↓ to navigate, Enter to select, Escape to dismiss)
- **Live price strip** — auto-refreshes ticker prices inside each response at configurable intervals (30 s / 1 m / 2 m / 5 m / off)
- **Progress feedback** — shows "Fetching live market data..." while the backend pipeline runs
- **Cancel button** — abort any in-flight request via the Send button mid-request
- **Suggestion chips** — one-click shortcuts for Pre-Market Movers, Market Scan, Bullish/Bearish Movers, Gap Plays, Most Active

---

## Data Sources

| Source | Used For | Auth |
|---|---|---|
| Alpaca Markets WebSocket `wss://stream.data.alpaca.markets/v2/iex` | Live trade + quote stream | API key + secret |
| Alpaca Markets REST `https://data.alpaca.markets/v2/stocks/bars` | OHLCV bars (1D/1H/15M/5M) | API key + secret |
| Alpaca Markets REST `https://api.alpaca.markets/v2/clock` | Market session (open/closed) | API key + secret |
| Alpaca Markets REST `https://data.alpaca.markets/v1beta1/options/snapshots` | ATM implied volatility | API key + secret |
| Yahoo Finance (free) | Current price, day high/low/volume, symbol search | None |
| Finnhub `https://finnhub.io/api/v1/news-sentiment` | Bull/bear sentiment + buzz ratio | API key |

---

## Prerequisites

| Requirement | Version |
|---|---|
| Java | 17+ |
| Maven | 3.8+ |
| Ollama | Latest |
| Ollama model | `qwen3:30b` (or any model with function-calling support) |

Pull the model before starting:
```bash
ollama pull qwen3:30b
```

---

## Configuration

All credentials live in `src/main/resources/application.yml`:

```yaml
server:
  port: 8080

spring:
  ai:
    ollama:
      base-url: http://127.0.0.1:11434
      chat:
        options:
          model: qwen3:30b
          temperature: 0.0

alpaca:
  api:
    key: "YOUR_ALPACA_API_KEY"
    secret: "YOUR_ALPACA_API_SECRET"

market:
  provider:
    api-url: "https://finnhub.io/api/v1/quote?symbol="
    news-url: "https://finnhub.io/api/v1/company-news?symbol="
    api-key: "YOUR_FINNHUB_API_KEY"
    connect-timeout-seconds: 5
    read-timeout-seconds: 5
```

> **Security note:** Never commit real API keys to source control. Use environment variable substitution (`${ALPACA_API_KEY}`) or a secrets manager in production.

Free accounts that cover all features:
- **Alpaca Markets** — [alpaca.markets](https://alpaca.markets) (free paper trading account gives full data access)
- **Finnhub** — [finnhub.io](https://finnhub.io) (free tier is sufficient)

---

## Getting Started

### 1. Start Ollama

```bash
ollama serve
```

### 2. Build and Run

```bash
cd trading-agent
mvn clean spring-boot:run
```

### 3. Open the App

```
http://localhost:8080
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/chat/stream` | Main chat endpoint — streams LLM response as plain text |
| `GET` | `/api/price/{symbol}` | Lightweight price refresh — tries Alpaca WS cache, falls back to Yahoo Finance |
| `GET` | `/api/search?q={term}` | Symbol autocomplete — proxies Yahoo Finance search, returns `[{symbol, name}]` |
| `GET` | `/api/backtest` | Walk-forward backtest — params: `symbol`, `days` (default 252), `holdDays` (default 5), `threshold` (default 20) |

### Chat request body
```json
{ "input": "analyze NVDA" }
```

### Backtest example
```
GET /api/backtest?symbol=TSLA&days=252&holdDays=5&threshold=25
```

---

## Example Queries

| Query | What happens |
|---|---|
| `NVDA` | Full single-stock analysis: price, MTF scores, buy/sell checklist, trade plan |
| `analyze Tesla` | Same as above — company names are resolved to tickers automatically |
| `AAPL in 4 weeks` | Single-stock analysis with a 20-trading-day expected move window |
| `scan the market` | Top 5 most-active stocks scored and ranked by confluence, rendered as a table |
| `pre market movers` | Gap & Go / Gap & Fade patterns across 30+ stocks before the open |
| `what are the biggest bullish movers today?` | Market scan filtered for strongest buy signals |
| `TSLA trend` | Historical RSI14, EMA crossover status, ATR-based support/resistance |

---

## How the Trade Plan Works

When a stock has a clear signal (confluence score outside ±15), AlphaQuant picks **one** of three blocks:

**Buy setup (score > +15)**
- Strategy in plain English: what is driving the buy and exactly where to enter
- Entry, take-profit, stop-loss prices
- Risk vs. Reward: "You risk $X per share to potentially gain $Y — a Z:1 payoff"
- Optional options play: call strike, target, expiry

**Sell / Short setup (score < −15)**
- Same structure but for the short side
- Entry, cover/exit target, stop-loss
- Optional options play: put strike, target, expiry

**No clear trade (score −15 to +15)**
- Explains why there is no edge right now
- Breakout/breakdown price levels to watch
- Guidance for existing holders (stop loss + trim level)

---

## Project Structure

```
trading-agent/
├── src/main/java/com/quant/agent/
│   ├── AlphaQuantAgentApplication.java   — Spring Boot entry point
│   ├── AlpacaStreamService.java          — WebSocket live quote cache
│   ├── MarketClockService.java           — Session detection
│   ├── McpServerConfig.java              — 4 AI tool functions + MTF engine
│   ├── TradingAgentService.java          — Intent routing + LLM client
│   ├── TradingAgentController.java       — REST: chat, price, search
│   ├── BacktestService.java              — Walk-forward backtest logic
│   └── BacktestController.java           — REST: backtest endpoint
└── src/main/resources/
    ├── application.yml                   — All configuration + API keys
    ├── logback-spring.xml                — Logging config
    └── static/index.html                 — Single-page chat UI
```
