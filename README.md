# AlphaQuant – AI-Powered Trading Assistant

AlphaQuant is a personal AI trading assistant that connects to live market data, analyzes stocks across multiple timeframes, and gives you clear, plain-English trade plans with defined entry, target, and stop levels.

---

## ✨ Core Features

### 📊 Multi-Timeframe Stock Analysis
Ask about any US stock and get a full trade plan in seconds — trend direction across daily/1h/15m/5m timeframes, RSI momentum, VWAP position, ADX trend strength, and an options strategy with exact strikes and expiry.

### 🔄 Swing Trade Detection
Real swing high/low levels detected from 30+ daily bars. Each stock shows its nearest swing support and resistance with a "tested N×" count. Stocks near key levels get a dedicated swing trade card:
- **SWING_LONG** — near swing support, long entry setup
- **SWING_SHORT** — near swing resistance, short entry setup
- **RANGE_PLAY** — price between levels with ADX < 20 → Iron Condor

### 🔍 Six Market Scanners
| Scanner | What it does |
|---|---|
| 📊 Market Scan | Top options plays from most-active stocks |
| 📈 Bullish Movers | Biggest gainers (Yahoo `day_gainers` screener) |
| 📉 Bearish Movers | Biggest losers (Yahoo `day_losers` screener) |
| 🔄 Swing Plays | Range-bound stocks (>500K volume) near swing support/resistance |
| 🌅 Pre-Market Movers | Gap-ups/downs with volume before the open |
| 🎡 Wheel Strategy | Cash-secured puts ($3–$80 stocks/ETFs, ≥1%/week target) |

### 📐 Price Targets
IV-based expected move for tomorrow, next week, and a 15-day range — always populated from realized volatility even when no custom timeframe is requested.

### 🐋 Smart Money Signals
Insider MSPR (buy/sell sentiment ratio), analyst consensus (Buy/Hold/Sell counts), and institutional flow — flagged as conflicting when chart and smart money disagree.

### 🌍 Market Regime Awareness
SPY trend + VIX-based regime detection (Normal / Elevated / High Fear) on every response. Position sizing adjusts automatically when volatility is elevated.

### 🤖 Multi-Provider AI
Switch between **Ollama** (local, free), **OpenAI**, or **Anthropic Claude** from the settings panel without restarting. Config is remembered across sessions.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Backend | Java 17 · Spring Boot 3.3.4 · Spring AI 1.0.0 |
| Market Data | Alpaca API (bars, options, news) · Finnhub (insider, earnings) |
| Screener Data | Yahoo Finance (day gainers/losers, most active) |
| AI Providers | Ollama · OpenAI · Anthropic |
| Frontend | HTML · CSS · JavaScript (no framework) |

---

## 🚀 Getting Started

### 1. Configure API Keys

Edit `src/main/resources/application.yml`:

```yaml
alpaca:
  api:
    key: "YOUR_ALPACA_KEY"
    secret: "YOUR_ALPACA_SECRET"

market:
  provider:
    api-key: "YOUR_FINNHUB_KEY"
```

### 2. Run

```bash
mvn spring-boot:run
```

### 3. Open

```
http://localhost:9090
```

---

## 💬 Example Queries

```
analyze NVDA
swing trade setups
what are the biggest bullish movers today?
pre market movers
wheel strategy scan
scan the market and show top options plays
```

---

## 📐 Stock Analysis Output Format

```
AAPL ($295.65)  |  ⏳ Hold & Wait  |  ✅ Normal  |  Market Closed  |  -0.79%

Trend: Daily ↓ · 1h ↓ · 15m ↓ · 5m ↓  |  RSI 42.1 (weak)  |  Why: below 20-day avg, VWAP pressure
VWAP $299.33  |  Vol 40,202,659  |  Range $296.76–$302.42  |  ADX 25.9 (Trending ✅)
Key Levels: PDH $301.88 · PDL $294.22  |  Swing Support: $289.10 (3× tested) · Swing Resistance: $308.45
Price Targets (IV 32.1%): Tomorrow $292–$299  ·  Next week $287–$304  ·  15-day $280–$311
Watch: breaks below $296.76 → downside  ·  bounces above $302.42 → short-cover possible
Smart Money: Institutions Buying  |  Insider MSPR 45.20 (buying)  |  Analysts 35 Buy · 8 Hold

WHAT TO DO  [colored card — Buy / Sell / Hold / Swing Long / Swing Short / Range Play]
```

---

## ⚙️ Switching AI Providers

Click **⚙** in the top-right to configure your model. Supports:
- **Ollama** — local, no API cost (e.g. `qwen3:30b`)
- **OpenAI** — GPT-4o or any OpenAI-compatible proxy
- **Anthropic** — Claude Sonnet or Opus
