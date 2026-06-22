# AlphaQuant – AI-Powered Trading Assistant

AlphaQuant is a personal trading assistant that connects to live market data, analyzes price trends across multiple timeframes, and gives you clear, plain-English trade plans with defined risk and targets.

It supports **stocks, options, and leveraged ETFs**, scans the market for setups, and can suggest **Wheel Strategy (cash-secured puts)** for income generation — all from a simple chat interface.

---

## ✨ Features

- **Multi-Timeframe Analysis** — 5m, 15m, 1h charts analyzed together; only flags trades where all timeframes agree
- **Options Strategies** — Bull/Bear Call/Put Debit Spreads with entry, target profit, and stop loss
- **Wheel Strategy Scanner** — Scans 50+ stocks and leveraged ETFs ($3–$80) for cash-secured put income plays (≥1%/week target)
- **Pre-Market Movers** — Scans gap-ups/gap-downs with volume before the open
- **Market Regime Awareness** — VIX-based regime detection (Normal / Elevated / High Fear) with automatic position-size guidance
- **$200 Max Risk Per Trade** — Fixed risk cap per position (shares and options contracts)
- **Leveraged ETF Support** — TSLL, NVDL, AAPU, METU, AMZU, MSFU, CONL, MSTU, TQQQ, SPXL, SOXL, LABU, FNGU included across all scans
- **Multi-Provider AI** — Switch between Ollama (local), OpenAI, or Anthropic Claude from the UI settings

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Backend | Java 17 + Spring Boot 3.3.4 |
| AI Framework | Spring AI 1.0.0 (MCP / function-calling) |
| Market Data | Alpaca Data API (bars, options snapshots) |
| Quote Data | Finnhub API |
| AI Providers | Ollama · OpenAI · Anthropic |
| Frontend | HTML + CSS + JavaScript (no framework) |

---

## 📋 Prerequisites

- **Java 17+** and **Maven**
- **Alpaca account** — free paper trading account gives API key + secret
- **Finnhub API key** — free tier is sufficient
- **AI model** — one of:
  - Ollama running locally (e.g. `qwen3.6:35b`)
  - OpenAI API key
  - Anthropic API key

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

### 2. Build and Run

```bash
mvn clean compile
mvn spring-boot:run
```

### 3. Open the App

```
http://localhost:9090
```

---

## 💬 What You Can Ask

| Chip / Prompt | What It Does |
|---|---|
| 🌅 Pre-Market Movers | Gap-ups/downs with volume before open |
| 📊 Market Scan | Top options plays across stocks + ETFs |
| 🎡 Wheel Strategy | Cash-secured put income scan ($3–$80) |
| 📈 Bullish Movers | Strongest upward momentum today |
| 📉 Bearish Movers | Strongest downward momentum today |
| ⚡ Gap Plays | Stocks gapping significantly |
| 🔥 Most Active | Highest volume names |
| `analyze NVDA` | Full trade plan for a specific stock |
| `wheel strategy scan` | Wheel income picks with weekly/monthly expiry |

---

## 📐 Trade Plan Format

Each stock analysis returns a concise, non-repetitive output:

```
GOOGL ($347.69)  |  🔴 Strong Sell (5/6 ↓ · 1/6 ↑)  |  ✅ Normal  |  Post-Market  |  -5.26%
Trend: Daily ↓ · 1h ↓ · 15m → · 5m ↓  |  RSI 36.1 (weak)  |  Why: below 20-day avg, MACD falling, vol confirms
VWAP $348.00  |  Vol 52,036,611  |  Range $341.72–$358.92  |  ADX 20.0 (No Clear Trend ⚠️)
Key Levels: PDH $369.46  ·  PDL $358.74
Price Targets (IV 33.74%): Tomorrow $342–$355  ·  Next week $335–$363  ·  15-day $324–$373
Watch: breaks below $341.72 → deeper downside  ·  bounces above $358.92 → short-cover possible
Smart Money: 🐋 Institutions Buying  |  Insider MSPR 21.10 (insiders buying)  |  Analysts 61 Buy · 9 Hold  ⚠️ Conflicts with chart signal

WHAT TO DO
📉 Bear Put Debit Spread — downside bet, fixed cost upfront, that's your max loss
Entry $354.47  |  Target Profit $319.81  |  Stop Loss $373.72
Risk $19.25 · Potential gain $34.66
Qty: ~10 shares or ~1 contract (exp. July 17, 2026)
Buy 1× $355.00 Put · Sell 1× $347.50 Put · Expires July 17, 2026
```

**What each line means:**
- Line 1 — ticker, signal score, market regime, session, % change
- Line 2 — all timeframe trends + RSI + reason for signal in one line
- Line 3 — live price context (VWAP, volume, range, ADX)
- Line 4 — PDH / PDL key levels only
- Line 5 — IV-based price prediction for tomorrow, next week, 15-day
- Line 6 — key levels to watch for breakout/breakdown
- Line 7 — smart money: institution flow, insider MSPR (buy/sell ratio), analyst consensus
- WHAT TO DO — the trade: strategy, entry/target/stop, risk/gain, qty, options play

---

## 🎡 Wheel Strategy Output

Results are shown as a table:

| Stock | Price | Type | IV | Sell Put Strike | You Collect | Weekly Income | Capital Needed | Expiry | If Assigned → Sell Call |
|---|---|---|---|---|---|---|---|---|---|
| TSLA | $X | Stock | 45% | $Y | $Z | 1.2%/wk | $K | weekly | ~$X+5 |

> Leveraged ETFs (2x/3x) carry amplified risk — position size accordingly.

---

## ⚙️ Switching AI Providers

Click the **⚙** settings icon in the top-right corner to switch between:

- **Ollama (Local)** — runs entirely on your machine, no API cost
- **OpenAI** — GPT-4o or any OpenAI-compatible endpoint
- **Anthropic** — Claude Sonnet or any Claude model
