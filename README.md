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

Each stock analysis returns:

```
[MARKET REGIME]  |  Bull / Bear / Neutral  |  ✅ Normal conditions

[WHAT TO DO]
📈 Strong Buy · Bull Call Debit Spread
Entry: $X  |  Target Profit: $Y  |  Stop Loss: $Z
Risk $R · Potential gain $G
Qty: ~N shares  or  ~M contract(s) (exp. YYYY-MM-DD)
```

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
