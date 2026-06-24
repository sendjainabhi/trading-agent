# AlphaQuant — AI-Powered Trading Assistant

AlphaQuant is a personal AI trading assistant that connects to live market data, analyzes stocks across multiple timeframes, and gives you clear, plain-English trade plans with defined entry, target, and stop levels.

---

## ✨ Features

| Feature | Description |
|---|---|
| 📊 Multi-Timeframe Analysis | Full trade plan — trend across Daily/1h/15m/5m, RSI, VWAP, ADX, options strikes |
| 🔄 Swing Trade Detection | Swing high/low levels from 30+ bars, tested-N× count, SWING_LONG / SWING_SHORT / RANGE_PLAY |
| 🔍 Six Market Scanners | Market Scan, Bullish, Bearish, Swing, Pre-Market, Wheel Strategy |
| 💬 Plain-English Q&A | Ask direct questions ("has AAPL trend flipped?") and get short focused answers |
| ⭐ Watchlist Sidebar | Up to 10 tickers, auto-refresh prices, click to analyze |
| 🌙 Dark / Light Mode | Full theme switch, preference saved across sessions |
| 🤖 Multi-Provider AI | Ollama (local) · OpenAI · Anthropic · Google Gemini — switch live, no restart |
| 💾 Persistent Settings | Watchlist, model credentials, and theme saved server-side across restarts and incognito |
| 📐 Price Targets | IV-based expected move for tomorrow, next week, and 15-day range |
| 🐋 Smart Money Signals | Insider MSPR, analyst consensus, institutional flow |
| 🌍 Market Regime | SPY + VIX regime detection (Normal / Elevated / High Fear), auto position sizing |

---

## 📋 Prerequisites

### 1. Java 17+
```bash
java -version   # must be 17 or higher
```
Download from: https://adoptium.net

---

### 2. Alpaca API Key — Live Market Data

Alpaca provides free real-time stock quotes, bars, and WebSocket streaming.

1. Sign up at **https://alpaca.markets**
2. Go to **Paper Trading** → **Overview** → **API Keys** → **Generate New Key**
3. Copy your **Key ID** and **Secret Key**
4. A free paper trading account is sufficient — no deposit required

---

### 3. Finnhub API Key — Insider & Analyst Data

Finnhub provides insider sentiment (MSPR), analyst ratings, and earnings data.

1. Sign up at **https://finnhub.io**
2. After login, your API key is shown on the **Dashboard** home screen
3. Free tier: 60 API calls/minute — sufficient for personal use

---

### 4. AI Model — At Least One Provider

| Provider | Cost | Where to get the key |
|---|---|---|
| **Ollama** (recommended, free) | Free — runs locally | Install from https://ollama.com · run `ollama pull qwen3:30b` |
| **Google Gemini** | Free tier available | https://aistudio.google.com/app/apikeys |
| **OpenAI** | Pay-per-use | https://platform.openai.com/api-keys |
| **Anthropic** | Pay-per-use | https://console.anthropic.com/settings/keys |

> **Tip:** Start with Ollama (free, no account needed) or Google Gemini (free tier). Models like `gemini-2.0-flash` or `qwen3:30b` work well for trading analysis.

---

## 🚀 Running AlphaQuant

### Option A — Run as Executable JAR (recommended)

**Step 1 — Build**
```bash
git clone https://github.com/sendjainabhi/trading-agent.git
cd trading-agent
mvn clean package -DskipTests
```

**Step 2 — Add your API keys to `application.yml`**
```bash
nano src/main/resources/application.yml
```
Find and replace the placeholder values:
```yaml
alpaca:
  api:
    key: "YOUR_ALPACA_KEY_ID"
    secret: "YOUR_ALPACA_SECRET_KEY"

market:
  provider:
    api-key: "YOUR_FINNHUB_KEY"
```

**Step 3 — Run**
```bash
java -jar target/trading-agent-0.0.1-SNAPSHOT.jar
```

**Step 4 — Open**
```
http://localhost:9090
```

**Step 5 — Set your AI model**
Click **⚙** in the top-right → enter your model provider details → **Test Connection** → **Save & Apply**.
Or upload a `model-config.sample.properties` file (see below).

---

### Option B — Run with Maven (development)

```bash
mvn spring-boot:run
```
Or on a different port:
```bash
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dserver.port=9091"
```

---

### Option C — Use the Start Script (with Cloudflare public tunnel)

```bash
./start.sh        # builds, starts app + public HTTPS tunnel at https://trading-agent.jaineo.win
./stop.sh         # stops everything
./local-start.sh  # local only, port 9091, no tunnel
```

---

### Passing keys without editing application.yml

Via command-line arguments:
```bash
java -jar trading-agent-0.0.1-SNAPSHOT.jar \
  --alpaca.api.key=YOUR_ALPACA_KEY \
  --alpaca.api.secret=YOUR_ALPACA_SECRET \
  --market.provider.api-key=YOUR_FINNHUB_KEY \
  --server.port=9090
```

Via environment variables:
```bash
export ALPACA_KEY=PKxxxxxx
export ALPACA_SECRET=xxxxxxxx
export FINNHUB_KEY=xxxxxxxx
export GOOGLE_API_KEY=AIzaSy...    # optional
export OPENAI_API_KEY=sk-...       # optional
export ANTHROPIC_API_KEY=sk-ant-.. # optional

java -jar trading-agent-0.0.1-SNAPSHOT.jar \
  --alpaca.api.key=${ALPACA_KEY} \
  --alpaca.api.secret=${ALPACA_SECRET} \
  --market.provider.api-key=${FINNHUB_KEY}
```

---

## 🤖 Switching AI Providers

Click **⚙** in the header to open Settings. Switch providers live — no restart needed.

| Provider | Model examples | Notes |
|---|---|---|
| 🦙 **Ollama** | `qwen3:30b`, `qwen3.6:35b`, `llama3:8b`, `gemma3:27b` | Free, local, no key needed |
| 🔮 **Google Gemini** | `gemini-2.0-flash`, `gemini-2.5-flash`, `gemini-2.5-pro` | Free tier at AI Studio |
| 🤖 **OpenAI** | `gpt-4o`, `gpt-4o-mini`, `gpt-4-turbo` | Pay-per-use |
| 🧠 **Anthropic** | `claude-sonnet-4-6`, `claude-opus-4-8` | Pay-per-use |

You can also upload a `.properties` config file — see `model-config.sample.properties` in this repo for the format. Upload via **⚙ → Upload Config File**.

Model credentials are saved server-side (`~/.alphaquant/prefs.json`) and survive server restarts.

---

## 💬 Example Queries

```
analyze NVDA
has AAPL trend flipped?
should I buy TSLA calls?
is MSFT above its 200-day?
pre market movers
swing trade setups
scan the market and show top options plays
what are the biggest bullish movers today?
wheel strategy scan
```

---

## 📐 Sample Stock Analysis Output

```
AAPL ($295.65)  |  ⏳ Hold & Wait  |  ✅ Normal  |  Market Closed  |  -0.79%

Trend: Daily ↓ · 1h ↓ · 15m ↓ · 5m ↓  |  RSI 42.1 (weak)
VWAP $299.33  |  Vol 40,202,659  |  ADX 25.9 (Trending)
Key Levels: PDH $301.88 · PDL $294.22
Swing Support: $289.10 (3× tested)  ·  Swing Resistance: $308.45
Price Targets (IV 32.1%): Tomorrow $292–$299  ·  Next week $287–$304  ·  15-day $280–$311
Smart Money: Institutions Buying  |  Insider MSPR 45.20 (buying)  |  Analysts 35 Buy · 8 Hold

WHAT TO DO  [Buy / Sell / Hold / Swing Long / Swing Short / Range Play]
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Backend | Java 17 · Spring Boot 3.3.4 · Spring AI 1.0.0 |
| Market Data | Alpaca API (bars, quotes, WebSocket) · Finnhub (insider, earnings, analysts) |
| Screener Data | Yahoo Finance (gainers/losers, most active, symbol search) |
| AI Providers | Ollama · OpenAI · Anthropic · Google Gemini |
| Frontend | HTML · CSS · Vanilla JavaScript |
| Persistence | `~/.alphaquant/prefs.json` (server-side) |

---

## 🔒 Security Notes

- API keys entered in the Settings modal are stored **server-side only** and never returned to the browser
- Root-level `.properties` files are excluded by `.gitignore` — never commit files containing real keys
- Replace the sample Alpaca and Finnhub keys in `application.yml` with your own before use
