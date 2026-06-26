# AlphaQuant — AI-Powered Trading Assistant

AlphaQuant is a personal AI trading assistant that connects to live market data, analyzes stocks across multiple timeframes, and gives you clear, plain-English trade plans with defined entry, target, and stop levels.

---

## ✨ Features

| Feature | Description |
|---|---|
| 📊 Multi-Timeframe Analysis | Full trade plan — trend across Daily/1h/15m/5m, RSI, VWAP, ADX, options strikes |
| 🔄 Swing Trade Detection | Swing high/low levels from 30+ bars, tested-N× count, SWING_LONG / SWING_SHORT / RANGE_PLAY |
| 🔍 11 Market Scanners | Market Scan, Bullish, Bearish, Swing, Pre-Market, Wheel Strategy, Sector Rotation, Squeeze, Earnings Plays, Failed Breakdowns, Watchlist Scan |
| 💬 Plain-English Q&A | Ask direct questions ("has AAPL trend flipped?") and get short focused answers |
| ⭐ Watchlist Sidebar | Up to 15 tickers, auto-refresh prices, click to analyze, 📊 Scan button to scan all at once |
| 🌙 Dark / Light Mode | Full theme switch, preference saved across sessions |
| 🤖 Multi-Provider AI | Ollama (local) · OpenAI · Anthropic · Google Gemini — switch live, no restart |
| 💾 Persistent Settings | Watchlist, model credentials, and theme saved server-side across restarts and incognito |
| 📐 Price Targets | IV-based expected move for tomorrow, next week, and 15-day range |
| 🐋 Smart Money Signals | Insider MSPR, analyst consensus, institutional flow |
| 🌍 Market Regime | SPY + VIX regime detection (Normal / Elevated / High Fear), auto position sizing |
| 🎯 Win Probability | Every trade card shows estimated win rate (35–82%) from 12 weighted signals |
| 📈 3-Strategy Options Ladder | Each trade shows Option A (1-leg), Option B (debit spread), Option C (credit spread) with a Recommended tag |
| ⚡ Directional Q&A | "Will TSLA go up next week?" returns a plain-English verdict, 3 reasons, probability, and what flips it — no full dashboard |
| 🔴🟢 Daily S/R Tiers | R1→R2→R3 resistance and S1→S2→S3 support (Immediate → Session → Structural) on every analysis |
| ↩️ Alt Scenario Cards | Every buy card shows a bear fallback; every sell card shows a bull fallback |
| 📋 Stock + Options in Scanners | Every scanner row shows a direct stock play (Buy/Short at support/resistance) alongside the options play |
| 🎯 Best Play Card | Scanner results include a "Best Play Right Now" recommendation with the highest-conviction setup |
| 🏷️ Ticker Badge | Analysis responses are tagged with a [SYMBOL] pill |
| 📉 TradingView Chart | Embedded interactive chart — expand / collapse per ticker |
| ⚠️ Earnings Safety | When earnings are within 10 days, trade cards automatically switch to Iron Condor / stand-aside mode |

---

## 📋 Prerequisites

### 1. Java 17+
```bash
java -version   # must be 17 or higher
```
Download from: https://adoptium.net

---

### 2. Alpaca API Key — Live Market Data

Alpaca provides free real-time US stock quotes, historical bars, and WebSocket streaming.
No deposit or brokerage account is needed — a free paper trading account is enough.

**Sign up and get your key:**

1. Go to **https://alpaca.markets** and click **Sign Up**
2. Complete email verification and log in
3. On the left sidebar click **Paper Trading**
4. In the top-right area click **Get API Keys** (or go to **Overview → API Keys**)
5. Click **Generate New Key**
6. Copy and save both values immediately — the **Key ID** and **Secret Key**
   > The Secret Key is shown only once. If you lose it, regenerate a new pair.
7. Paste them into `application.yml`:
   ```yaml
   alpaca:
     api:
       key: "PKxxxxxxxxxxxxxxxxxxxxxx"      # your Key ID
       secret: "xxxxxxxxxxxxxxxxxxxxxxxx"  # your Secret Key
   ```

**What AlphaQuant uses Alpaca for:**
- Real-time stock price quotes via WebSocket (IEX free feed)
- Historical OHLCV bars for multi-timeframe technical analysis
- Options chain data for strike recommendations

---

### 3. Finnhub API Key — Insider & Analyst Data

Finnhub provides insider trading sentiment (MSPR), analyst buy/hold/sell ratings, earnings dates, and institutional ownership data.
The free tier is completely sufficient for personal use.

**Sign up and get your key:**

1. Go to **https://finnhub.io** and click **Get free API key** (top-right)
2. Sign up with email or continue with Google
3. After login you land on the **Dashboard** — your API key is shown in the center of the page under **Your API Key**
4. Copy the key (it looks like: `d8i9xxxxxxxxxxxxxxxxxxxx`)
5. Paste it into `application.yml`:
   ```yaml
   market:
     provider:
       api-key: "d8i9xxxxxxxxxxxxxxxxxxxx"
   ```

**Free tier limits:** 60 API calls/minute, access to insider sentiment, analyst ratings, earnings calendar, and basic fundamentals — all that AlphaQuant needs.

**What AlphaQuant uses Finnhub for:**
- Insider MSPR (buy/sell sentiment ratio) on every stock analysis
- Analyst consensus (Buy / Hold / Sell counts)
- Earnings date alerts

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
sector rotation scan
show me squeeze setups
earnings plays scan
failed breakdown reversal setups
will TSLA go up next week? what are the chances?
is NVDA likely to drop from here?
```

---

## 📐 Sample Stock Analysis Output

```
AAPL ($295.65)  |  ⏳ Hold & Wait  |  ✅ Normal  |  Market Closed  |  -0.79%

Trend: Daily ↓ · 1h ↓ · 15m ↓ · 5m ↓  |  RSI 42.1 (weak)
VWAP $299.33  |  Vol 40,202,659  |  ADX 25.9 (Trending)
Key Levels: PDH $301.88 · PDL $294.22
Daily S/R — R1 $299.33 ──▶ R2 $301.88 ──▶ R3 $308.45 (structural)
           S1 $294.22 ──▶ S2 $292.00 ──▶ S3 $289.10 (structural)
Swing Support: $289.10 (3× tested)  ·  Swing Resistance: $308.45
Price Targets (IV 32.1%): Tomorrow $292–$299  ·  Next week $287–$304  ·  15-day $280–$311
Smart Money: Institutions Buying  |  Insider MSPR 45.20 (buying)  |  Analysts 35 Buy · 8 Hold

WHAT TO DO — [Buy / Sell / Hold]
⚡ Option A — Long Call / Long Put
📊 Option B — Bull Call Spread / Bear Put Spread  ← Recommended
💰 Option C — Bull Put Credit Spread / Bear Call Credit Spread
Est. Win Rate: 67% (Moderate-High)
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Backend | Java 17 · Spring Boot 3.3.4 · Spring AI 1.0.0 |
| Market Data | Alpaca API (bars, quotes, WebSocket) · Finnhub (insider, earnings, analysts) |
| Screener Data | Yahoo Finance (gainers/losers, most active, symbol search) |
| AI Providers | Ollama · OpenAI · Anthropic · Google Gemini |
| Frontend | HTML · CSS · Vanilla JavaScript · TradingView widget |
| Persistence | `~/.alphaquant/prefs.json` (server-side) |

---

## 🔒 Security Notes

- API keys entered in the Settings modal are stored **server-side only** and never returned to the browser
- Root-level `.properties` files are excluded by `.gitignore` — never commit files containing real keys
- Replace the sample Alpaca and Finnhub keys in `application.yml` with your own before use
