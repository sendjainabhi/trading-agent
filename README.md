# AlphaQuant – Your Smart Options Trading Assistant

AlphaQuant is a friendly, powerful trading assistant that does all your stock chart homework for you. It connects to live market data, analyzes price trends across multiple charts, and gives you clear, plain-English instructions on exactly what options trades to make.

No more squinting at messy charts, guessing your next move, or risking poor risk management—AlphaQuant enforces an institutional-grade **strict 1:2 Risk-to-Reward strategy** and gives you a simple green light (**ACCELERATE**) or red light (**STAND DOWN**) based on real-time market behavior.

---

## 📋 Prerequisites (What You Need First)

Before launching AlphaQuant, make sure you have these core pieces set up on your computer:

* **Java & Maven:** You need **Java 17** (or newer) and **Maven** installed to compile and run the backend code.
* **The AI Model (The Brain):** An AI model that supports **Function Calling** (the ability to use tools). If you are running your AI locally, make sure **Ollama** is active in the background with a capable model (such as `qwen3:30b` or similar).
* **MCP (Model Context Protocol) Tools:** The system relies on Spring AI's built-in function-calling protocol. This acts as the MCP layer, allowing the AI model to automatically request live data from the Java backend when you ask a question.
* **Market Data Access:** A free API key from **Finnhub** to pull live stock quotes and historical candle data.

---

## ✨ What It Does Best

* **Enforced 1:2 Risk-to-Reward Math:** The assistant is mathematically locked into a strict 1:2 risk profile. It is completely blocked from inventing random profit targets. Every single trade ensures your potential reward is exactly double your technical risk, mathematically dropping your break-even win rate to just 33.4%.
* **Automatic Multi-Timeframe Chart Checker:** The assistant looks at the 5-minute, 15-minute, and 1-hour charts all at once behind the scenes. It makes sure the short-term momentum and long-term institutional money are moving in the exact same direction before telling you to trade.
* **Smart Risk Guardrails:** Instead of using wild guesses, it uses a stock's actual volatility to set a logical baseline for your safety net (Stop-Loss), placing it right at key technical support or resistance channels.
* **Auto-Updating Dates:** It reads your computer's calendar automatically. When you ask for "next week's play," it instantly calculates the exact Friday date for the upcoming options contracts so you don't have to look it up.
* **Visual Trading Bars:** The chat window doesn't just give you text; it instantly draws a simple, color-coded visual progress bar showing your entry point, your target zone (green), and your danger zone (red) in exact 1:2 geometric proportions.
* **Anti-Lockout Protection:** It remembers recent data calculations so it doesn't waste your live data feeds, keeping your connection fast, smooth, and free from rate-limit errors.

---

## 📐 How the 1:2 Strategy Works (Under the Hood)

When you look up a stock, the system handles the math automatically depending on the direction of the market:

### For Bullish Trades (Betting the Stock Goes Up)

1. **Entry Price** = Current Stock Price
2. **Stop-Loss** = Technical Support Floor (set by your backend)
3. **Risk Unit (R)** = Entry Price − Stop-Loss
4. **Take-Profit** = Entry Price + (2 × R)

### For Bearish Trades (Betting the Stock Goes Down)

1. **Entry Price** = Current Stock Price
2. **Stop-Loss** = Technical Resistance Ceiling (set by your backend)
3. **Risk Unit (R)** = Stop-Loss − Entry Price
4. **Take-Profit** = Entry Price − (2 × R)

---

## 🛠️ How It's Built

* **The Backend:** Built with Java and Spring Boot. This handles all the heavy data lifting, math calculations, MCP tool connections, and live stock market feeds in milliseconds.
* **The Face (Frontend):** A clean, simple web page using basic HTML, CSS, and JavaScript. No messy installations or accounts required—just open it in your browser and type.

---

## 🚀 Getting Started

### 1. Add Your Secret Key

Open `src/main/resources/application.properties` and paste your free Finnhub API key:

```properties
market.provider.api-url=https://finnhub.io/api/v1/quote?symbol=
market.provider.api-key=YOUR_API_KEY_HERE
market.provider.read-timeout-seconds=5

```

### 2. Turn It On

Open your terminal or command prompt, navigate to the project folder, and run these commands to clear old files and launch the server:

```bash
mvn clean compile
mvn spring-boot:run

```

### 3. Open the App

Open your favorite web browser and go to:

```text
http://localhost:8080/index.html

```

*(Tip: Press `Ctrl + F5` or `Cmd + Shift + R` to freshly load the clean visual interface!)*

---

## 💬 Things You Can Ask It

Just type naturally into the chat box like you're talking to a friend or co-worker:

* `"Give me a top 5 market scan for next week"` — To see the best, highest-probability trades across major stocks right now using the 1:2 framework.
* `"Analyze TSLA"` — To get an instant, plain-English game plan for Tesla with custom entries, hard targets, and automated chart confirmations.
* `"What's the play for NVTS right now?"` — To see if the system gives you a green light to buy or a warning to stay on the sidelines.