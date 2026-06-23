package com.quant.agent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.anthropic.AnthropicChatModel;
import org.springframework.ai.anthropic.AnthropicChatOptions;
import org.springframework.ai.anthropic.api.AnthropicApi;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.ai.model.tool.ToolCallingManager;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.OpenAiApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class TradingAgentService {

    private static final Logger log = LoggerFactory.getLogger(TradingAgentService.class);

    // ── Intent detection constants ────────────────────────────────────────────

    private static final Pattern TICKER_PATTERN =
            Pattern.compile("(?<![A-Za-z])\\$?([A-Z]{1,5})(?![A-Za-z])");

    // Common uppercase words that are NOT tickers — keep in sync with JS TICKER_STOP
    private static final Set<String> TICKER_STOP_WORDS = Set.of(
            "A", "I", "AM", "AN", "AT", "BE", "BY", "DO", "GO", "HI", "IF", "IN", "IS", "IT",
            "ME", "MY", "NO", "OF", "OK", "ON", "OR", "SO", "TO", "UP", "US", "WE", "AI", "VS",
            "AS", "TV", "PM", "IM", "ETF", "CEO", "CFO", "CTO", "IPO", "OTC", "SEC", "FED",
            "GDP", "IRS", "ISM", "EMA", "RSI", "ATR", "SMA", "VWAP", "ATH", "ATL",
            "AND", "ARE", "BUT", "CAN", "FOR", "GET", "HAS", "HOW", "NOW", "THE",
            "ALL", "BUY", "TOP", "RUN", "ASK", "BIG", "HIT", "SET", "USE", "ANY",
            // scanner / directive words that appear uppercase but are not tickers
            "SCAN", "SHOW", "SELL", "FIND", "GIVE", "LAST", "LIVE", "JUST",
            "WHAT", "WHEN", "WITH", "MOST", "BEST", "FROM", "HIGH", "LOW",
            "HOT", "NEW", "PRE", "POST", "MORE", "LESS", "ALSO", "OPEN",
            "YTD", "MTD", "QTD",
            // query context words falsely matched as tickers
            "TODAY", "WEEK", "MONTH", "YEAR", "DAYS", "NEXT", "OVER", "PAST",
            "PLAY", "PLAYS", "PICK", "PICKS", "MOVE", "MOVES", "MOVER", "MOVERS",
            "BULL", "BEAR", "GAIN", "LOSS", "FALL", "RISE", "DROP", "JUMP",
            "SOON", "THEN", "THEM", "THEY", "THIS", "THAT", "WILL",
            "WATCH", "LIST", "IDEA", "IDEAS", "TRADE", "TRADES", "STOCK", "STOCKS",
            "CALL", "PUTS", "LONG", "SHORT", "MARKET", "OPTION", "OPTIONS",
            "SWING", "RANGE", "SETUP", "SETUPS", "LEVEL", "LEVELS"
    );

    // Well-known company names → ticker (handles natural language like "analyze Tesla")
    private static final Map<String, String> COMPANY_TO_TICKER = Map.ofEntries(
            Map.entry("tesla",     "TSLA"),
            Map.entry("nvidia",    "NVDA"),
            Map.entry("apple",     "AAPL"),
            Map.entry("microsoft", "MSFT"),
            Map.entry("amazon",    "AMZN"),
            Map.entry("google",    "GOOGL"),
            Map.entry("alphabet",  "GOOGL"),
            Map.entry("meta",      "META"),
            Map.entry("netflix",   "NFLX"),
            Map.entry("palantir",  "PLTR"),
            Map.entry("coinbase",  "COIN"),
            Map.entry("amd",       "AMD"),
            Map.entry("intel",     "INTC"),
            Map.entry("broadcom",  "AVGO"),
            Map.entry("uber",      "UBER"),
            Map.entry("disney",    "DIS"),
            Map.entry("jpmorgan",  "JPM"),
            Map.entry("goldman",   "GS"),
            Map.entry("microstrategy", "MSTR"),
            Map.entry("rivian",    "RIVN")
    );

    // ── Provider management ───────────────────────────────────────────────────

    @Autowired
    private ToolCallingManager toolCallingManager;

    private final ChatClient.Builder ollamaClientBuilder;
    private final Map<String, ChatClient> providerClients = new ConcurrentHashMap<>();
    private volatile ChatClient activeChatClient;
    private volatile String activeProvider    = "ollama";
    private volatile String activeModel;
    private volatile float  activeTemperature = 0.0f;
    private volatile String activeApiKey      = "";
    private volatile String activeBaseUrl     = "";

    // Persisted to the workspace parent directory (one level above trading-agent/)
    private static final String SAVED_CONFIG_PATH =
            java.nio.file.Paths.get(System.getProperty("user.dir", "."))
                    .getParent().resolve("agent-config.json").toString();

    @Value("${spring.ai.ollama.base-url:http://127.0.0.1:11434}")
    private String ollamaBaseUrl;

    @Value("${spring.ai.ollama.chat.options.model:qwen3:30b}")
    private String defaultOllamaModel;

    @Value("${spring.ai.openai.api-key:}")
    private String openAiKey;

    @Value("${spring.ai.openai.base-url:https://api.openai.com}")
    private String defaultOpenAiBaseUrl;

    @Value("${spring.ai.openai.chat.options.model:gpt-4o}")
    private String defaultOpenAiModel;

    private volatile String runtimeOpenAiBaseUrl;
    private volatile String runtimeOpenAiKey;

    @Value("${spring.ai.anthropic.api-key:}")
    private String anthropicKey;

    @Value("${spring.ai.anthropic.base-url:https://api.anthropic.com}")
    private String defaultAnthropicBaseUrl;

    private volatile String runtimeAnthropicBaseUrl;
    private volatile String runtimeAnthropicKey;

    @Value("${spring.ai.anthropic.chat.options.model:claude-sonnet-4-6}")
    private String defaultAnthropicModel;

    private final HttpClient statusHttpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ObjectMapper statusMapper = new ObjectMapper();

    // ── Base rules sent on every request (~4 KB) ──────────────────────────────
    private static final String BASE_RULES = """
                    You are 'AlphaQuant', a friendly trading assistant. Explain everything like you are a knowledgeable friend helping someone understand the markets — no jargon, no acronyms unless you immediately explain them in plain words right after.

                    MANDATORY TOOL CALLING RULE:
                    You MUST call 'stockPriceFunction' for specific ticker inquiries — it returns all technical data including trend, RSI, EMA crossover, and trade setup in one call. You MUST call 'generalMarketScannerFunction' for broad scans, top options, or trending lists. You MUST call 'bearishScannerFunction' when the user asks for bearish movers, biggest losers, stocks falling, or downside plays. You MUST call 'bullishScannerFunction' when the user asks for bullish movers, biggest gainers, stocks rising, or upside plays. You MUST call 'swingScannerFunction' when the user asks for swing trades, swing scan, swing plays, range-bound stocks, consolidating stocks, or stocks near support/resistance — it detects real swing highs/lows from daily bars and requires volume > 500K. You MUST call 'preMarketScannerFunction' for pre-market queries. You MUST call 'wheelStrategyScannerFunction' when the user says 'wheel', 'wheel strategy', 'wheel scan', 'wheel picks', 'CSP', or 'sell puts for income' — never use generalMarketScannerFunction for wheel requests. Never invent data. Never call a second function for more data on the same ticker.

                    PLAIN LANGUAGE TRANSLATION RULES — apply these everywhere, every time:
                    A. INDICATOR TRANSLATION:
                       - ema_crossover_status "Bullish Cross" → "Trend just flipped UP ↑"
                       - ema_crossover_status "Bearish Cross" → "Trend just flipped DOWN ↓"
                       - ema_crossover_status "Bullish"       → "Trending UP ↑"
                       - ema_crossover_status "Bearish"       → "Trending DOWN ↓"
                       - RSI below 30   → "[value] — Stock looks oversold (possible bounce)"
                       - RSI 30–50      → "[value] — Momentum is weak, losing steam"
                       - RSI 50–70      → "[value] — Momentum is healthy"
                       - RSI above 70   → "[value] — Stock looks overbought (may pull back)"
                    B. SIGNAL STRENGTH (total_confluence_score, range -100 to +100):
                       - above +60  → "Very Strong Buy Signal ([score])"
                       - +15 to +60 → "Moderate Buy Signal ([score])"
                       - -15 to +15 → "Hold & Wait ([score])"
                       - -60 to -15 → "Moderate Sell Signal ([score])"
                       - below -60  → "Very Strong Sell Signal ([score])"
                    C. VERDICT TRANSLATION (automated_trade_verdict — use strategy_name field instead, NEVER show raw tag):
                       - EXECUTE_CALL_OR_LONG_SPREAD / PREPARE_LONG_BUY_DIP_AT_VWAP → entry timing is "execute now" or "wait for a dip"
                       - EXECUTE_PUT_OR_SHORT_SPREAD / PREPARE_SHORT_FADE_BOUNCE_AT_VWAP → entry timing is "execute now" or "wait for a bounce"
                       - STAND_DOWN_COLLECT_PREMIUM → no clear direction, collect range premium
                    D. BUY STRENGTH TRANSLATION (buy_strength — NEVER show the raw tag):
                       - STRONG_BUY  → "🔥 Strong Buy"
                       - BUY         → "✅ Buy"
                       - WATCH       → "⏳ Watch & Wait"
                       - SELL        → "⚠️ Sell Signal"
                       - STRONG_SELL → "🔴 Strong Sell"
                    E. SMART MONEY TRANSLATION (smart_money_verdict — NEVER show the raw tag):
                       - ACCUMULATING → "🐋 Institutions Buying"
                       - DISTRIBUTING → "🚨 Institutions Selling"
                       - NEUTRAL      → "😐 Institutions Neutral"
                    F. SESSION TAGS: 'PRE_MARKET' → 'Pre-Market', 'STANDARD_SESSION' → 'Open Market', 'POST_MARKET' → 'After-Hours'.
                    G. NEVER use asterisks (**) for bolding. Use only <b> HTML tags.
                    H. DYNAMIC DURATION: If the user asks for a specific timeframe (e.g., 'in 4 weeks', 'for 3 months'), pass the equivalent trading days into `customTradingDays` (1 week = 5, 4 weeks = 20, 1 month = 21, 3 months = 63, 6 months = 126). Omit custom range if no timeframe was requested.
                    I. INTENT TAGS: When the message contains an [Intent: ...] tag, call the specified function immediately.
                    J. ZERO HALLUCINATION: Every number must come verbatim from the live function payload.
                    K. SMART MONEY NOTE: smart_money_score, smart_money_verdict, insider_mspr, analyst_buy/hold/sell, and smart_money_conflict are included in the stockPriceFunction payload — already blended into total_confluence_score.
                    L. ADX TRANSLATION: adx_trend "Trending" → "Strong Trend ✅ (signals reliable)"; adx_trend "Choppy" → "No Clear Trend ⚠️ (use caution with breakout entries)".
                    M. MARKET REGIME: When market_regime contains "HIGH FEAR" always note reduced position sizing. When "BULL MARKET" confirm long bias. When "RISK-OFF" flag caution on long entries.
                    N. EARNINGS WARNING: If earnings_flag is true, always display the earnings warning prominently. Recommend straddle/strangle or standing aside for debit spread strategies.
                    O. VWAP BANDS: vwap_upper_1sd is resistance, vwap_lower_1sd is support. Price above vwap_upper_1sd = extended/overbought intraday. Price below vwap_lower_1sd = oversold intraday.
                    P. PRIOR DAY LEVELS: prior_day_high is key resistance, prior_day_low is key support — mention these as "Yesterday's High/Low" in plain English.
                    """;

    // ── Single-stock template — injected only for ticker queries (~5 KB) ─────
    private static final String STOCK_TEMPLATE = """
                    ── SINGLE-STOCK ANALYSIS TEMPLATE ───────────────────────────────────────────
                    (Use ONLY for a specific ticker. Never for scans.)

                    COLOR RULES — substitute the EXACT hex code at every <span>:
                    - Price header:  #28a745 if percent_change starts with "+", else #dc3545
                    - Trend/Verdict/Trade: #28a745 if total_confluence_score > 15 | #dc3545 if < -15 | #ffc107 if between
                    - Per-timeframe (Daily/1h/15m/5m): #28a745 bullish | #dc3545 bearish | #ffc107 flat
                    - Change %: #28a745 if starts with "+" else #dc3545
                    BOLD RULE: <b> on (1) ticker, (2) section headers, (3) verdict text, (4) trade price labels only.

                    OUTPUT FORMAT — render exactly in this order, no extra sections, no repeated data:

                    <b style="color:[price color]">[SYMBOL] ($[current_price])</b>  |  [Rule D emoji+text] ([sell_score]/6 ↓ · [buy_score]/6 ↑)  |  [regime_note]  |  [session_status]  |  <span style="color:[change color]">[percent_change]</span>[if earnings_flag is true: "  |  ⚠️ Earnings in [earnings_days_away]d ([earnings_date])"]
                    <b>Trend:</b> Daily <span style="color:[daily color]">[↑/↓/→]</span> · 1h <span style="color:[1h color]">[↑/↓/→]</span> · 15m <span style="color:[15m color]">[↑/↓/→]</span> · 5m <span style="color:[5m color]">[↑/↓/→]</span>  |  RSI [calculated_rsi_14d] ([RSI label Rule A])  |  Why: [active_sell_signals if sell > buy, else active_buy_signals — one compact sentence]
                    <b>VWAP</b> $[intraday_vwap]  |  <b>Vol</b> [volume]  |  <b>Range</b> $[micro_support]–$[micro_resistance]  |  <b>ADX</b> [adx_value 1dp] ([adx_trend][if adx_trend Choppy/No Clear Trend: " ⚠️"])
                    <b>Key Levels:</b> PDH <b>$[prior_day_high]</b>  ·  PDL <b>$[prior_day_low]</b>[if swing_support > 0: "  |  <b>Swing Support:</b> <b style='color:#28a745'>$[swing_support]</b> ([swing_support_strength]× tested)"][if swing_resistance > 0: "  ·  <b>Swing Resistance:</b> <b style='color:#dc3545'>$[swing_resistance]</b> ([swing_resistance_strength]× tested)"]
                    <b>Price Targets</b> (IV [implied_volatility]): Tomorrow $[tomorrow_lower]–$[tomorrow_upper]  ·  Next week $[next_week_lower]–$[next_week_upper]  ·  [if custom_days > 0: "[custom_days]-day" else "15-day"] $[custom_lower]–$[custom_upper]
                    <b>Watch:</b> [One concise action sentence about key break levels]
                    <b>Smart Money:</b> [Rule E]  |  Insider MSPR <b>[insider_mspr 2dp]</b> ([if > 20: "<span style='color:#28a745'>insiders buying</span>" | if < -20: "<span style='color:#dc3545'>insiders selling</span>" | else: "neutral"])  |  Analysts <b style="color:#28a745">[analyst_buy] Buy</b> · [analyst_hold] Hold · <b style="color:#dc3545">[analyst_sell] Sell</b>[if smart_money_conflict: "  ⚠️ Conflicts with chart signal"][if false+ACCUMULATING: "  <span style='color:#28a745'>✅ Agrees with chart</span>"][if false+DISTRIBUTING: "  <span style='color:#28a745'>✅ Agrees with chart</span>"][if false+NEUTRAL: "  — rely on technicals"]

                    [Output ONLY the matching block — never print the condition label:]
                    [If total_confluence_score > +15:]
                    <div class="trade-card buy">
                    <b style="color:#28a745">📈 WHAT TO DO — Strong Buy</b> · <i>[strategy_name]</i> — upside bet, fixed cost upfront, that's your max loss
                    <b>Entry:</b> <b>$[final_entry]</b>  |  <b style="color:#28a745">Target Profit:</b> <b style="color:#28a745">$[final_tp]</b>  |  <b style="color:#dc3545">Stop Loss:</b> <b style="color:#dc3545">$[final_sl]</b>
                    <span style="color:#dc3545">Risk $[final_entry−final_sl, 2dp]</span> · <span style="color:#28a745">Potential gain $[final_tp−final_entry, 2dp]</span>
                    Qty: ~[suggested_shares] shares  or  ~[suggested_contracts] contract(s) (exp. [targetExpiration])
                    [options_line]
                    </div>
                    [If total_confluence_score < −15:]
                    <div class="trade-card sell">
                    <b style="color:#dc3545">📉 WHAT TO DO — Strong Sell</b> · <i>[strategy_name]</i> — downside bet, fixed cost upfront, that's your max loss
                    <b>Entry:</b> <b>$[final_entry]</b>  |  <b style="color:#28a745">Target Profit:</b> <b style="color:#28a745">$[final_tp]</b>  |  <b style="color:#dc3545">Stop Loss:</b> <b style="color:#dc3545">$[final_sl]</b>
                    <span style="color:#dc3545">Risk $[final_sl−final_entry, 2dp]</span> · <span style="color:#28a745">Potential gain $[final_entry−final_tp, 2dp]</span>
                    Qty: ~[suggested_shares] shares  or  ~[suggested_contracts] contract(s) (exp. [targetExpiration])
                    [options_line]
                    </div>
                    [if swing_trade_signal is SWING_LONG:]
                    <div class="trade-card swing-long">
                    <b style="color:#28a745">🔄 SWING TRADE — Near Support, Long Opportunity</b>
                    [swing_note]
                    <b>Strategy:</b> <i>[swing_strategy]</i>  |  Holding: days to weeks
                    <b>Entry:</b> <b>$[swing_entry]</b>  |  <b style="color:#28a745">Target:</b> <b style="color:#28a745">$[swing_target]</b>  |  <b style="color:#dc3545">Stop:</b> <b style="color:#dc3545">$[swing_stop]</b>
                    Support tested <b>[swing_support_strength]×</b> — stronger the more times it held
                    </div>
                    [if swing_trade_signal is SWING_SHORT:]
                    <div class="trade-card swing-short">
                    <b style="color:#dc3545">🔄 SWING TRADE — Near Resistance, Short Opportunity</b>
                    [swing_note]
                    <b>Strategy:</b> <i>[swing_strategy]</i>  |  Holding: days to weeks
                    <b>Entry:</b> <b>$[swing_entry]</b>  |  <b style="color:#28a745">Target:</b> <b style="color:#28a745">$[swing_target]</b>  |  <b style="color:#dc3545">Stop:</b> <b style="color:#dc3545">$[swing_stop]</b>
                    Resistance tested <b>[swing_resistance_strength]×</b> — stronger the more times it rejected
                    </div>
                    [if swing_trade_signal is RANGE_PLAY:]
                    <div class="trade-card swing-range">
                    <b style="color:#ffc107">🔄 RANGE PLAY — Stock Stuck Between Two Levels</b>
                    [swing_note]
                    <b>Strategy:</b> <i>[swing_strategy]</i> — collect premium from both sides
                    <b>Sell Put at:</b> <b style="color:#28a745">$[swing_support]</b>  ·  <b>Sell Call at:</b> <b style="color:#dc3545">$[swing_resistance]</b>
                    Profit if price stays in range until expiry
                    </div>

                    [If between −15 and +15:]
                    <div class="trade-card hold">
                    <b style="color:#ffc107">⏳ WHAT TO DO — Hold & Wait</b> — no clear signal yet, but here's what to do when price moves:

                    <b style="color:#28a745">🟢 If price breaks above <b>$[micro_resistance]</b> → Buy</b>
                    Strategy: <i>[bullish strategy_name]</i>
                    <b>Entry:</b> <b>$[micro_resistance]</b>  |  <b style="color:#28a745">Target Profit:</b> <b style="color:#28a745">$[micro_resistance + (micro_resistance − final_sl), 2dp]</b>  |  <b style="color:#dc3545">Stop Loss:</b> <b style="color:#dc3545">$[final_sl]</b>
                    Qty: ~[suggested_shares] shares  or  ~[suggested_contracts] contract(s) (exp. [targetExpiration])

                    <b style="color:#dc3545">🔴 If price drops below <b>$[micro_support]</b> → Sell</b>
                    Strategy: <i>[bearish strategy_name]</i>
                    <b>Entry:</b> <b>$[micro_support]</b>  |  <b style="color:#28a745">Target Profit:</b> <b style="color:#28a745">$[micro_support − (final_sl − micro_support), 2dp]</b>  |  <b style="color:#dc3545">Stop Loss:</b> <b style="color:#dc3545">$[micro_resistance]</b>
                    Qty: ~[suggested_shares] shares  or  ~[suggested_contracts] contract(s) (exp. [targetExpiration])
                    </div>
                    """;

    // ── Wheel strategy template ───────────────────────────────────────────────
    private static final String WHEEL_TEMPLATE = """
                    ── WHEEL STRATEGY TEMPLATE ───────────────────────────────────────────────────
                    (Use ONLY when payload contains wheel_candidates. Show all picks immediately.)

                    WHEEL STRATEGY — How it works (show this once at the top, in plain English):
                    "You sell a put option and collect cash upfront. If the stock stays above your strike, you keep the cash and repeat. If it drops below, you buy the stock — then sell a call to collect more cash while you wait to exit."

                    <b>🎡 Wheel Strategy Picks — Top [count] picks ranked by weekly income</b>
                    Scanned: [scan_date]

                    Render ONE table with ALL candidates. No blocks, no dividers between rows.

                    <table>
                    <tr>
                      <th>Stock</th>
                      <th>Price</th>
                      <th>Type</th>
                      <th>IV</th>
                      <th>Sell Put Strike</th>
                      <th>You Collect</th>
                      <th>Weekly Income</th>
                      <th>Capital Needed</th>
                      <th>Expiry</th>
                      <th>If Assigned → Sell Call</th>
                    </tr>
                    [One <tr> per wheel_candidate:]
                    <tr>
                      <td><b>[ticker]</b></td>
                      <td><b>$[price]</b></td>
                      <td>[if is_etf: "<span style='color:#ffc107'>Leveraged ETF ⚠️</span>" else "Stock"]</td>
                      <td>[iv]%</td>
                      <td><b>$[put_strike]</b> ([pct_otm = (price−put_strike)/price×100, 1dp]% below price)</td>
                      <td><b style="color:#28a745">~$[put_premium]/share = $[total_premium_per_contract] per contract</b></td>
                      <td><b style="color:#28a745">[weekly_return_pct]%/wk</b></td>
                      <td><b>$[put_strike × 100, 0dp]</b></td>
                      <td>[expiry]</td>
                      <td><b>$[call_strike]</b> strike · collect ~<b style="color:#28a745">$[call_premium × 100, 0dp]</b> per contract</td>
                    </tr>
                    </table>

                    After the table, show one line: ⚠️ Leveraged ETF note: if assigned on any ETF, sell the covered call immediately and exit within 1–2 weeks — these decay over time.
                    """;

    // ── Swing scanner table — injected for swing/range queries ───────────────
    private static final String SWING_SCANNER_TEMPLATE = """
                    ── SWING SCANNER TABLE ──────────────────────────────────────────────────────
                    (Use ONLY when payload contains swing_scan_results. Render all rows immediately.)

                    <b>🔄 SWING TRADE SETUPS — Range-Bound & Near-Level Plays</b>
                    High-volume stocks at key support/resistance levels. Each setup includes a specific options strategy.

                    <table>
                    <tr><th>Stock</th><th>Price</th><th>Chg%</th><th>Volume</th><th>ADX</th><th>Setup</th><th>Support</th><th>Resistance</th><th>Entry</th><th>Target</th><th>Stop</th><th>Strategy</th></tr>
                    [One <tr> per swing_scan_results object:
                    - Stock: <td><span style="color:[#17a2b8 if SWING_LONG, #6f42c1 if SWING_SHORT, #fd7e14 if RANGE_PLAY]"><b>[symbol]</b></span></td>
                    - Price: <td>$[current_price]</td>
                    - Chg%: <td><span style="color:[#28a745 if percent_change starts with +, else #dc3545]">[percent_change]</span></td>
                    - Volume: <td>[volume]</td>
                    - ADX: <td>[adx_value | 0dp] <i>([<20: "Ranging", 20-25: "Weak Trend", >25: "Trending"])</i></td>
                    - Setup: <td><b>[swing_trade_signal mapped: SWING_LONG→<span style="color:#17a2b8">Near Support ↑</span>, SWING_SHORT→<span style="color:#6f42c1">Near Resistance ↓</span>, RANGE_PLAY→<span style="color:#fd7e14">Range Play ↔</span>]</b></td>
                    - Support: <td><b style="color:#28a745">$[swing_support]</b></td>
                    - Resistance: <td><b style="color:#dc3545">$[swing_resistance]</b></td>
                    - Entry: <td><b>$[swing_entry]</b></td>
                    - Target: <td><b style="color:#28a745">$[swing_target]</b></td>
                    - Stop: <td><b style="color:#dc3545">$[swing_stop]</b></td>
                    - Strategy: <td>[swing_strategy]</td>]
                    </table>
                    [swing_note for each row if available — one short sentence per stock on why the setup is valid]
                    [1-2 plain English sentences on the strongest setup and overall market context. No jargon.]
                    ---
                    """;

    // ── Market scanner table — injected only for scan queries (~3 KB) ────────
    private static final String SCANNER_TEMPLATE = """
                    ── MARKET SCANNER TABLE ──────────────────────────────────────────────────────
                    (Use ONLY when payload contains scan_results. Show all rows immediately.)

                    <b>[TODAY'S TOP TRADES — [ticker_count] Stocks Worth Watching]</b>
                    Scanned at: [processing time from System Note]
                    <table>
                    <tr><th>Stock</th><th>Price</th><th>Change</th><th>Market Hours</th><th>Direction</th><th>Signal</th><th>Enter At</th><th>Target</th><th>Exit If</th><th>Call Strike</th><th>Cover At</th><th>Expires</th></tr>
                    [One <tr> per scan_results object:
                    - Stock: <td><span style="color:[#28a745/>0 else #dc3545]"><b>[symbol]</b></span></td>
                    - Price: <td>$[current_price]</td>
                    - Change: <td><span style="color:[#28a745/+ else #dc3545]">[percent_change]</span></td>
                    - Market Hours: <td>[session_status plain English]</td>
                    - Direction: <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107]">[Buy/>15 / Sell/<-15 / Hold]</span></td>
                    - Signal: <td><span style="color:[#28a745 if total_confluence_score>0 else #dc3545]"><b>[Rule B short form e.g. "Strong Buy (+72)"]</b></span></td>
                    - Enter At: <td><b>$[final_entry]</b></td>
                    - Target: <td><b style="color:#28a745">$[final_tp]</b></td>
                    - Exit If: <td><b style="color:#dc3545">$[final_sl]</b></td>
                    - Call Strike: <td><b>$[strike_buy]</b></td>
                    - Cover At: <td><b>$[strike_sell]</b></td>
                    - Expires: <td>[target_expiration]</td>]
                    </table>
                    [1-2 plain English sentences on overall market mood. No jargon.]
                    ---
                    """;

    // ── Pre-market table — injected only for pre-market queries (~4 KB) ──────
    private static final String PRE_MARKET_TEMPLATE = """
                    ── PRE-MARKET TABLE ─────────────────────────────────────────────────────────
                    (Use ONLY when payload contains pre_market_scan_results. Show all rows immediately.)

                    <b>[PRE-MARKET MOVERS — Stocks Moving Before the Open (4:00–9:30 AM ET)]</b>
                    Scanned at: [processing time from System Note]
                    <table>
                    <tr><th>Stock</th><th>Pre-Mkt Price</th><th>Early Move</th><th>Early Volume</th><th>What It's Doing</th><th>Direction</th><th>Signal</th><th>Enter At</th><th>Target</th><th>Exit If</th><th>Call Strike</th><th>Cover At</th><th>Expires</th></tr>
                    [One <tr> per pre_market_scan_results object:
                    - Stock: <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107]"><b>[symbol]</b></span></td>
                    - Pre-Mkt Price: <td>$[current_price]</td>
                    - Early Move: <td><span style="color:[#28a745/+ else #dc3545]">[percent_change]</span></td>
                    - Early Volume: <td>[pre_market_volume]</td>
                    - What It's Doing: <td><i>[pattern plain English: "Gap & Go (Bullish)"→"Opened higher and keeps climbing", "Gap & Go (Bearish)"→"Opened lower and keeps falling", "Gap & Fade (Selling Pressure)"→"Opened higher but sellers pushing back down", "Gap & Fade (Buying Interest)"→"Opened lower but buyers stepping in", "Consolidating at Gap"→"Holding gap level", "Gap Up (Mixed)"→"Opened higher, direction unclear", "Gap Down (Mixed)"→"Opened lower, direction unclear", "Flat Drift"→"Barely moved overnight"]</i></td>
                    - Direction: <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107]">[Buy/Sell/Hold]</span></td>
                    - Signal: <td><span style="color:[#28a745 if total_confluence_score>0 else #dc3545]"><b>[Rule B short form]</b></span></td>
                    - Enter At: <td><b>$[final_entry]</b></td>
                    - Target: <td><b style="color:#28a745">$[final_tp]</b></td>
                    - Exit If: <td><b style="color:#dc3545">$[final_sl]</b></td>
                    - Call Strike: <td><b>$[strike_buy]</b></td>
                    - Cover At: <td><b>$[strike_sell]</b></td>
                    - Expires: <td>[target_expiration]</td>]
                    </table>
                    [2-3 plain English sentences: overall pre-market mood, strongest setup, best stock for the open. No jargon.]
                    ---
                    """;

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        this.ollamaClientBuilder = chatClientBuilder
                .defaultSystem(BASE_RULES + STOCK_TEMPLATE + SCANNER_TEMPLATE + PRE_MARKET_TEMPLATE + WHEEL_TEMPLATE + SWING_SCANNER_TEMPLATE)
                .defaultToolNames("stockPriceFunction", "generalMarketScannerFunction", "bearishScannerFunction", "bullishScannerFunction", "swingScannerFunction", "preMarketScannerFunction", "wheelStrategyScannerFunction")
                .defaultAdvisors(new SimpleLoggerAdvisor());
    }

    @PostConstruct
    public void initProviders() {
        this.activeModel = defaultOllamaModel;
        ChatClient ollamaClient = ollamaClientBuilder.build();
        providerClients.put("ollama", ollamaClient);
        activeChatClient = ollamaClient;
        tryInitOpenAi(openAiKey, defaultOpenAiModel, defaultOpenAiBaseUrl);
        tryInitAnthropic(anthropicKey, defaultAnthropicModel, defaultAnthropicBaseUrl);
        loadSavedConfig();
    }

    private void loadSavedConfig() {
        File f = new File(SAVED_CONFIG_PATH);
        if (!f.exists()) return;
        try {
            Map<String, String> config = statusMapper.readValue(f, new TypeReference<Map<String, String>>() {});
            if (!config.isEmpty()) {
                updateModelConfig(config);
                log.info("Restored model config from {}", SAVED_CONFIG_PATH);
            }
        } catch (Exception e) {
            log.warn("Could not load saved config from {}: {}", SAVED_CONFIG_PATH, e.getMessage());
        }
    }

    private void saveCurrentConfig() {
        try {
            Map<String, String> config = new HashMap<>();
            config.put("provider",    activeProvider);
            config.put("model",       activeModel);
            config.put("apiKey",      activeApiKey);
            config.put("baseUrl",     activeBaseUrl);
            config.put("temperature", String.valueOf(activeTemperature));
            statusMapper.writerWithDefaultPrettyPrinter().writeValue(new File(SAVED_CONFIG_PATH), config);
            log.info("Saved model config — provider: {}, model: {}", activeProvider, activeModel);
        } catch (Exception e) {
            log.warn("Could not save config to {}: {}", SAVED_CONFIG_PATH, e.getMessage());
        }
    }

    public Map<String, Object> testProviderConfig(Map<String, String> config) {
        String provider = config.getOrDefault("provider", "").toLowerCase();
        String apiKey   = config.getOrDefault("apiKey",   "");
        String baseUrl  = config.getOrDefault("baseUrl",  "");

        if ("ollama".equals(provider)) {
            String url = isValidUrl(baseUrl) ? baseUrl : ollamaBaseUrl;
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url + "/api/tags"))
                        .timeout(Duration.ofSeconds(5))
                        .GET().build();
                int status = statusHttpClient.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
                return status == 200
                        ? Map.of("connected", true)
                        : Map.of("connected", false, "error", "Ollama returned HTTP " + status);
            } catch (Exception e) {
                return Map.of("connected", false, "error", "Cannot reach Ollama at " + url + ": " + e.getMessage());
            }
        }

        if ("openai".equals(provider) || "anthropic".equals(provider)) {
            // Fall back to stored runtime key when form left the key field blank (e.g. after file upload)
            String resolvedKey = isValidKey(apiKey) ? apiKey
                    : ("openai".equals(provider) ? runtimeOpenAiKey : runtimeAnthropicKey);
            String resolvedUrl = isValidUrl(baseUrl) ? baseUrl
                    : ("openai".equals(provider) ? runtimeOpenAiBaseUrl : runtimeAnthropicBaseUrl);
            if (!isValidKey(resolvedKey)) return Map.of("connected", false, "error", "API key is required");
            if (!isValidUrl(resolvedUrl)) return Map.of("connected", false, "error", "Base URL is required");
            String model = config.getOrDefault("model", "");
            if (model.isBlank()) return Map.of("connected", false, "error", "Model name is required");

            // Send a minimal chat completion to verify BOTH connectivity and model availability.
            // GET /v1/models is unreliable on enterprise proxies — they often return 404/405 even
            // when the chat endpoint works fine, so this POST gives a definitive answer.
            try {
                String chatUrl = resolvedUrl.endsWith("/")
                        ? resolvedUrl + "v1/chat/completions"
                        : resolvedUrl + "/v1/chat/completions";
                String safeModel = model.replace("\\", "\\\\").replace("\"", "\\\"");
                String body = "{\"model\":\"" + safeModel + "\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":1}";
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(chatUrl))
                        .header("Authorization", "Bearer " + resolvedKey)
                        .header("Content-Type", "application/json")
                        .header("Accept", "application/json")
                        .timeout(Duration.ofSeconds(20))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
                HttpResponse<String> resp = statusHttpClient.send(req, HttpResponse.BodyHandlers.ofString());
                int status = resp.statusCode();
                if (status == 200 || status == 201) return Map.of("connected", true);
                if (status == 401) return Map.of("connected", false, "error", "Invalid API key (401 Unauthorized)");
                if (status == 403) return Map.of("connected", false, "error", "Access forbidden (403)");
                if (status == 429) return Map.of("connected", false, "error", "Rate limit exceeded — try again later");
                if (status == 404) {
                    String respBody = resp.body() != null ? resp.body() : "";
                    if (respBody.contains("model_not_found")) {
                        return Map.of("connected", false, "error",
                                "Model '" + model + "' not found — verify the model name with your provider");
                    }
                    return Map.of("connected", false, "error",
                            "Endpoint not found (404) — verify the base URL");
                }
                return Map.of("connected", false, "error", "Server returned HTTP " + status);
            } catch (Exception e) {
                return Map.of("connected", false, "error", "Connection error: " + e.getMessage());
            }
        }

        return Map.of("connected", false, "error", "Unknown provider: " + provider);
    }

    public Map<String, Object> testProviderConnection() {
        if ("ollama".equals(activeProvider)) {
            boolean ok = checkOllamaReachable();
            return ok ? Map.of("connected", true)
                      : Map.of("connected", false, "error", "Cannot reach Ollama at " + ollamaBaseUrl);
        }
        if (!providerClients.containsKey(activeProvider)) {
            return Map.of("connected", false, "error",
                    "No API key configured — upload a config file or enter the key in settings");
        }
        String baseUrl = "openai".equals(activeProvider) ? runtimeOpenAiBaseUrl : runtimeAnthropicBaseUrl;
        String key     = "openai".equals(activeProvider) ? runtimeOpenAiKey     : runtimeAnthropicKey;
        if (baseUrl == null || key == null) {
            return Map.of("connected", false, "error", "Provider not fully configured");
        }
        try {
            String testUrl = baseUrl.endsWith("/") ? baseUrl + "v1/models" : baseUrl + "/v1/models";
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(testUrl))
                    .header("Authorization", "Bearer " + key)
                    .header("Accept", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .GET().build();
            int status = statusHttpClient.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
            if (status == 200) return Map.of("connected", true);
            if (status == 401) return Map.of("connected", false, "error", "Invalid API key (401 Unauthorized)");
            if (status == 403) return Map.of("connected", false, "error", "Access forbidden (403)");
            // Some OpenAI-compatible proxies don't expose /v1/models — treat 404/405 as "likely OK"
            if (status == 404 || status == 405)
                return Map.of("connected", true, "warning",
                        "Models list not available (" + status + ") — chat should still work");
            return Map.of("connected", false, "error", "Server returned HTTP " + status);
        } catch (Exception e) {
            return Map.of("connected", false, "error", "Connection error: " + e.getMessage());
        }
    }

    // ── Provider initialisation helpers ──────────────────────────────────────

    private ChatClient wrapWithDefaults(ChatClient.Builder b) {
        return b.defaultSystem(BASE_RULES + STOCK_TEMPLATE + SCANNER_TEMPLATE + PRE_MARKET_TEMPLATE + WHEEL_TEMPLATE + SWING_SCANNER_TEMPLATE)
                .defaultToolNames("stockPriceFunction", "generalMarketScannerFunction", "bearishScannerFunction", "bullishScannerFunction", "swingScannerFunction", "preMarketScannerFunction", "wheelStrategyScannerFunction")
                .defaultAdvisors(new SimpleLoggerAdvisor())
                .build();
    }

    private void tryInitOpenAi(String key, String model, String baseUrl) {
        if (!isValidKey(key)) return;
        try {
            String resolvedUrl = isValidUrl(baseUrl) ? baseUrl : defaultOpenAiBaseUrl;
            OpenAiApi api = OpenAiApi.builder()
                    .apiKey(key)
                    .baseUrl(resolvedUrl)
                    .build();
            OpenAiChatOptions options = OpenAiChatOptions.builder()
                    .model(model)
                    .temperature(0.0)
                    .build();
            OpenAiChatModel chatModel = OpenAiChatModel.builder()
                    .openAiApi(api)
                    .defaultOptions(options)
                    .toolCallingManager(toolCallingManager)
                    .build();
            providerClients.put("openai", wrapWithDefaults(ChatClient.builder(chatModel)));
            runtimeOpenAiBaseUrl = resolvedUrl;
            runtimeOpenAiKey = key;
            log.info("OpenAI provider ready — model: {}, url: {}", model, resolvedUrl);
        } catch (Exception e) {
            log.warn("OpenAI init failed: {}", e.getMessage());
        }
    }

    private void tryInitAnthropic(String key, String model, String baseUrl) {
        if (!isValidKey(key)) return;
        try {
            String resolvedUrl = isValidUrl(baseUrl) ? baseUrl : defaultAnthropicBaseUrl;
            AnthropicApi api = AnthropicApi.builder()
                    .apiKey(key)
                    .baseUrl(resolvedUrl)
                    .build();
            AnthropicChatOptions options = AnthropicChatOptions.builder()
                    .model(model)
                    .temperature(0.0)
                    .build();
            AnthropicChatModel chatModel = AnthropicChatModel.builder()
                    .anthropicApi(api)
                    .defaultOptions(options)
                    .toolCallingManager(toolCallingManager)
                    .build();
            providerClients.put("anthropic", wrapWithDefaults(ChatClient.builder(chatModel)));
            runtimeAnthropicBaseUrl = resolvedUrl;
            runtimeAnthropicKey = key;
            log.info("Anthropic provider ready — model: {}, url: {}", model, baseUrl);
        } catch (Exception e) {
            log.warn("Anthropic init failed: {}", e.getMessage());
        }
    }

    private static boolean isValidUrl(String url) {
        return url != null && !url.isBlank() && (url.startsWith("http://") || url.startsWith("https://"));
    }

    private static boolean isValidKey(String key) {
        return key != null && !key.isBlank() && !key.startsWith("${");
    }

    // ── Model management (called by TradingAgentController) ───────────────────

    public Map<String, Object> getModelStatus() {
        Map<String, Object> r = new HashMap<>();
        r.put("provider", activeProvider);
        r.put("model", activeModel);
        r.put("temperature", activeTemperature);
        r.put("connected", checkConnected());
        r.put("availableProviders", new ArrayList<>(providerClients.keySet()));
        r.put("ollamaBaseUrl", ollamaBaseUrl);
        r.put("openAiBaseUrl", runtimeOpenAiBaseUrl != null ? runtimeOpenAiBaseUrl : defaultOpenAiBaseUrl);
        r.put("anthropicBaseUrl", runtimeAnthropicBaseUrl != null ? runtimeAnthropicBaseUrl : defaultAnthropicBaseUrl);
        if ("ollama".equals(activeProvider)) r.put("ollamaModels", getOllamaModels());
        return r;
    }

    public Map<String, Object> getModelConfig() {
        Map<String, Object> r = new HashMap<>();
        r.put("provider", activeProvider);
        r.put("model", activeModel);
        r.put("temperature", activeTemperature);
        r.put("ollamaBaseUrl", ollamaBaseUrl);
        r.put("openAiBaseUrl", runtimeOpenAiBaseUrl != null ? runtimeOpenAiBaseUrl : defaultOpenAiBaseUrl);
        r.put("anthropicBaseUrl", runtimeAnthropicBaseUrl != null ? runtimeAnthropicBaseUrl : defaultAnthropicBaseUrl);
        r.put("availableProviders", new ArrayList<>(providerClients.keySet()));
        r.put("openAiConfigured", providerClients.containsKey("openai"));
        r.put("anthropicConfigured", providerClients.containsKey("anthropic"));
        return r;
    }

    public synchronized Map<String, Object> updateModelConfig(Map<String, String> config) {
        String provider = config.getOrDefault("provider", activeProvider).toLowerCase();
        String model    = config.getOrDefault("model", activeModel);
        String apiKey   = config.getOrDefault("apiKey", "");
        String baseUrl  = config.getOrDefault("baseUrl", "");
        float  temp     = parseFloat(config.get("temperature"), activeTemperature);

        if (!List.of("ollama", "openai", "anthropic").contains(provider)) {
            return Map.of("success", false, "error", "Unknown provider: " + provider);
        }

        // Reinitialise when a new key is supplied, or when the base URL changes
        if (!apiKey.isBlank() || !baseUrl.isBlank()) {
            if ("openai".equals(provider))
                tryInitOpenAi(apiKey.isBlank() ? openAiKey : apiKey, model,
                              baseUrl.isBlank() ? defaultOpenAiBaseUrl : baseUrl);
            if ("anthropic".equals(provider))
                tryInitAnthropic(apiKey.isBlank() ? anthropicKey : apiKey, model,
                                 baseUrl.isBlank() ? defaultAnthropicBaseUrl : baseUrl);
        }

        if (!providerClients.containsKey(provider)) {
            return Map.of("success", false, "error",
                    "Provider '" + provider + "' is not configured. Provide an API key.");
        }

        activeProvider    = provider;
        activeModel       = model;
        activeTemperature = temp;
        activeApiKey      = switch (provider) {
            case "openai"    -> runtimeOpenAiKey     != null ? runtimeOpenAiKey     : "";
            case "anthropic" -> runtimeAnthropicKey  != null ? runtimeAnthropicKey  : "";
            default          -> "";
        };
        activeBaseUrl     = switch (provider) {
            case "openai"    -> runtimeOpenAiBaseUrl    != null ? runtimeOpenAiBaseUrl    : defaultOpenAiBaseUrl;
            case "anthropic" -> runtimeAnthropicBaseUrl != null ? runtimeAnthropicBaseUrl : defaultAnthropicBaseUrl;
            default          -> ollamaBaseUrl;
        };
        activeChatClient  = providerClients.get(provider);
        saveCurrentConfig();

        return Map.of("success", true, "provider", activeProvider,
                "model", activeModel, "connected", checkConnected(),
                "temperature", activeTemperature);
    }

    private boolean checkConnected() {
        return switch (activeProvider) {
            case "openai"    -> providerClients.containsKey("openai");
            case "anthropic" -> providerClients.containsKey("anthropic");
            default -> checkOllamaReachable();
        };
    }

    private boolean checkOllamaReachable() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(ollamaBaseUrl + "/api/tags"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            return statusHttpClient.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private List<String> getOllamaModels() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(ollamaBaseUrl + "/api/tags"))
                    .timeout(Duration.ofSeconds(5))
                    .GET().build();
            HttpResponse<String> res = statusHttpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return List.of();
            JsonNode models = statusMapper.readTree(res.body()).path("models");
            List<String> names = new ArrayList<>();
            for (JsonNode m : models) names.add(m.path("name").asText());
            return names;
        } catch (Exception e) {
            return List.of();
        }
    }

    private static float parseFloat(String val, float fallback) {
        if (val == null || val.isBlank()) return fallback;
        try { return Float.parseFloat(val); } catch (NumberFormatException e) { return fallback; }
    }

    // ── Intent detection ──────────────────────────────────────────────────────

    /**
     * Extracts the first plausible ticker from the input.
     * Checks company name aliases first, then scans for uppercase symbol tokens.
     */
    private String extractTicker(String raw) {
        String lower = raw.toLowerCase(Locale.US);
        for (Map.Entry<String, String> entry : COMPANY_TO_TICKER.entrySet()) {
            if (lower.contains(entry.getKey())) return entry.getValue();
        }
        Matcher m = TICKER_PATTERN.matcher(raw.toUpperCase(Locale.US));
        while (m.find()) {
            String candidate = m.group(1);
            if (candidate.length() >= 2 && !TICKER_STOP_WORDS.contains(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * Classifies the user's raw input and appends an [Intent:] routing hint so the LLM
     * calls the correct function without guessing from ambiguous phrasing.
     */
    private String enrichPrompt(String raw) {
        String lower = raw.toLowerCase(Locale.US);

        // 0. Wheel strategy intent — check before everything else
        if (lower.matches(".*\\b(wheel|csp|cash.?secured.?put|sell.?put.*income|put.*sell.*income|wheel.?scan|wheel.?pick|wheel.?strategy).*")) {
            log.info("[INTENT] wheelStrategy");
            return raw + "\n\n[Intent: Call wheelStrategyScannerFunction — user wants wheel strategy picks]";
        }

        // 1. Pre-market intent — most specific, check first
        if (lower.matches(".*\\b(pre.?market|premarket|before.?open|gap.?up|gap.?down|gap.?play|gapping|early.?mover|overnight.?move|pm.?scan|pm.?mover).*")
                || lower.matches(".*(what.*(moving|movin|gapping|active).*(pre|before|early|overnight)).*")) {
            log.info("[INTENT] preMarketScanner");
            return raw + "\n\n[Intent: Call preMarketScannerFunction — user wants pre-market movers and gap patterns]";
        }

        // 2. Ticker extraction — happens after pre-market so stop-words filter runs cleanly
        String ticker = extractTicker(raw);

        // 3a. Bearish movers
        if (ticker == null && lower.matches(".*\\b(bear|bearish|loser|losing|falling|drop|biggest.?down|downside|sell.?mover|short.?play|most.?bearish|worst.?performer|biggest.?fall|biggest.?loss).*")) {
            log.info("[INTENT] bearishScanner");
            return raw + "\n\n[Intent: Call bearishScannerFunction — user wants biggest bearish movers]";
        }

        // 3b. Bullish movers
        if (ticker == null && lower.matches(".*\\b(bull|bullish|gainer|gaining|rising|biggest.?up|upside|buy.?mover|most.?bullish|best.?performer|biggest.?gain|biggest.?rise|biggest.?winner).*")) {
            log.info("[INTENT] bullishScanner");
            return raw + "\n\n[Intent: Call bullishScannerFunction — user wants biggest bullish movers]";
        }

        // 3c. Swing scanner — range-bound setups and stocks near swing support/resistance
        if (ticker == null && lower.matches(".*\\b(swing|swing.?trade|swing.?scan|swing.?play|swing.?pick|swing.?setup|consolidat|range.?play|range.?bound|range.?stock|near.?support|near.?resistance|at.?support|at.?resistance|ranging|channel.?play|price.?range).*")) {
            log.info("[INTENT] swingScanner");
            return raw + "\n\n[Intent: Call swingScannerFunction — user wants swing trade setups at key levels]";
        }

        // 3d. General scanner — wins when no specific ticker is present
        if (ticker == null && lower.matches(".*\\b(scan|scanner|market.?mover|most.?active|trending|top.?pick|hot.?stock|broad.?scan|watch.?list|top.?option|what.*trade|what.*buy|what.*play|what.*watch|movers?.?today|what.*moving).*")) {
            log.info("[INTENT] generalMarketScanner");
            return raw + "\n\n[Intent: Call generalMarketScannerFunction — user wants a broad market scan]";
        }

        // 4. Trend / technicals with a specific ticker
        if (ticker != null && lower.matches(".*\\b(trend|rsi|ema|technical|chart|signal|momentum|macd|sma).*")) {
            log.info("[INTENT] stockPrice ticker={} (technicals)", ticker);
            return raw + "\n\n[Intent: Call stockPriceFunction for " + ticker + " and render Dashboard Output Template — user wants technical indicators]";
        }

        // 5. Bare ticker — user typed just a symbol (or symbol + a few words)
        if (ticker != null && raw.trim().length() <= ticker.length() + 25) {
            log.info("[INTENT] stockPrice ticker={} (bare)", ticker);
            return raw + "\n\n[Intent: Call stockPriceFunction for " + ticker + " and render Dashboard Output Template]";
        }

        // 6. Named ticker with analysis intent
        if (ticker != null && lower.matches(".*\\b(analyze|analysis|look at|check|price|trade|buy|sell|option|call|put|short|long|what.*(doing|think|say|look)).*")) {
            log.info("[INTENT] stockPrice ticker={} (analysis)", ticker);
            return raw + "\n\n[Intent: Call stockPriceFunction for " + ticker + " and render Dashboard Output Template]";
        }

        log.info("[INTENT] none — ticker={} raw routing", ticker);
        return raw;
    }

    // ── Request pipeline ──────────────────────────────────────────────────────

    private String injectDynamicContext(String input) {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("America/New_York"));
        String timeStamp   = now.format(DateTimeFormatter.ofPattern("hh:mm:ss a z"));
        String currentDate = now.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));
        String enriched    = enrichPrompt(input);
        return "/no_think\n" + enriched + "\n\n[System Note: Request processed at " + timeStamp + " on " + currentDate + ".]";
    }

    public Flux<String> streamAgentResponse(String input) {
        return Flux.concat(
            Flux.just("__PROGRESS__:Fetching live market data..."),
            Flux.defer(() -> {
                ChatClient client = activeChatClient != null
                        ? activeChatClient
                        : providerClients.get("ollama");
                if (client == null) {
                    return Flux.just("### Configuration Error\nNo AI provider is configured. Open Settings to set up a model.");
                }

                log.info("[REQUEST] provider={} model={} input=\"{}\"",
                        activeProvider, activeModel,
                        input.length() > 120 ? input.substring(0, 120) + "…" : input);

                long t0 = System.currentTimeMillis();
                var promptSpec = client.prompt().user(injectDynamicContext(input));
                String response = switch (activeProvider) {
                    case "openai"    -> promptSpec.options(
                                            OpenAiChatOptions.builder().model(activeModel).build()
                                        ).call().content();
                    case "anthropic" -> promptSpec.options(
                                            AnthropicChatOptions.builder().model(activeModel).maxTokens(8096).build()
                                        ).call().content();
                    default          -> promptSpec.call().content();
                };
                log.info("[RESPONSE] provider={} model={} elapsed={}ms chars={}",
                        activeProvider, activeModel,
                        System.currentTimeMillis() - t0,
                        response == null ? 0 : response.length());

                if (response == null || response.isBlank()) {
                    return Flux.just("### Pipeline Delay\nMarket processing streams returned empty data frames.");
                }
                return Flux.just(response);
            }).subscribeOn(Schedulers.boundedElastic())
        ).onErrorResume(e -> {
            log.error("[ERROR] provider={} model={} error={}", activeProvider, activeModel, e.getMessage());
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (msg.contains("model_not_found") || msg.contains("model not found")) {
                return Flux.just("### Model Not Found\n" +
                        "The model **" + activeModel + "** was not found at the configured endpoint.\n\n" +
                        "**To fix:** click ⚙ → Edit → update **Model Name** → Test Connection → Save & Apply.");
            }
            if (msg.contains("401") || msg.contains("Unauthorized")) {
                return Flux.just("### Authentication Failed\n" +
                        "Your API key was rejected. Click ⚙ → Edit and update the API key.");
            }
            if (msg.contains("403") || msg.contains("Forbidden")) {
                return Flux.just("### Access Denied\n" +
                        "The endpoint returned 403 Forbidden. Your account may not have access to this model or endpoint.");
            }
            if (msg.contains("Connection refused") || msg.contains("connect timed out") || msg.contains("UnknownHost")) {
                return Flux.just("### Connection Failed\n" +
                        "Cannot reach the provider. Click ⚙ → Edit and verify the **Endpoint URL**.");
            }
            return Flux.just("### Error\n" + msg);
        });
    }
}
