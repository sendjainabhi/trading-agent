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

    @Autowired
    private TechnicalAnalysisService scannerService;

    @Autowired
    private MarketPulseService marketPulseService;

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
                    .getParent().resolve("local-persistance/agent-config.json").toString();

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

    // Google Gemini — uses OpenAI-compatible endpoint (no extra Maven dependency needed)
    @Value("${GOOGLE_API_KEY:}")
    private String googleApiKey;

    private static final String DEFAULT_GOOGLE_BASE_URL  = "https://generativelanguage.googleapis.com/v1beta/openai";
    private static final String DEFAULT_GOOGLE_MODEL     = "gemini-2.0-flash";

    private volatile String runtimeGoogleKey;
    private volatile String runtimeGoogleBaseUrl;

    private final HttpClient statusHttpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ObjectMapper statusMapper = new ObjectMapper();

    // ── Base rules sent on every request (~4 KB) ──────────────────────────────
    private static final String BASE_RULES = """
                    You are 'AlphaQuant', a friendly trading assistant. Explain everything like you are a knowledgeable friend helping someone understand the markets — no jargon, no acronyms unless you immediately explain them in plain words right after.

                    MANDATORY TOOL CALLING RULE:
                    You MUST call 'stockPriceFunction' for specific ticker inquiries — it returns all technical data including trend, RSI, EMA crossover, and trade setup in one call. You MUST call 'generalMarketScannerFunction' for broad scans, top options, or trending lists. You MUST call 'bearishScannerFunction' when the user asks for bearish movers, biggest losers, stocks falling, or downside plays. You MUST call 'bullishScannerFunction' when the user asks for bullish movers, biggest gainers, stocks rising, or upside plays. You MUST call 'swingScannerFunction' when the user asks for swing trades, swing scan, swing plays, range-bound stocks, consolidating stocks, or stocks near support/resistance — it detects real swing highs/lows from daily bars and requires volume > 500K. You MUST call 'preMarketScannerFunction' for pre-market queries. You MUST call 'wheelStrategyScannerFunction' when the user says 'wheel', 'wheel strategy', 'wheel scan', 'wheel picks', 'CSP', or 'sell puts for income' — never use generalMarketScannerFunction for wheel requests. You MUST call 'sectorRotationScannerFunction' when the user asks about sector rotation, where money is flowing, which sectors are leading or lagging, or sector ETF rankings. You MUST call 'squeezeScannerFunction' when the user asks about squeeze setups, volatility coils, stocks about to break out, or compression plays. You MUST call 'earningsPlaysScannerFunction' when the user asks about earnings plays, stocks near earnings, pre-earnings setups, or IV crush opportunities. You MUST call 'failedBreakdownScannerFunction' when the user asks about failed breakdowns, reversal setups, bullish divergence plays, or snap-back trades. You MUST call 'watchlistScannerFunction' when the user asks to scan their watchlist or scan specific listed stocks — pass the tickers as a comma-separated string. Never invent data. Never call a second function for more data on the same ticker.

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
                    Q. DIRECTIONAL QUESTIONS — triggers: "go up/down?", "bullish/bearish?", "should I buy/sell?", "next week/month/tomorrow", "chances?", "likely?", "will it fall/rise?", "good time to buy?", "going lower/higher?", "what are the odds?" — SKIP the full analysis template entirely. Output ONLY this compact plain-English block:
                       VERDICT RULE — derive the verdict STRICTLY from total_confluence_score, NEVER from the direction the user asked about:
                         • total_confluence_score > +15 → verdict is "Most likely going UP" (color #28a745)
                         • total_confluence_score < -15 → verdict is "Most likely going DOWN" (color #dc3545)
                         • between -15 and +15 → verdict is "Too early to call — mixed signals" (color #ffc107)
                       The user may ask "will it go down?" — if the score is bullish you MUST still say "Most likely going UP." Never invert the verdict to match what the user asked.
                       <b style="color:[derived color above]">[SYMBOL] — [derived verdict above]</b>
                       <b>Why:</b>
                       • [Strongest reason from the data — verify each claim against the actual numbers e.g. if price > intraday_vwap say "above VWAP" not "below VWAP"]
                       • [Second reason]
                       • [Third reason]
                       ACCURACY CHECK before writing Why bullets: (1) if current_price > intraday_vwap → price is ABOVE VWAP; (2) if total_confluence_score > 0 → bias is bullish; (3) win_probability reflects the BULLISH trade succeeding — if user asked about downside, the down probability = 100 − win_probability.
                       <b>Key levels:</b> Support <b style="color:#28a745">$[micro_support]</b> (below this = more downside) · Resistance <b style="color:#dc3545">$[micro_resistance]</b> (above this = setup flips)
                       [If question mentions a timeframe like "next week", "tomorrow", "this week": "<b>[timeframe] price range:</b> $[relevant_lower]–$[relevant_upper] based on current IV"]
                       <b>Chances of [the direction user asked about]:</b> [if user asked about UP direction: "<span style='color:[#28a745 if win_probability>=65, #ffc107 if >=52, #dc3545 if <52]'><b>~[win_probability]%</b> ([win_probability_label])</span>" | if user asked about DOWN direction: "<span style='color:[#28a745 if (100-win_probability)>=65, #ffc107 if >=52, #dc3545 if <52]'><b>~[100−win_probability]%</b> ([recompute label: High if >=65, Moderate if >=52, Low if <52])</span>"]
                       <b>What changes this:</b> [one plain sentence]
                       Do NOT output any HTML trade cards, VWAP line, Trend line, Key Levels section, dashboard header, or full analysis template for these questions.
                    R. WIN PROBABILITY DISPLAY — in every trade card (buy, sell, swing), always show the win probability as the last line before </div>:
                       <b>Est. Win Rate:</b> <span style="color:[#28a745 if win_probability>=65, #ffc107 if >=50, #dc3545 if <50]">[win_probability]%</span> — <i>[win_probability_label]</i>
                    S. BANNED OUTPUT LINES — NEVER generate any of these lines, they add no value:
                       - "Trend Quality: ..." — those fields are ONLY for composing the Outlook sentence. Never output them as a separate line.
                       - "Watch: ..." — this line is removed from the template. Never generate it.
                       Raw metric dumps (weekly_bias, ma_stack_score, market_breadth, composite_trend_score as separate labeled lines) are strictly forbidden.
                    T. HEADER EMOJI: Rule D buy_strength emoji MUST match exactly — STRONG_BUY→🔥, BUY→✅, WATCH→⏳, SELL→⚠️, STRONG_SELL→🔴. NEVER substitute ↑/↓/📈/📉 for buy_strength; those are only for price change direction.
                    U. S/R CHAIN RULE: In the Daily S/R section R1 < R2 < R3 (strictly ascending, all above current_price) and S1 > S2 > S3 (strictly descending, all below current_price). If any candidate value is on the wrong side of current_price, or would invert the chain, skip it and use the next available candidate from the same source set. Never write a level that breaks the chain.
                    V. MACD DIVERGENCE: macd_divergence field — "BEARISH_DIV" means price rising but MACD histogram declining (hidden distribution, warn of potential reversal). "BULLISH_DIV" means price falling but MACD histogram rising (hidden accumulation, potential bounce). "NONE" = no divergence. Mention when not NONE.
                    W. PATTERNS: daily_candle_pattern values — HAMMER (bullish reversal), BULLISH_ENGULFING (strong bullish), BEARISH_ENGULFING (strong bearish), SHOOTING_STAR (bearish reversal), DOJI (indecision), BULLISH_MARUBOZU (strong bull with no wicks), BEARISH_MARUBOZU (strong bear with no wicks). chart_pattern values — NR7 (7-bar narrowest range, breakout imminent), INSIDE_BAR (consolidation), BULL_FLAG (bullish continuation), BEAR_FLAG (bearish continuation). Always mention these when not NONE. Translate to plain English.
                    X. RVOL (Relative Volume): rvol field — compares today's volume to 10-day average, time-adjusted. rvol >= 3.0 → "Extremely heavy volume 🔥🔥"; rvol >= 2.0 → "Heavy volume 🔥 (institutional activity likely)"; rvol >= 1.2 → "Above-average volume"; rvol < 0.8 → "Light volume — move may lack conviction". Always mention in analysis.
                    Y. BOLLINGER SQUEEZE: bbw_squeeze true = Bollinger Bands are exceptionally tight (BBW < 5%) — volatility compressed, sharp directional break coming soon. Mention as "🗜️ Volatility Squeeze — breakout imminent". bbw_percent shows exact band width as % of middle band. vwap_zscore shows how many standard deviations price is from VWAP — above +2σ = extended, below -2σ = oversold intraday.
                    Z. PUT/CALL RATIO (single stock): put_call_ratio field — ratio of put to call volume on nearest expiry. >1.2 = bearish sentiment (heavy put buying 🔴); 0.8–1.2 = neutral; <0.7 = bullish sentiment (call buying dominant 🟢). Mention when available (> 0).
                    AA. SECTOR RELATIVE STRENGTH: sector_etf field shows which sector ETF covers this stock (e.g. "XLK" for tech). sector_rs is the sector ETF's 20-day return minus SPY's 20-day return — how much the sector beat or lagged the market. sector_trend values: STRONG_INFLOW (sector beating SPY by >3%) → 🟢 strong tailwind; INFLOW (>1%) → mild tailwind; STRONG_OUTFLOW (lagging by >3%) → 🔴 strong headwind; OUTFLOW (>1%) → mild headwind; NEUTRAL → no sector edge. When sector_trend is STRONG_OUTFLOW or OUTFLOW, always warn that the sector headwind may limit upside even on a bullish technical setup. When STRONG_INFLOW, note that sector momentum is confirming the trade. Mention sector_etf by name (e.g. "XLK outperforming SPY"). Only mention if sector_etf is not empty.
                    """;

    // ── Single-stock template — injected only for ticker queries ─────────────
    private static final String STOCK_TEMPLATE = """
                    ── SINGLE-STOCK ANALYSIS TEMPLATE ───────────────────────────────────────────
                    (Use ONLY for a specific ticker. Never for scans.)

                    COLOR RULES:
                    - Price header: #28a745 if percent_change starts with "+", else #dc3545
                    - Trade/Signal: #28a745 if total_confluence_score > 15 | #dc3545 if < −15 | #ffc107 if between
                    - Per-timeframe arrows: #28a745 bullish | #dc3545 bearish | #ffc107 flat
                    BOLD RULE: <b> on ticker, section headers, verdict text, price labels only. Never use asterisks.

                    OUTPUT FORMAT — render exactly in this order. Do NOT output any section labels or separator lines. No blank lines between sections — only inside trade cards as shown below.

                    <b style="color:[price color]">[SYMBOL] ($[current_price])</b> | [Rule D emoji+text] | [session_status] | <span style="color:[change color]">[percent_change]</span>[if earnings_flag: " | ⚠️ Earnings in [earnings_days_away]d ([earnings_date])"]
                    <b>📈 Trend</b> Daily <span style="color:[daily color]">[↑/↓/→]</span> · 1h <span style="color:[1h color]">[↑/↓/→]</span> · 15m <span style="color:[15m color]">[↑/↓/→]</span> · 5m <span style="color:[5m color]">[↑/↓/→]</span> · Aligned: [tf_agreement]/4[if tf_agreement == 0: " ⚠️ <span style='color:#ffc107'>All timeframes conflicting</span>"]
                    <b>Signal:</b> <span style="color:[#28a745 if confidence_score>=75, #ffc107 if confidence_score>=50, #dc3545 if confidence_score<50]"><b>[buy_strength: STRONG_BUY→"🔥 Strong Buy", BUY→"🟢 Moderate Buy", WATCH→"⏳ Mixed Signals", SELL→"🟠 Moderate Sell", STRONG_SELL→"🔴 Strong Sell"]</b></span> · [confidence_score]% · RSI [calculated_rsi_14d] ([RSI label per Rule A]) · RVOL [rvol | 1dp]×[if rvol>=2.0: " 🔥"]
                    <b>Why:</b> [active_buy_signals if buy_score > sell_score else active_sell_signals — 1 plain sentence, no jargon, must match RSI label above]
                    <b>📊 What's Happening:</b> [Write 2–7 plain English sentences: (1) where price sits relative to VWAP ($[intraday_vwap]) and nearest support/resistance — is it holding, rejecting, or breaking a level; (2) momentum direction — "strong/fading/weak/recovering", not indicator names; (3) if daily_candle_pattern or chart_pattern is not NONE or bbw_squeeze is true — MUST state the pattern in plain English AND what it typically means for the next move; (4) if sector_etf is not empty — state sector context using Rule AA; (5) smart money — always include: translate smart_money_verdict per Rule E (e.g. "🐋 Institutions Buying — big money is accumulating" or "🚨 Institutions Selling — distribution detected" or "😐 Institutions Neutral"); (6) insider activity — skip only if both insider_buys AND insider_sells are 0. Otherwise output a dedicated sentence structured as:
              First pick the verdict: insider_buys > insider_sells → <span style="color:#28a745"><b>🟢 Insider Buying</b></span> | insider_sells > insider_buys → <span style="color:#dc3545"><b>🔴 Insider Selling</b></span> | equal non-zero → <span style="color:#ffc107"><b>🟡 Insider Mixed</b></span>.
              Then write: "[verdict] — [insider_buys] open-market purchase(s) vs [insider_sells] sale(s) in last 90 days".
              Then add a plain-English interpretation tailored to the numbers:
                • insider_buys == 0 AND insider_sells > 0: add " · No executive has bought shares — removes a key bullish signal." Use red color for this note.
                • insider_buys > 0 AND insider_sells == 0: add " · Executives buying with no sales — a strong vote of confidence." Use green color.
                • insider_buys > 0 AND insider_sells > 0: add " · Note: insider sells are often routine (tax, diversification); open-market buys carry more weight."
              Finally, if insider_mspr is available and non-zero: add MSPR severity — insider_mspr < -60 → " · MSPR [insider_mspr]: heavy distribution 🚨", insider_mspr < -20 → " · MSPR [insider_mspr]: moderate net selling", insider_mspr > 60 → " · MSPR [insider_mspr]: strong accumulation 🚀", insider_mspr > 20 → " · MSPR [insider_mspr]: moderate net buying".
              MSPR explanation: MSPR (Monthly Share Purchase Ratio) measures net dollar value of insider buys vs. sells; strongly negative = executives are cashing out at scale. (7) analyst consensus — only if notable.]
                    <b>📍 Key Levels</b>
                    <span style="color:#dc3545"><b>Resistance:</b></span> <b style="color:#dc3545">$[sr_r1]</b> <i>(nearest)</i> · <b style="color:#dc3545">$[sr_r2]</b> · <b style="color:#dc3545">$[sr_r3]</b> <i>(structural)</i>
                    <span style="color:#28a745"><b>Support:</b></span> <b style="color:#28a745">$[sr_s1]</b> <i>(nearest)</i> · <b style="color:#28a745">$[sr_s2]</b> · <b style="color:#28a745">$[sr_s3]</b> <i>(structural)</i>
                    <span style="color:#6c757d"><b>PDH/PDL:</b></span> Yesterday High <b style="color:#dc3545">$[prior_day_high]</b> · Yesterday Low <b style="color:#28a745">$[prior_day_low]</b> <i>(key intraday reference)</i>
                    <span style="color:#6c757d"><b>Range:</b></span> Tomorrow <b>$[tomorrow_lower]–$[tomorrow_upper]</b> · This week <b>$[next_week_lower]–$[next_week_upper]</b>[if custom_days > 0: " · [custom_days]-day <b>$[custom_lower]–$[custom_upper]</b>"][if iv_regime is HIGH: " · <span style='color:#dc3545'>IV expensive</span>" | if iv_regime is LOW: " · <span style='color:#28a745'>IV cheap</span>"]
                    [Output ONLY the matching trade block below. No blank lines before the first card.]
                    [EARNINGS OVERRIDE: if earnings_flag is true AND earnings_days_away <= 10, output ONLY this card:]
                    <div class="trade-card hold">
                    <b style="color:#ffc107">⚠️ Earnings in [earnings_days_away] days ([earnings_date]) — Stand Aside</b>
                    Options premiums are inflated before earnings — buying a spread is overpriced. Best approach: Iron Condor to collect premium from both sides, or simply wait for the earnings reaction and enter after the dust settles.
                    </div>
                    [end earnings override — skip buy/sell/hold below if earnings override triggered]
                    [If total_confluence_score > +15 AND no earnings override:]
                    <div class="trade-card buy">
                    <b style="color:#28a745">📈 WHAT TO DO — [if EXECUTE_CALL_OR_LONG_SPREAD: "Buy Now" | else: "Wait for Dip, Then Buy"]</b>
                    [if EXECUTE_CALL_OR_LONG_SPREAD: "🎯 Enter now at <b style='color:#28a745'>$[final_entry]</b> — momentum confirmed" | else: "🎯 Wait for a pullback to <b style='color:#28a745'>$[final_entry]</b> — price is extended at $[current_price]"]
                    <b>Target:</b> <b style="color:#28a745">$[final_tp]</b> · <b>Stop:</b> <b style="color:#dc3545">$[final_sl]</b> · <b>R:R</b> [rr_ratio] · Size: ~[suggested_shares] shares / ~[suggested_contracts] contract(s)
                    <b>Strategy:</b> <i>[if recommended_strategy is "A": strategy_a | if "B": strategy_b | else: strategy_c]</i> — [if recommended_strategy is "A": options_line_a | if "B": options_line_b | else: options_line_c]
                    <b>Est. Win Rate:</b> <span style="color:[#28a745 if win_probability>=65, #ffc107 if >=52, #dc3545 if <52]"><b>[win_probability]%</b></span> <i>([win_probability_label])</i>
                    <span style="color:#6c757d"><b>S/R Context:</b></span> Support floor <b style="color:#28a745">$[sr_s1]</b> · <b style="color:#28a745">$[sr_s2]</b> · Resistance targets <b style="color:#dc3545">$[sr_r1]</b> → <b style="color:#dc3545">$[sr_r2]</b> → <b style="color:#dc3545">$[sr_r3]</b>
                    🔴 Flip bearish if breaks <b style="color:#dc3545">$[sr_s1]</b> (S1) → Bear Put Spread · Buy $[alt_bear_put_buy] Put / Sell $[alt_bear_put_sell] Put · Target $[sr_s2] (S2) · Stop $[sr_r1] · exp. [target_expiration]
                    🟢 Momentum accelerates above <b style="color:#28a745">$[sr_r1]</b> (R1) → add contracts · same strategy one strike higher · next target $[sr_r2] (R2)
                    </div>
                    [If total_confluence_score < −15 AND no earnings override:]
                    <div class="trade-card sell">
                    <b style="color:#dc3545">📉 WHAT TO DO — [if EXECUTE_PUT_OR_SHORT_SPREAD: "Sell Now" | else: "Wait for Bounce, Then Sell"]</b>
                    [if EXECUTE_PUT_OR_SHORT_SPREAD: "🎯 Enter now at <b style='color:#dc3545'>$[final_entry]</b> — bearish momentum confirmed" | else: "🎯 Wait for a bounce to <b style='color:#dc3545'>$[final_entry]</b> — price is oversold at $[current_price]"]
                    <b>Target:</b> <b style="color:#28a745">$[final_tp]</b> · <b>Stop:</b> <b style="color:#dc3545">$[final_sl]</b> · <b>R:R</b> [rr_ratio] · Size: ~[suggested_shares] shares / ~[suggested_contracts] contract(s)
                    <b>Strategy:</b> <i>[if recommended_strategy is "A": strategy_a | if "B": strategy_b | else: strategy_c]</i> — [if recommended_strategy is "A": options_line_a | if "B": options_line_b | else: options_line_c]
                    <b>Est. Win Rate:</b> <span style="color:[#28a745 if win_probability>=65, #ffc107 if >=52, #dc3545 if <52]"><b>[win_probability]%</b></span> <i>([win_probability_label])</i>
                    <span style="color:#6c757d"><b>S/R Context:</b></span> Resistance wall <b style="color:#dc3545">$[sr_r1]</b> → <b style="color:#dc3545">$[sr_r2]</b> · Support targets <b style="color:#28a745">$[sr_s1]</b> → <b style="color:#28a745">$[sr_s2]</b> → <b style="color:#28a745">$[sr_s3]</b>
                    🟢 Flip bullish if reclaims <b style="color:#28a745">$[sr_r1]</b> (R1) → Bull Call Spread · Buy $[alt_bull_call_buy] Call / Sell $[alt_bull_call_sell] Call · Target $[sr_r2] (R2) · Stop $[sr_s1] · exp. [target_expiration]
                    🔴 Momentum accelerates below <b style="color:#dc3545">$[sr_s1]</b> (S1) → add contracts · same strategy one strike lower · next target $[sr_s2] (S2)
                    </div>
                    [If total_confluence_score between −15 and +15 AND no earnings override:]
                    <div class="trade-card hold">
                    <b style="color:#ffc107">⏳ WHAT TO DO — Wait for a Clear Signal</b>[if tf_agreement == 0: " <span style='color:#ffc107'>(all timeframes conflicting)</span>"]
                    <b>Est. Win Rate once triggered:</b> <span style="color:[#28a745 if win_probability>=65, #ffc107 if >=52, #dc3545 if <52]"><b>[win_probability]%</b></span> <i>([win_probability_label])</i>
                    <span style="color:#6c757d"><b>S/R Context:</b></span> Resistance <b style="color:#dc3545">$[sr_r1]</b> · <b style="color:#dc3545">$[sr_r2]</b> · Support <b style="color:#28a745">$[sr_s1]</b> · <b style="color:#28a745">$[sr_s2]</b>
                    🟢 Flip bullish above <b style="color:#28a745">$[sr_r1]</b> (R1) → Bull Call Spread · Buy $[alt_bull_call_buy] Call / Sell $[alt_bull_call_sell] Call · Target $[sr_r2] (R2) · Stop $[sr_s1] · exp. [target_expiration] · ~[suggested_contracts] contract(s)
                    🔴 Flip bearish below <b style="color:#dc3545">$[sr_s1]</b> (S1) → Bear Put Spread · Buy $[alt_bear_put_buy] Put / Sell $[alt_bear_put_sell] Put · Target $[sr_s2] (S2) · Stop $[sr_r1] · exp. [target_expiration] · ~[suggested_contracts] contract(s)
                    </div>
                    [if swing_trade_signal is SWING_LONG:]
                    <div class="trade-card swing-long">
                    <b style="color:#28a745">🔄 Swing Trade — Near Support</b>
                    [swing_note]
                    🎯 Entry near <b style="color:#28a745">$[swing_entry]</b>  ·  Target <b style="color:#28a745">$[swing_target]</b>  ·  Stop <b style="color:#dc3545">$[swing_stop]</b>  ·  Timeframe: days to weeks
                    Strategy: <i>[swing_strategy]</i>  ·  Support tested <b>[swing_support_strength]×</b> (more tests = stronger level)
                    <span style="color:#6c757d"><b>S/R Levels:</b></span> Support floor <b style="color:#28a745">$[sr_s1]</b> · <b style="color:#28a745">$[sr_s2]</b> · <b style="color:#28a745">$[sr_s3]</b> · Targets <b style="color:#dc3545">$[sr_r1]</b> → <b style="color:#dc3545">$[sr_r2]</b> → <b style="color:#dc3545">$[sr_r3]</b>
                    🔴 Invalidated if closes below <b style="color:#dc3545">$[sr_s2]</b> (S2) — structure breaks
                    <b>Est. Win Rate:</b> <span style="color:[#28a745 if win_probability>=65, #ffc107 if win_probability>=52, #dc3545 if win_probability<52]"><b>[win_probability]%</b></span> <i>([win_probability_label])</i>
                    </div>
                    [if swing_trade_signal is SWING_SHORT:]
                    <div class="trade-card swing-short">
                    <b style="color:#dc3545">🔄 Swing Trade — Near Resistance</b>
                    [swing_note]
                    🎯 Entry near <b style="color:#dc3545">$[swing_entry]</b>  ·  Target <b style="color:#28a745">$[swing_target]</b>  ·  Stop <b style="color:#dc3545">$[swing_stop]</b>  ·  Timeframe: days to weeks
                    Strategy: <i>[swing_strategy]</i>  ·  Resistance tested <b>[swing_resistance_strength]×</b> (more tests = stronger level)
                    <span style="color:#6c757d"><b>S/R Levels:</b></span> Resistance wall <b style="color:#dc3545">$[sr_r1]</b> · <b style="color:#dc3545">$[sr_r2]</b> · <b style="color:#dc3545">$[sr_r3]</b> · Downside targets <b style="color:#28a745">$[sr_s1]</b> → <b style="color:#28a745">$[sr_s2]</b> → <b style="color:#28a745">$[sr_s3]</b>
                    🟢 Invalidated if closes above <b style="color:#28a745">$[sr_r2]</b> (R2) — bears lose control
                    <b>Est. Win Rate:</b> <span style="color:[#28a745 if win_probability>=65, #ffc107 if win_probability>=52, #dc3545 if win_probability<52]"><b>[win_probability]%</b></span> <i>([win_probability_label])</i>
                    </div>
                    [if swing_trade_signal is RANGE_PLAY:]
                    <div class="trade-card swing-range">
                    <b style="color:#ffc107">🔄 Swing Trade — Range Play</b>
                    [swing_note]
                    Strategy: <i>[swing_strategy]</i> — collect premium while price stays range-bound
                    Sell Put at <b style="color:#28a745">$[sr_s1]</b> (S1)  ·  Sell Call at <b style="color:#dc3545">$[sr_r1]</b> (R1)  ·  Full range: <b style="color:#28a745">$[sr_s2]</b>–<b style="color:#dc3545">$[sr_r2]</b>
                    🔴 Exit if breaks below <b style="color:#dc3545">$[sr_s2]</b> or above <b style="color:#28a745">$[sr_r2]</b> — range invalidated
                    <b>Est. Win Rate:</b> <span style="color:[#28a745 if win_probability>=65, #ffc107 if win_probability>=52, #dc3545 if win_probability<52]"><b>[win_probability]%</b></span> <i>([win_probability_label])</i>
                    </div>
                    """;

    // ── Wheel strategy template ───────────────────────────────────────────────
    private static final String WHEEL_TEMPLATE = """
                    ── WHEEL STRATEGY TEMPLATE ───────────────────────────────────────────────────
                    (Use ONLY when payload contains wheel_candidates. Show all picks immediately.)

                    WHEEL STRATEGY — How it works (show this once at the top, in plain English):
                    "You sell a put option and collect cash upfront. If the stock stays above your strike, you keep the cash and repeat. If it drops below, you buy the stock — then sell a call to collect more cash while you wait to exit."

                    <b>🎡 Wheel Strategy Picks — [wheel_candidates.length] picks · ranked by conviction</b>
                    Scanned: [scan_date]

                    Render ONE table with ALL candidates. No blocks, no dividers between rows.
                    Sort order: sub-$80 stocks first → by Priority (🔴 Red Day → 🟡 Bearish → ⚪ Standard) → within same priority by Day% ascending (biggest drop first)
                    Priority key: 🔴 RED_DAY = down >0.5% today (best entry) · 🟡 BEARISH_BIAS = weekly slope down · ⚪ STANDARD

                    <table>
                    <tr>
                      <th>Stock</th>
                      <th>Price / Day</th>
                      <th>Type</th>
                      <th>Priority</th>
                      <th>IV</th>
                      <th>Delta</th>
                      <th>Put Strike</th>
                      <th>Premium / Contract</th>
                      <th>Weekly %</th>
                      <th>Capital Needed</th>
                      <th>Take Profit</th>
                      <th>Expiry</th>
                      <th>If Assigned → Covered Call</th>
                    </tr>
                    [One <tr> per wheel_candidate. Apply row rules below:]

                    COMPANION ROW RULE: if is_companion=true, render ticker cell as:
                      <td><b style="color:#6c757d;padding-left:1.2em">↳ [ticker]</b> <span style="font-size:0.8em;color:#adb5bd">2× alt for [parent_ticker]</span></td>

                    EARNINGS WEEK RULE: if earnings_this_week=true, render ENTIRE PUT SIDE as:
                      <td colspan="6"><span style="color:#fd7e14">⚠️ Earnings this week — skip new puts · see CC →</span></td>
                    and still render the covered call column normally.

                    NORMAL ROW (earnings_this_week=false):
                    <tr>
                      <td>[if is_companion: companion label (see above), else: <b><span class="ticker-link" onclick="submitChip('analyze [ticker]')" style="cursor:pointer;text-decoration:underline dotted">[ticker]</span></b> [if near_support: 🎯]]</td>
                      <td><b>$[price]</b> <span style="font-size:0.85em;color:[#dc3545 if percent_change<0 else #28a745]">([+/−][percent_change | 1dp]%)</span></td>
                      <td>[if is_etf: "<span style='color:#ffc107'>Leveraged ETF ⚠️</span>" else "Stock"]</td>
                      <td>[priority mapped: RED_DAY→<span style="color:#dc3545;font-weight:700">🔴 Red Day</span>, BEARISH_BIAS→<span style="color:#fd7e14">🟡 Bearish</span>, STANDARD→<span style="color:#6c757d">⚪</span>]</td>
                      <td>[iv]%</td>
                      <td><span style="color:#6c757d">[delta | 2dp]</span></td>
                      <td><b>$[put_strike]</b> <span style="font-size:0.85em;color:#6c757d">([pct_otm=(price−put_strike)/price×100, 1dp]% OTM)</span></td>
                      <td><b style="color:#28a745">$[put_premium]/sh · $[total_premium_per_contract]/contract</b></td>
                      <td><b style="color:#28a745">[weekly_return_pct | 2dp]%/wk</b></td>
                      <td><b>$[capital_if_assigned, 0dp]</b></td>
                      <td><span style="color:#17a2b8">Close @ $[take_profit_at]/sh</span> <span style="font-size:0.8em;color:#6c757d">(90% profit)</span></td>
                      <td>[expiry]</td>
                      <td><b>$[call_strike, 2dp]</b> strike · ~<b style="color:#28a745">$[call_premium × 100, 0dp]</b>/contract [if analyst_buy_pct >= 70: <span style="font-size:0.8em;color:#28a745"> · [analyst_buy_pct]% analyst buy</span>]</td>
                    </tr>

                    EARNINGS WEEK ROW (earnings_this_week=true):
                    <tr>
                      <td>[ticker cell as above]</td>
                      <td>$[price] ([pct]%)</td>
                      <td>[type]</td>
                      <td>[priority]</td>
                      <td colspan="6"><span style="color:#fd7e14">⚠️ Earnings in [earnings_days_away]d — no new puts this week</span></td>
                      <td><b>$[call_strike, 2dp]</b> strike · ~<b style="color:#28a745">$[call_premium × 100, 0dp]</b>/contract</td>
                    </tr>
                    </table>

                    ── BEST PLAY CARD ──
                    After the table, pick the single highest-conviction candidate (RED_DAY > near_support > highest weekly_return_pct) and show:

                    <div class="trade-card" style="margin-top:12px;border-left:3px solid #28a745">
                    <b>⭐ Best Play Right Now — [ticker] [if is_etf: "(ETF)"]</b><br>
                    Sell <b>$[put_strike] put</b> expiring <b>[expiry]</b> · collect <b style="color:#28a745">~$[total_premium_per_contract]</b><br>
                    Close position when premium decays to <b style="color:#17a2b8">$[take_profit_at]/share</b> (90% profit taken)<br>
                    [if earnings_this_week: <span style="color:#fd7e14">⚠️ Skip put sell — earnings in [earnings_days_away] days. Run covered call instead: $[call_strike] · ~$[call_premium × 100, 0dp]/contract</span>]
                    [if near_support: <span style="color:#6c757d;font-size:0.85em">🎯 Near 20d support — good risk/reward for put sale</span>]
                    </div>

                    ⚠️ Leveraged ETF note: if assigned on any leveraged ETF row, sell the covered call immediately and exit within 1–2 weeks — these products decay over time due to daily rebalancing.
                    """;

    // ── Swing scanner table — injected for swing/range queries ───────────────
    private static final String SWING_SCANNER_TEMPLATE = """
                    ── SWING SCANNER TABLE ──────────────────────────────────────────────────────
                    (Use ONLY when payload contains swing_scan_results. Render all rows immediately.)

                    <b>🔄 SWING TRADE SETUPS — Range-Bound & Near-Level Plays</b>
                    High-volume stocks at key support/resistance levels. Each setup includes a specific options strategy.

                    <table>
                    <tr><th>Stock</th><th>Price</th><th>Chg%</th><th>ADX</th><th>Setup</th><th>Support</th><th>Resistance</th><th>📈 Stock Play</th><th>📊 Options Play</th></tr>
                    [One <tr> per swing_scan_results object:
                    - Stock: <td><span class="ticker-link" onclick="submitChip('analyze [symbol]')" style="color:[#17a2b8 if SWING_LONG, #6f42c1 if SWING_SHORT, #fd7e14 if RANGE_PLAY];cursor:pointer;text-decoration:underline dotted;font-weight:700">[symbol]</span></td>
                    - Price: <td>$[current_price]</td>
                    - Chg%: <td><span style="color:[#28a745 if percent_change starts with +, else #dc3545]">[percent_change]</span></td>
                    - ADX: <td>[adx_value | 0dp] <i>([<20: "Ranging", 20-25: "Weak", >25: "Trending"])</i></td>
                    - Setup: <td><b>[swing_trade_signal mapped: SWING_LONG→<span style="color:#17a2b8">Near Support ↑</span>, SWING_SHORT→<span style="color:#6f42c1">Near Resistance ↓</span>, RANGE_PLAY→<span style="color:#fd7e14">Range Play ↔</span>]</b>
                      [if breakout_type=="FRESH_CROSS": <br><span style="font-size:0.8em;color:#fd7e14">🔥 EMA Cross</span>]
                      [if breakout_type=="ABOVE_EMA50": <br><span style="font-size:0.8em;color:#17a2b8">⬆ EMA50 Break</span>]
                      [if breakout_type=="RANGE_BREAK": <br><span style="font-size:0.8em;color:#6f42c1">📊 Range Break</span>]
                      [if volume_ratio>=1.5: <span style="font-size:0.8em;color:#17a2b8"> · Vol [volume_ratio]x</span>]</td>
                    - Support: <td><b style="color:#28a745">$[swing_support]</b></td>
                    - Resistance: <td><b style="color:#dc3545">$[swing_resistance]</b></td>
                    - Stock Play: [if SWING_LONG: <td><b style="color:#28a745">📈 Buy @ $[swing_entry]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[swing_target]</b> · Stop <b style="color:#dc3545">$[swing_stop]</b></span></td>]
                                  [if SWING_SHORT: <td><b style="color:#dc3545">📉 Short @ $[swing_entry]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[swing_target]</b> · Stop <b style="color:#dc3545">$[swing_stop]</b></span></td>]
                                  [if RANGE_PLAY: <td><b style="color:#fd7e14">↔ Buy $[swing_support] / Sell $[swing_resistance]</b><br><span style="font-size:0.85em;color:#6c757d">Stop outside range</span></td>]
                    - Options Play: <td><i>[swing_strategy]</i><br><span style="font-size:0.85em;color:#6c757d">[options_line_b]</span></td>]
                    </table>
                    [swing_note for each row if available — one short sentence per stock on why the setup is valid]

                    REQUIRED — always render this block after the table, never skip it:
                    <div class="best-play-card">
                    <b>🔄 Best Swing Symbols to Play</b>
                    Pick the top 2 SWING_LONG setups ranked by confluence + breakout bonus + volume surge. One line each:
                    <b style="color:#17a2b8">[SYMBOL]</b> — [why: at $[swing_support] support, [breakout_type readable], [volume_ratio]x volume] · Entry: <b>$[swing_entry]</b> → TP <b style="color:#28a745">$[swing_target]</b> · Stop <b style="color:#dc3545">$[swing_stop]</b> · Options: <i>[swing_strategy]</i>
                    If no SWING_LONG, pick the best RANGE_PLAY with the tightest range width.
                    </div>
                    ---
                    """;

    private static final String SECTOR_ROTATION_TEMPLATE = """
                    ── SECTOR ROTATION TABLE ──────────────────────────────────────────────────
                    (Use ONLY when payload contains sector_rotation_results. Render all rows immediately.)

                    <b>💹 SECTOR ROTATION SCANNER — Where Is the Money Flowing?</b>
                    Market Regime: [market_regime] | SPY 1-Month: [spy_1m_return] | Scan: [scan_time]

                    <table>
                    <tr><th>Sector ETF</th><th>Price</th><th>1W</th><th>1M</th><th>RS vs SPY</th><th>Volume</th><th>News</th><th>P/C Ratio</th><th>Score</th><th>Signal</th><th>Top Stocks</th></tr>
                    [One <tr> per sector_rotation_results object:
                    - Sector ETF: <td><b>[etf]</b> <span style="color:#6c757d;font-size:0.85em">[sector_name]</span></td>
                    - Price: <td>$[current_price]</td>
                    - 1W: <td><span style="color:[#28a745 if starts with +, else #dc3545]"><b>[week_return]</b></span></td>
                    - 1M: <td><span style="color:[#28a745 if starts with +, else #dc3545]"><b>[month_return]</b></span></td>
                    - RS vs SPY: <td><span style="color:[#28a745 if starts with +, else #dc3545]">[rs_vs_spy]</span></td>
                    - Volume: <td><span style="color:[#28a745 if RISING, #dc3545 if FALLING, else #6c757d]">[volume_trend]</span></td>
                    - News: <td><span style="color:[#28a745 if Bullish, #dc3545 if Bearish, else #6c757d]">[news_sentiment]</span></td>
                    - P/C Ratio: <td><span style="color:[#28a745 if contains ↑, #dc3545 if contains ↓, else #6c757d]">[put_call_ratio]</span></td>
                    - Score: <td>[rotation_score | 1dp]</td>
                    - Signal: <td><b style="color:[#28a745 if STRONG_INFLOW, #17a2b8 if INFLOW, #6c757d if NEUTRAL, #fd7e14 if OUTFLOW, #dc3545 if STRONG_OUTFLOW]">[signal mapped: STRONG_INFLOW→🟢 STRONG INFLOW, INFLOW→🔵 INFLOW, NEUTRAL→⚪ NEUTRAL, OUTFLOW→🟠 OUTFLOW, STRONG_OUTFLOW→🔴 STRONG OUTFLOW]</b></td>
                    - Top Stocks: <td><span style="color:#6c757d">[top_stocks]</span></td>]
                    </table>

                    After the table, in 3–5 plain English sentences:
                    1. Name the top 2 sectors with STRONG_INFLOW or INFLOW. Mention their news sentiment and P/C ratio if confirming.
                    2. For each top sector, name 1–2 specific stocks from top_stocks that look best to trade right now.
                    3. Name 1–2 sectors to avoid (OUTFLOW / STRONG_OUTFLOW) and why (e.g. bearish news + high P/C ratio).
                    4. State whether the overall regime favors long, short, or rotation plays.
                    ---
                    """;

    private static final String SQUEEZE_SCANNER_TEMPLATE = """
                    ── SQUEEZE SCANNER TABLE ────────────────────────────────────────────────────
                    (Use ONLY when payload contains squeeze_scan_results. Render all rows immediately.)

                    <b>🔥 SQUEEZE SETUPS — Volatility Coiled, Breakout Imminent</b>
                    Stocks with ADX below 15 and low IV rank — the market is sleeping on these. One catalyst could trigger a sharp move.

                    <table>
                    <tr><th>Stock</th><th>Price</th><th>Chg%</th><th>ADX</th><th>IV Rank</th><th>Breakout Dir</th><th>📈 Stock Play</th><th>📊 Options Play</th></tr>
                    [One <tr> per squeeze_scan_results object:
                    - Stock: <td><span class="ticker-link" onclick="submitChip('analyze [symbol]')" style="color:#ffc107;cursor:pointer;text-decoration:underline dotted;font-weight:700">[symbol]</span></td>
                    - Price: <td>$[current_price]</td>
                    - Chg%: <td><span style="color:[#28a745 if starts with +, else #dc3545]">[percent_change]</span></td>
                    - ADX: <td><b style="color:#ffc107">[adx_value | 0dp]</b> <i>(Flat)</i></td>
                    - IV Rank: <td>[iv_rank | 0dp]% <span style="color:#28a745">🟢 Cheap</span></td>
                    - Breakout Dir: <td>[if total_confluence_score>0: "<span style='color:#28a745'>⬆ Bullish bias</span>" else if <0: "<span style='color:#dc3545'>⬇ Bearish bias</span>" else "<span style='color:#ffc107'>↔ Neutral — wait</span>"]</td>
                    - Stock Play: [if total_confluence_score>0: <td><b style="color:#28a745">📈 Buy @ $[micro_support]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b></span></td>]
                                  [if total_confluence_score<=0: <td><b style="color:#dc3545">📉 Short @ $[micro_resistance]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b></span></td>]
                    - Options Play: <td><i>[strategy_name]</i><br><span style="font-size:0.85em;color:#6c757d">[options_line_b]</span></td>]
                    </table>

                    REQUIRED — always render this block after the table, never skip it:
                    <div class="best-play-card">
                    <b>🔥 Best Squeeze to Play</b>
                    Pick the 1–2 tightest squeezes (lowest ADX) with a clear directional bias (|total_confluence_score| > 20). One line each:
                    <b style="color:#ffc107">[SYMBOL]</b> — ADX [adx_value], IV rank [iv_rank]% (cheap options) · Bias: [bullish/bearish] · Entry: <b>$[micro_support or micro_resistance]</b> → TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b> · Play: <i>[strategy_name]</i> — ideal for low-cost straddle if direction unclear
                    </div>
                    ---
                    """;

    private static final String EARNINGS_SCANNER_TEMPLATE = """
                    ── EARNINGS PLAYS TABLE ─────────────────────────────────────────────────────
                    (Use ONLY when payload contains earnings_scan_results. Render all rows immediately.)

                    <b>📅 EARNINGS PLAYS — Pre-Earnings Volatility Opportunities</b>
                    Stocks reporting earnings soon with elevated option premiums. Buy before IV spikes further, or sell premium before IV crush.

                    <table>
                    <tr><th>Stock</th><th>Price</th><th>Earnings In</th><th>IV Rank</th><th>Options Flow</th><th>📈 Stock Play</th><th>📊 Options Play</th></tr>
                    [One <tr> per earnings_scan_results object:
                    - Stock: <td><span class="ticker-link" onclick="submitChip('analyze [symbol]')" style="color:#ffc107;cursor:pointer;text-decoration:underline dotted;font-weight:700">[symbol]</span></td>
                    - Price: <td>$[current_price]</td>
                    - Earnings In: <td><b style="color:#ffc107">⚠️ [earnings_days_away]d ([earnings_date])</b></td>
                    - IV Rank: <td><span style="color:[#dc3545 if iv_rank > 70, #ffc107 if iv_rank > 40, else #28a745]"><b>[iv_rank | 0dp]%</b> [if >70: "🔴 Expensive" | if >40: "🟡 Elevated" | else: "🟢 Fair"]</span></td>
                    - Options Flow: <td><span style="color:[#28a745 if UNUSUAL_CALL_BUYING, #dc3545 if UNUSUAL_PUT_BUYING, #ffc107 if UNUSUAL_BOTH, else #6c757d]">[UNUSUAL_CALL_BUYING→⚡ Call Buying, UNUSUAL_PUT_BUYING→⚡ Put Buying, UNUSUAL_BOTH→⚡ Both Sides, NONE→Normal]</span></td>
                    - Stock Play: [if total_confluence_score>0: <td><b style="color:#28a745">📈 Buy @ $[micro_support]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b></span></td>]
                                  [if total_confluence_score<=0: <td><b style="color:#dc3545">📉 Short @ $[micro_resistance]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b></span></td>]
                    - Options Play: <td><i>[strategy_name]</i><br><span style="font-size:0.85em;color:#6c757d">[options_line_b]</span></td>]
                    </table>

                    REQUIRED — always render this block after the table, never skip it:
                    <div class="best-play-card">
                    <b>📅 Best Earnings Play Right Now</b>
                    Pick the top 1–2 by: earnings soonest + highest confluence + strongest unusual options flow. One line each:
                    <b style="color:#ffc107">[SYMBOL]</b> — earnings in [earnings_days_away]d · IV rank [iv_rank]% ([if >70: sell premium — IV is expensive | if <40: buy options — IV is cheap]) · [unusual flow if any] · Stock: <b>[direction] @ $[entry]</b> → TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b> · Options: <i>[strategy_name]</i>
                    One sentence on key risk: "Binary event — exit before earnings if not playing the report."
                    </div>
                    ---
                    """;

    private static final String FAILED_BREAKDOWN_TEMPLATE = """
                    ── FAILED BREAKDOWN TABLE ───────────────────────────────────────────────────
                    (Use ONLY when payload contains failed_breakdown_results. Render all rows immediately.)

                    <b>↩️ FAILED BREAKDOWNS — Reversal Snap-Back Setups</b>
                    These stocks tested a key support level but buyers stepped in. Price went down, but momentum didn't follow — classic bull trap for short sellers.

                    <table>
                    <tr><th>Stock</th><th>Price</th><th>Chg%</th><th>RSI Div</th><th>Support</th><th>Resistance</th><th>📈 Stock Play</th><th>📊 Options Play</th></tr>
                    [One <tr> per failed_breakdown_results object:
                    - Stock: <td><span class="ticker-link" onclick="submitChip('analyze [symbol]')" style="color:#17a2b8;cursor:pointer;text-decoration:underline dotted;font-weight:700">[symbol]</span></td>
                    - Price: <td>$[current_price]</td>
                    - Chg%: <td><span style="color:[#28a745 if starts with +, else #dc3545]">[percent_change]</span></td>
                    - RSI Div: <td><b style="color:#28a745">✅ Bullish Div</b></td>
                    - Support: <td><b style="color:#28a745">$[swing_support]</b> ([swing_support_strength]× tested)</td>
                    - Resistance: <td><b style="color:#dc3545">$[swing_resistance]</b></td>
                    - Stock Play: <td><b style="color:#28a745">📈 Buy @ $[swing_entry]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[swing_target]</b> · Stop <b style="color:#dc3545">$[swing_stop]</b></span></td>
                    - Options Play: <td><i>[swing_strategy]</i><br><span style="font-size:0.85em;color:#6c757d">[options_line_b]</span></td>]
                    </table>
                    REQUIRED — always render this block after the table, never skip it:
                    <div class="best-play-card">
                    <b>↩️ Best Failed Breakdown to Play</b>
                    Pick the top 1–2 by highest confluence + volume surge (prefer volume_ratio>=1.5). One line each:
                    <b style="color:#17a2b8">[SYMBOL]</b> — held $[swing_support] support with [volume_ratio]x volume · Entry: <b>$[swing_entry]</b> → TP <b style="color:#28a745">$[swing_target]</b> · Stop <b style="color:#dc3545">$[swing_stop]</b> · Options: <i>[swing_strategy]</i> @ $[strike_buy]
                    </div>
                    ---
                    """;

    // ── Market scanner table — injected only for scan queries (~3 KB) ────────
    private static final String SCANNER_TEMPLATE = """
                    ── MARKET SCANNER TABLE ──────────────────────────────────────────────────────
                    (Use ONLY when payload contains scan_results. Show all rows immediately.)

                    <b>[TODAY'S TOP TRADES — [ticker_count] Stocks Worth Watching]</b>
                    Scanned at: [processing time from System Note]
                    <table>
                    <tr><th>Stock</th><th>Price</th><th>Change</th><th>Direction</th><th>Signal / Setup</th><th>📈 Stock Play</th><th>📊 Options Play</th><th>Win%</th></tr>
                    [One <tr> per scan_results object:
                    - Stock: <td><span class="ticker-link" onclick="submitChip('analyze [symbol]')" style="color:[#28a745 if total_confluence_score>0 else #dc3545];cursor:pointer;text-decoration:underline dotted;font-weight:700">[symbol]</span></td>
                    - Price: <td>$[current_price]</td>
                    - Change: <td><span style="color:[#28a745/+ else #dc3545]">[percent_change]</span></td>
                    - Direction: <td><span style="color:[#28a745 if >15, #dc3545 if <-15, #ffc107 else]"><b>[Buy if >15 / Sell if <-15 / Hold]</b></span></td>
                    - Signal / Setup: <td><span style="color:[#28a745 if total_confluence_score>0 else #dc3545]"><b>[Rule B short form e.g. "Strong Buy (+72)"]</b></span>
                      [if breakout_type=="FRESH_CROSS": <br><span style="font-size:0.8em;color:#fd7e14">🔥 EMA Cross</span>]
                      [if breakout_type=="ABOVE_EMA50": <br><span style="font-size:0.8em;color:#17a2b8">⬆ EMA50 Break</span>]
                      [if breakout_type=="RANGE_BREAK": <br><span style="font-size:0.8em;color:#6f42c1">📊 Range Break</span>]
                      [if volume_ratio>=2.0: <span style="font-size:0.8em;color:#17a2b8"> · Vol <b>[volume_ratio]x</b></span>]
                      [if volume_ratio>=1.5 and <2.0: <span style="font-size:0.8em;color:#6c757d"> · Vol [volume_ratio]x</span>]</td>
                    - Stock Play: [if bullish: <td><b style="color:#28a745">📈 Buy @ $[micro_support]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b></span></td>]
                                  [if bearish: <td><b style="color:#dc3545">📉 Short @ $[micro_resistance]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b></span></td>]
                    - Options Play: <td><i>[strategy_name]</i><br><span style="font-size:0.85em;color:#6c757d">[options_line_b]</span></td>
                    - Win%: <td><span style="color:[#28a745 if win_probability>=65, #ffc107 if >=52, #dc3545 if <52]"><b>[win_probability]%</b></span></td>]
                    </table>

                    REQUIRED — always render this block after the table, never skip it:
                    <div class="best-play-card">
                    <b>🎯 Best Symbols to Play Right Now</b>
                    Rank the results by: highest absolute confluence score first, then +20 bonus for FRESH_CROSS breakout, +15 for ABOVE_EMA50, +10 for RANGE_BREAK, +10 for volume_ratio>=2.0. Pick the top 2–3 and write one line each:
                    <b style="color:#28a745">[SYMBOL]</b> — [breakout type in plain English, e.g. "fresh EMA cross"] · [volume_ratio]x volume surge · Stock: <b>📈 Buy @ $[micro_support]</b> → TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b> · Options: <i>[strategy_name]</i> @ $[strike_buy] · Win Rate <b>[win_probability]%</b>
                    Always mention the sector if it's cybersecurity, semiconductor, or biotech.
                    </div>
                    ---
                    """;

    // ── Pre-market table — injected only for pre-market queries (~4 KB) ──────
    private static final String PRE_MARKET_TEMPLATE = """
                    ── PRE-MARKET TABLE ─────────────────────────────────────────────────────────
                    (Use ONLY when payload contains pre_market_scan_results. Show all rows immediately.)

                    <b>[PRE-MARKET MOVERS — Stocks Moving Before the Open (4:00–9:30 AM ET)]</b>
                    Scanned at: [processing time from System Note]
                    <table>
                    <tr><th>Stock</th><th>Pre-Mkt</th><th>Move</th><th>What It's Doing</th><th>Direction</th><th>Signal</th><th>📈 Stock Play</th><th>📊 Options Play</th></tr>
                    [One <tr> per pre_market_scan_results object:
                    - Stock: <td><span class="ticker-link" onclick="submitChip('analyze [symbol]')" style="color:[#28a745/>15, #dc3545/<-15, #ffc107];cursor:pointer;text-decoration:underline dotted;font-weight:700">[symbol]</span></td>
                    - Pre-Mkt: <td>$[current_price]</td>
                    - Move: <td><span style="color:[#28a745/+ else #dc3545]">[percent_change]</span></td>
                    - What It's Doing: <td><i>[pattern plain English: "Gap & Go (Bullish)"→"Opened higher, keeps climbing", "Gap & Go (Bearish)"→"Opened lower, keeps falling", "Gap & Fade (Selling)"→"Opened higher but sellers pushing back", "Gap & Fade (Buying)"→"Opened lower but buyers stepping in", "Consolidating at Gap"→"Holding gap level", "Gap Up (Mixed)"→"Opened higher, unclear direction", "Gap Down (Mixed)"→"Opened lower, unclear direction", "Flat Drift"→"Barely moved overnight"]</i></td>
                    - Direction: <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107]"><b>[Buy/Sell/Hold]</b></span></td>
                    - Signal: <td><span style="color:[#28a745 if total_confluence_score>0 else #dc3545]"><b>[Rule B short form]</b></span></td>
                    - Stock Play: [if bullish: <td><b style="color:#28a745">📈 Buy @ $[micro_support]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b></span></td>]
                                  [if bearish: <td><b style="color:#dc3545">📉 Short @ $[micro_resistance]</b><br><span style="font-size:0.85em;color:#6c757d">TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b></span></td>]
                    - Options Play: <td><i>[strategy_name]</i><br><span style="font-size:0.85em;color:#6c757d">[options_line_b]</span></td>]
                    </table>

                    REQUIRED — always render this block after the table, never skip it:
                    <div class="best-play-card">
                    <b>🌅 Best Pre-Market Play Right Now</b>
                    Pick the 1–2 stocks with the biggest confirmed move AND positive confluence. One line each:
                    <b style="color:#28a745">[SYMBOL]</b> — [move size, e.g. "gapping up +8%"] · [pattern, e.g. "Gap & Go"] · Stock: <b>📈 Buy @ $[micro_support]</b> → TP <b style="color:#28a745">$[final_tp]</b> · Stop <b style="color:#dc3545">$[final_sl]</b> · Options: <i>[strategy_name]</i> @ $[strike_buy]
                    One sentence on overall pre-market mood (risk-on / risk-off / mixed).
                    </div>
                    ---
                    """;

    // ── Quick-answer system prompt — used ONLY for direct questions about a single stock ──
    private static final String QUICK_ANSWER_RULES = """
            You are a concise stock trading assistant. The user asked a direct question about a specific stock.

            Your job:
            1. Call stockPriceFunction for the ticker named in [Intent: QUICK_QUESTION for TICKER].
            2. Answer ONLY the specific question asked — in 2 to 5 plain English sentences.
            3. Reference real numbers: current price, key levels (support/resistance), RSI, trend direction, or relevant technicals.
            4. If the question is about whether to buy a call or put, or which strike, give a specific actionable recommendation.
            5. State clearly if the trend/setup is bullish, bearish, or neutral, and on which timeframe.
            6. Do NOT produce the full dashboard template, table, or headers. No HTML tables. No lengthy analysis.
            7. Be direct and specific. Avoid generic disclaimers like "this is not financial advice" in every sentence.
            """;

    // ── Directional-answer system prompt — for "will it go up/down?" type questions ──
    private static final String DIRECTIONAL_ANSWER_RULES = """
            You are a concise stock trading assistant. The user asked a directional question — whether a stock will go up or down, and the probability.

            Your job:
            1. Call stockPriceFunction for the ticker in [Intent: DIRECTIONAL_QUESTION for TICKER].
            2. Use ONLY real numbers from the live payload — never guess or hallucinate.
            3. Output EXACTLY this compact format — no more, no less:

            <b style="color:[#28a745 if total_confluence_score>15, #dc3545 if <-15, #ffc107 otherwise]">[SYMBOL] — [plain English verdict: "Most likely going DOWN", "Leaning UP — momentum building", "Too close to call — mixed signals right now"]</b>

            <b>Why:</b>
            • [Strongest reason in plain English with real numbers — e.g. "All 4 timeframes bearish (Daily+1h+15m+5m all pointing down)"]
            • [Second reason — e.g. "Price is below VWAP $381 — sellers are in control intraday"]
            • [Third reason — e.g. "RSI at 45 with no reversal pattern — momentum is fading"]

            <b>Key levels:</b> Support <b style="color:#28a745">$[micro_support]</b> (break below = more selling) · Resistance <b style="color:#dc3545">$[micro_resistance]</b> (break above = buyers take control)
            [If the question mentions "next week": show <b>Next week price range:</b> $[next_week_lower]–$[next_week_upper] based on current IV]
            [If the question mentions "tomorrow" or "today": show <b>Tomorrow's range:</b> $[tomorrow_lower]–$[tomorrow_upper] based on current IV]

            <b>Probability of [the direction the user asked about]:</b> <span style="color:[#28a745 if win_probability>=65, #ffc107 if >=52, #dc3545 if <52]"><b>~[win_probability]%</b></span> <i>([win_probability_label])</i>

            <b>What would change this:</b> [one plain English sentence — e.g. "A strong close above $383 on high volume would flip the setup bullish"]

            4. Do NOT produce any dashboard header, Trend line, VWAP line, ADX line, Key Levels section, Price Targets, Outlook, Smart Money, or trade cards.
            5. Write like a trader talking to another trader — plain English, real levels, zero jargon.
            """;

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        this.ollamaClientBuilder = chatClientBuilder
                .defaultSystem(BASE_RULES + STOCK_TEMPLATE + SCANNER_TEMPLATE + PRE_MARKET_TEMPLATE + WHEEL_TEMPLATE + SWING_SCANNER_TEMPLATE + SECTOR_ROTATION_TEMPLATE + SQUEEZE_SCANNER_TEMPLATE + EARNINGS_SCANNER_TEMPLATE + FAILED_BREAKDOWN_TEMPLATE)
                .defaultToolNames("stockPriceFunction", "generalMarketScannerFunction", "bearishScannerFunction", "bullishScannerFunction", "swingScannerFunction", "preMarketScannerFunction", "wheelStrategyScannerFunction", "sectorRotationScannerFunction", "squeezeScannerFunction", "earningsPlaysScannerFunction", "failedBreakdownScannerFunction", "watchlistScannerFunction")
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
        tryInitGoogle(googleApiKey, DEFAULT_GOOGLE_MODEL, DEFAULT_GOOGLE_BASE_URL);
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

        if ("openai".equals(provider) || "anthropic".equals(provider) || "google".equals(provider)) {
            // Fall back to stored runtime key when form left the key field blank (e.g. after file upload)
            String resolvedKey = isValidKey(apiKey) ? apiKey
                    : ("openai".equals(provider)    ? runtimeOpenAiKey
                    : "google".equals(provider)     ? runtimeGoogleKey
                    :                                 runtimeAnthropicKey);
            String resolvedUrl = isValidUrl(baseUrl) ? baseUrl
                    : ("openai".equals(provider)    ? runtimeOpenAiBaseUrl
                    : "google".equals(provider)     ? runtimeGoogleBaseUrl
                    :                                 runtimeAnthropicBaseUrl);
            if (!isValidKey(resolvedKey)) {
                log.warn("[TEST-CONN] {} — API key missing", provider);
                return Map.of("connected", false, "error", "API key is required");
            }
            if (!isValidUrl(resolvedUrl)) {
                log.warn("[TEST-CONN] {} — base URL missing or invalid", provider);
                return Map.of("connected", false, "error", "Base URL is required");
            }
            String model = config.getOrDefault("model", "");
            if (model.isBlank()) {
                log.warn("[TEST-CONN] {} — model name is blank", provider);
                return Map.of("connected", false, "error", "Model name is required");
            }

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
                String respBody = resp.body() != null ? resp.body() : "";
                if (status == 401) {
                    log.warn("[TEST-CONN] {} — 401 Unauthorized. model={} url={} body={}", provider, model, chatUrl, respBody);
                    return Map.of("connected", false, "error", "Invalid API key (401 Unauthorized)");
                }
                if (status == 403) {
                    log.warn("[TEST-CONN] {} — 403 Forbidden. model={} url={} body={}", provider, model, chatUrl, respBody);
                    return Map.of("connected", false, "error", "Access forbidden (403)");
                }
                if (status == 429) {
                    log.warn("[TEST-CONN] {} — 429 Rate limited. model={} url={}", provider, model, chatUrl);
                    return Map.of("connected", false, "error", "Rate limit exceeded — try again later");
                }
                if (status == 404) {
                    log.warn("[TEST-CONN] {} — 404. model={} url={} body={}", provider, model, chatUrl, respBody);
                    if (respBody.contains("model_not_found") || respBody.contains("MODEL_NOT_FOUND")) {
                        return Map.of("connected", false, "error",
                                "Model '" + model + "' not found — verify the model name with your provider");
                    }
                    return Map.of("connected", false, "error",
                            "Endpoint not found (404) — verify the base URL");
                }
                log.warn("[TEST-CONN] {} — HTTP {}. model={} url={} body={}", provider, status, model, chatUrl, respBody);
                return Map.of("connected", false, "error", "Server returned HTTP " + status + " — " + respBody.substring(0, Math.min(200, respBody.length())));
            } catch (Exception e) {
                log.warn("[TEST-CONN] {} — exception: {}", provider, e.getMessage());
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
        String baseUrl = "openai".equals(activeProvider) ? runtimeOpenAiBaseUrl
                       : "google".equals(activeProvider) ? runtimeGoogleBaseUrl
                       : runtimeAnthropicBaseUrl;
        String key     = "openai".equals(activeProvider) ? runtimeOpenAiKey
                       : "google".equals(activeProvider) ? runtimeGoogleKey
                       : runtimeAnthropicKey;
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
        return b.defaultSystem(BASE_RULES + STOCK_TEMPLATE + SCANNER_TEMPLATE + PRE_MARKET_TEMPLATE + WHEEL_TEMPLATE + SWING_SCANNER_TEMPLATE + SECTOR_ROTATION_TEMPLATE + SQUEEZE_SCANNER_TEMPLATE + EARNINGS_SCANNER_TEMPLATE + FAILED_BREAKDOWN_TEMPLATE)
                .defaultToolNames("stockPriceFunction", "generalMarketScannerFunction", "bearishScannerFunction", "bullishScannerFunction", "swingScannerFunction", "preMarketScannerFunction", "wheelStrategyScannerFunction", "sectorRotationScannerFunction", "squeezeScannerFunction", "earningsPlaysScannerFunction", "failedBreakdownScannerFunction", "watchlistScannerFunction")
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

    private void tryInitGoogle(String key, String model, String baseUrl) {
        if (!isValidKey(key)) return;
        try {
            String resolvedUrl   = isValidUrl(baseUrl) ? baseUrl : DEFAULT_GOOGLE_BASE_URL;
            String resolvedModel = (model != null && !model.isBlank()) ? model : DEFAULT_GOOGLE_MODEL;
            OpenAiApi api = OpenAiApi.builder()
                    .apiKey(key)
                    .baseUrl(resolvedUrl)
                    .build();
            OpenAiChatOptions options = OpenAiChatOptions.builder()
                    .model(resolvedModel)
                    .temperature(0.0)
                    .build();
            OpenAiChatModel chatModel = OpenAiChatModel.builder()
                    .openAiApi(api)
                    .defaultOptions(options)
                    .toolCallingManager(toolCallingManager)
                    .build();
            providerClients.put("google", wrapWithDefaults(ChatClient.builder(chatModel)));
            runtimeGoogleBaseUrl = resolvedUrl;
            runtimeGoogleKey     = key;
            log.info("Google Gemini provider ready — model: {}, url: {}", resolvedModel, resolvedUrl);
        } catch (Exception e) {
            log.warn("Google Gemini init failed: {}", e.getMessage());
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
        r.put("ollamaBaseUrl",    ollamaBaseUrl);
        r.put("openAiBaseUrl",    runtimeOpenAiBaseUrl    != null ? runtimeOpenAiBaseUrl    : defaultOpenAiBaseUrl);
        r.put("anthropicBaseUrl", runtimeAnthropicBaseUrl != null ? runtimeAnthropicBaseUrl : defaultAnthropicBaseUrl);
        r.put("googleBaseUrl",    runtimeGoogleBaseUrl    != null ? runtimeGoogleBaseUrl    : DEFAULT_GOOGLE_BASE_URL);
        if ("ollama".equals(activeProvider)) r.put("ollamaModels", getOllamaModels());
        return r;
    }

    public Map<String, Object> getModelConfig() {
        Map<String, Object> r = new HashMap<>();
        r.put("provider", activeProvider);
        r.put("model", activeModel);
        r.put("temperature", activeTemperature);
        r.put("ollamaBaseUrl",    ollamaBaseUrl);
        r.put("openAiBaseUrl",    runtimeOpenAiBaseUrl    != null ? runtimeOpenAiBaseUrl    : defaultOpenAiBaseUrl);
        r.put("anthropicBaseUrl", runtimeAnthropicBaseUrl != null ? runtimeAnthropicBaseUrl : defaultAnthropicBaseUrl);
        r.put("googleBaseUrl",    runtimeGoogleBaseUrl    != null ? runtimeGoogleBaseUrl    : DEFAULT_GOOGLE_BASE_URL);
        r.put("availableProviders", new ArrayList<>(providerClients.keySet()));
        r.put("openAiConfigured",    providerClients.containsKey("openai"));
        r.put("anthropicConfigured", providerClients.containsKey("anthropic"));
        r.put("googleConfigured",    providerClients.containsKey("google"));
        return r;
    }

    public synchronized Map<String, Object> updateModelConfig(Map<String, String> config) {
        String provider = config.getOrDefault("provider", activeProvider).toLowerCase();
        String model    = config.getOrDefault("model", activeModel);
        String apiKey   = config.getOrDefault("apiKey", "");
        String baseUrl  = config.getOrDefault("baseUrl", "");
        float  temp     = parseFloat(config.get("temperature"), activeTemperature);

        if (!List.of("ollama", "openai", "anthropic", "google").contains(provider)) {
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
            if ("google".equals(provider))
                tryInitGoogle(apiKey.isBlank() ? googleApiKey : apiKey, model,
                              baseUrl.isBlank() ? DEFAULT_GOOGLE_BASE_URL : baseUrl);
        }

        if (!providerClients.containsKey(provider)) {
            return Map.of("success", false, "error",
                    "Provider '" + provider + "' is not configured. Provide an API key.");
        }

        activeProvider    = provider;
        activeModel       = model;
        activeTemperature = temp;
        activeApiKey      = switch (provider) {
            case "openai"    -> runtimeOpenAiKey    != null ? runtimeOpenAiKey    : "";
            case "anthropic" -> runtimeAnthropicKey != null ? runtimeAnthropicKey : "";
            case "google"    -> runtimeGoogleKey    != null ? runtimeGoogleKey    : "";
            default          -> "";
        };
        activeBaseUrl     = switch (provider) {
            case "openai"    -> runtimeOpenAiBaseUrl    != null ? runtimeOpenAiBaseUrl    : defaultOpenAiBaseUrl;
            case "anthropic" -> runtimeAnthropicBaseUrl != null ? runtimeAnthropicBaseUrl : defaultAnthropicBaseUrl;
            case "google"    -> runtimeGoogleBaseUrl    != null ? runtimeGoogleBaseUrl    : DEFAULT_GOOGLE_BASE_URL;
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
            case "google"    -> providerClients.containsKey("google");
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

        // 0a. Sector rotation intent
        if (lower.matches(".*\\b(sector.?rotation|sector.?scan|sector.?momentum|sector.?etf|sector.?rank|sector.?leader|sector.?lagg|where.?is.?money.?flow|money.?flow.?(sector|market)|which.?sector|rotate.?(into|out)|best.?sector|top.?sector|xlk|xlf|xle|xli|xlv|xlc|xly|xlp|xlb|xlre|xlu).*")) {
            log.info("[INTENT] sectorRotation");
            return raw + "\n\n[Intent: Call sectorRotationScannerFunction — user wants sector rotation analysis]";
        }

        // 0c. Squeeze scanner intent
        if (lower.matches(".*\\b(squeeze|squeezing|volatility.?coil|compression.?play|breakout.?setup|about.?to.?break.?out|low.?adx|low.?volatility.?setup|coiled).*")) {
            log.info("[INTENT] squeezeScanner");
            return raw + "\n\n[Intent: Call squeezeScannerFunction — user wants squeeze/volatility coil setups]";
        }

        // 0d. Earnings plays scanner intent
        if (lower.matches(".*\\b(earnings.?play|pre.?earnings|stocks.?near.?earnings|earnings.?volatility|iv.?crush|earnings.?trade|earnings.?setup|before.?earnings|upcoming.?earnings).*")) {
            log.info("[INTENT] earningsPlaysScanner");
            return raw + "\n\n[Intent: Call earningsPlaysScannerFunction — user wants earnings plays]";
        }

        // 0e. Failed breakdown scanner intent
        if (lower.matches(".*\\b(failed.?breakdown|reversal.?setup|snap.?back|bullish.?divergence.?play|stocks.?at.?support.?bounced|failed.?breakdown.?scan).*")) {
            log.info("[INTENT] failedBreakdownScanner");
            return raw + "\n\n[Intent: Call failedBreakdownScannerFunction — user wants failed breakdown reversal setups]";
        }

        // 0b. Wheel strategy intent — check before everything else
        if (lower.matches(".*\\b(wheel|csp|cash.?secured.?put|sell.?put.*income|put.*sell.*income|wheel.?scan|wheel.?pick|wheel.?strategy).*")) {
            log.info("[INTENT] wheelStrategy");
            return raw + "\n\n[Intent: Call wheelStrategyScannerFunction — user wants wheel strategy picks]";
        }

        // 1. Pre-market intent — most specific, check first
        // Watchlist scan — must be before general scan; extract uppercase tickers from the message
        if (lower.matches(".*\\b(scan.*watchlist|watchlist.*scan|my watchlist|my stocks|my picks|scan my|scan these stocks|scan these tickers).*")) {
            java.util.regex.Matcher wlm = java.util.regex.Pattern.compile("\\b([A-Z]{1,5})\\b").matcher(raw.toUpperCase());
            java.util.List<String> wlTickers = new java.util.ArrayList<>();
            java.util.Set<String> stopWords = new java.util.HashSet<>(java.util.Arrays.asList(
                "SCAN","MY","WATCHLIST","STOCKS","TICKERS","THESE","PICKS","AND","THE","FOR","ALL"));
            while (wlm.find()) {
                String t = wlm.group(1);
                if (t.length() >= 2 && !stopWords.contains(t)) wlTickers.add(t);
            }
            String tickersCsv = String.join(",", wlTickers);
            log.info("[INTENT] watchlistScanner tickers={}", tickersCsv);
            return raw + "\n\n[Intent: Call watchlistScannerFunction with tickers=\"" + tickersCsv + "\" — apply SCANNER_TEMPLATE with Best Play recommendation]";
        }

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

        // 3e. Quick question about a specific stock — direct answer first, then abbreviated card
        if (ticker != null) {
            boolean hasQuestionMark = raw.trim().endsWith("?");
            boolean hasQuestionWord = lower.matches(
                    ".*\\b(should i|should we|will it|will [a-z]+|is it|is [a-z]+|has [a-z]+|have [a-z]+|did [a-z]+|" +
                    "would you|would i|can [a-z]+|could [a-z]+|when should|when will|why is|why did|why has|" +
                    "how is|how does|flip|flipped|reversal|reversed|bounce|breaking|holding|good time to|time to buy|time to sell)\\b.*");
            boolean isDirectional = lower.matches(
                    ".*\\b(go.?up|go.?down|go.?higher|go.?lower|goes.?up|goes.?down|goes.?higher|goes.?lower|" +
                    "fall|falls|drop|drops|rise|rises|rally|rallying|crash|dump|pump|" +
                    "bullish|bearish|buy.?(now|here|today)|sell.?(now|here|today)|" +
                    "from.?here|from.?this.?level|continue|reverse|turn|turning|break.?down|break.?out|" +
                    "good.?buy|good.?time|right.?time|time.?to.?buy|time.?to.?sell|hold.?or.?sell|buy.?or.?sell|" +
                    "next.?week|next.?month|this.?week|tomorrow|chances|probability|likely|prediction|forecast|" +
                    "going.?to|will.?it|can.?it|would.?it|move.?up|move.?down)\\b.*");
            boolean isCommand = lower.matches(
                    ".*\\b(analyze|analysis|check|scan|show me|tell me|give me|run|pull up|look at)\\b.*");
            if ((hasQuestionMark || hasQuestionWord) && !isCommand) {
                if (isDirectional) {
                    log.info("[INTENT] directionalQuestion ticker={}", ticker);
                    return raw + "\n\n[Intent: DIRECTIONAL_QUESTION for " + ticker
                            + " — call stockPriceFunction, apply Rule Q: lead with Direct Answer + win probability, "
                            + "then show abbreviated Trend + Key Levels + Trade card with Est. Win Rate per Rule R]";
                }
                log.info("[INTENT] quickQuestion ticker={}", ticker);
                return raw + "\n\n[Intent: QUICK_QUESTION for " + ticker
                        + " — call stockPriceFunction then answer only the specific question asked in 3–5 plain English sentences; "
                        + "include win_probability as 'Setup confidence: [X]% ([label])' at the end]";
            }
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

    private String buildPulseContext(String enrichedInput) {
        // Skip for scanner queries — not relevant to single stock context
        boolean isScanner = enrichedInput.contains("[Intent: Call bearishScannerFunction")
                || enrichedInput.contains("[Intent: Call bullishScannerFunction")
                || enrichedInput.contains("[Intent: Call generalMarketScannerFunction")
                || enrichedInput.contains("[Intent: Call preMarketScannerFunction")
                || enrichedInput.contains("[Intent: Call swingScannerFunction")
                || enrichedInput.contains("[Intent: Call wheelStrategyScannerFunction")
                || enrichedInput.contains("[Intent: Call sectorRotationScannerFunction")
                || enrichedInput.contains("[Intent: Call squeezeScannerFunction")
                || enrichedInput.contains("[Intent: Call earningsPlaysScannerFunction")
                || enrichedInput.contains("[Intent: Call failedBreakdownScannerFunction")
                || enrichedInput.contains("[Intent: Call watchlistScannerFunction");
        if (isScanner) return "";
        try {
            MarketPulseService.MarketPulseResult pulse = marketPulseService.getMarketPulse();
            if (pulse == null) return "";
            StringBuilder sb = new StringBuilder();
            if (!pulse.events().isEmpty()) {
                sb.append("\n\n[MACRO CALENDAR — next high-impact US events:]\n");
                pulse.events().stream().limit(3).forEach(ev -> {
                    sb.append("• ").append(ev.etTime()).append(" — ").append(ev.event());
                    if (!ev.estimate().isEmpty()) sb.append(" (Est: ").append(ev.estimate())
                            .append(ev.unit().isEmpty() ? "" : " " + ev.unit()).append(")");
                    sb.append("\n");
                });
                sb.append("Consider whether the analyzed stock's sector is sensitive to these events.");
            }
            if (!pulse.news().isEmpty()) {
                sb.append("\n\n[BREAKING MARKET NEWS — may impact this stock or its sector:]\n");
                pulse.news().stream().limit(3).forEach(item ->
                    sb.append("• [").append(item.ageLabel()).append("] ").append(item.headline()).append("\n"));
                sb.append("If any news above is directly relevant to the stock you're analyzing (same sector/company/macro driver), "
                        + "mention it in the 'What's Happening' section with its specific impact on this ticker.");
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    private String injectDynamicContext(String input) {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("America/New_York"));
        String timeStamp   = now.format(DateTimeFormatter.ofPattern("hh:mm:ss a z"));
        String currentDate = now.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));
        String enriched    = enrichPrompt(input);
        String pulseCtx    = buildPulseContext(enriched);
        return "/no_think\n" + enriched + pulseCtx + "\n\n[System Note: Request processed at " + timeStamp + " on " + currentDate + ".]";
    }

    private record ScanResult(String data, String label) {}

    /**
     * Server-side scanner dispatch — detects scanner intent, calls the Java scanner directly,
     * and injects the pre-fetched JSON data into the prompt so the AI only needs to format it.
     * Each scanner has its own try-catch so one failure does not block the others.
     * Returns null if no scanner intent is detected (fall through to normal AI path).
     */
    private ScanResult preFetchScannerData(String enrichedInput) {
        if (enrichedInput.contains("[Intent: Call generalMarketScannerFunction")) {
            log.info("[SCANNER] Starting market scan...");
            try {
                String data = scannerService.scanMarket();
                log.info("[SCANNER] Market scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "SCANNER_TEMPLATE (key: scan_results)");
            } catch (Exception e) { log.warn("[SCANNER] Market scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call bearishScannerFunction")) {
            log.info("[SCANNER] Starting bearish scan...");
            try {
                String data = scannerService.scanBearish();
                log.info("[SCANNER] Bearish scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "SCANNER_TEMPLATE (key: scan_results)");
            } catch (Exception e) { log.warn("[SCANNER] Bearish scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call bullishScannerFunction")) {
            log.info("[SCANNER] Starting bullish scan...");
            try {
                String data = scannerService.scanBullish();
                log.info("[SCANNER] Bullish scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "SCANNER_TEMPLATE (key: scan_results)");
            } catch (Exception e) { log.warn("[SCANNER] Bullish scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call swingScannerFunction")) {
            log.info("[SCANNER] Starting swing scan...");
            try {
                String data = scannerService.scanSwing();
                log.info("[SCANNER] Swing scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "SWING_SCANNER_TEMPLATE (key: swing_scan_results)");
            } catch (Exception e) { log.warn("[SCANNER] Swing scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call preMarketScannerFunction")) {
            log.info("[SCANNER] Starting pre-market scan...");
            try {
                String data = scannerService.scanPreMarket();
                log.info("[SCANNER] Pre-market scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "PRE_MARKET_TEMPLATE (key: pre_market_scan_results)");
            } catch (Exception e) { log.warn("[SCANNER] Pre-market scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call wheelStrategyScannerFunction")) {
            log.info("[SCANNER] Starting wheel strategy scan...");
            try {
                String data = scannerService.scanWheelStrategy();
                log.info("[SCANNER] Wheel scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "WHEEL_TEMPLATE (key: wheel_candidates)");
            } catch (Exception e) { log.warn("[SCANNER] Wheel scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call sectorRotationScannerFunction")) {
            log.info("[SCANNER] Starting sector rotation scan...");
            try {
                String data = scannerService.scanSectorRotation();
                log.info("[SCANNER] Sector rotation scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "SECTOR_ROTATION_TEMPLATE (key: sector_rotation_results)");
            } catch (Exception e) { log.warn("[SCANNER] Sector rotation scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call squeezeScannerFunction")) {
            log.info("[SCANNER] Starting squeeze scan...");
            try {
                String data = scannerService.scanSqueeze();
                log.info("[SCANNER] Squeeze scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "SQUEEZE_SCANNER_TEMPLATE (key: squeeze_scan_results)");
            } catch (Exception e) { log.warn("[SCANNER] Squeeze scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call earningsPlaysScannerFunction")) {
            log.info("[SCANNER] Starting earnings plays scan...");
            try {
                String data = scannerService.scanEarningsPlays();
                log.info("[SCANNER] Earnings scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "EARNINGS_SCANNER_TEMPLATE (key: earnings_scan_results)");
            } catch (Exception e) { log.warn("[SCANNER] Earnings scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call failedBreakdownScannerFunction")) {
            log.info("[SCANNER] Starting failed breakdown scan...");
            try {
                String data = scannerService.scanFailedBreakdown();
                log.info("[SCANNER] Failed breakdown scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data, "FAILED_BREAKDOWN_TEMPLATE (key: failed_breakdown_results)");
            } catch (Exception e) { log.warn("[SCANNER] Failed breakdown scan FAILED: {}", e.getMessage()); return null; }
        }
        if (enrichedInput.contains("[Intent: Call watchlistScannerFunction")) {
            java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("tickers=\"([^\"]+)\"").matcher(enrichedInput);
            String tickers = m.find() ? m.group(1) : "";
            log.info("[SCANNER] Starting watchlist scan — tickers={}", tickers);
            try {
                String data = scannerService.scanWatchlist(tickers);
                log.info("[SCANNER] Watchlist scan OK — {} chars", data == null ? 0 : data.length());
                return new ScanResult(data,
                        "SCANNER_TEMPLATE as a scanner TABLE (key: scan_results) — render as table rows, NOT as individual stock analysis cards");
            } catch (Exception e) { log.warn("[SCANNER] Watchlist scan FAILED: {}", e.getMessage()); return null; }
        }
        return null;
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
                String enrichedInput = injectDynamicContext(input);

                // Pre-fetch scanner data server-side so any model can format it without tool calls
                ScanResult scanResult = preFetchScannerData(enrichedInput);
                if (scanResult != null && scanResult.data() != null) {
                    log.info("[SCANNER-PREFETCH] data fetched ({} chars), injecting into prompt", scanResult.data().length());
                    enrichedInput = enrichedInput
                            + "\n\n[PRE-FETCHED SCANNER DATA — render this immediately using "
                            + scanResult.label()
                            + ". DO NOT call any tool functions — data is already here:]\n"
                            + scanResult.data();
                }

                boolean quickQuestion       = enrichedInput.contains("[Intent: QUICK_QUESTION");
                boolean directionalQuestion = enrichedInput.contains("[Intent: DIRECTIONAL_QUESTION");
                var promptSpec = directionalQuestion
                        ? client.prompt().system(DIRECTIONAL_ANSWER_RULES).user(enrichedInput)
                        : quickQuestion
                        ? client.prompt().system(QUICK_ANSWER_RULES).user(enrichedInput)
                        : client.prompt().user(enrichedInput);
                String response = switch (activeProvider) {
                    case "openai", "google" -> promptSpec.options(
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
