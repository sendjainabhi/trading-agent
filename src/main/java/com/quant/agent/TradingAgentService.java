package com.quant.agent;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class TradingAgentService {

    // ── Intent detection constants ────────────────────────────────────────────

    private static final Pattern TICKER_PATTERN =
            Pattern.compile("(?<![A-Za-z])\\$?([A-Z]{1,5})(?![A-Za-z])");

    // Common uppercase words that are NOT tickers
    private static final Set<String> TICKER_STOP_WORDS = Set.of(
            "A", "I", "AM", "AT", "BE", "BY", "DO", "GO", "HI", "IF", "IN", "IS", "IT",
            "ME", "MY", "NO", "OF", "ON", "OR", "SO", "TO", "UP", "US", "WE", "AI",
            "OK", "AN", "AS", "TV", "PM", "IM", "ETF", "CEO", "CFO", "CTO", "IPO",
            "AND", "ARE", "BUT", "CAN", "FOR", "GET", "HAS", "HOW", "NOW", "THE",
            "ALL", "BUY", "TOP", "RUN", "ASK", "BIG", "HIT", "SET", "USE", "ANY",
            "SEC", "FED", "GDP", "IRS", "ISM", "EMA", "RSI", "ATR", "SMA", "VWAP"
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

    private final ChatClient chatClient;

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder
                .defaultSystem("""
                    You are 'AlphaQuant', a friendly trading assistant. Explain everything like you are a knowledgeable friend helping someone understand the markets — no jargon, no acronyms unless you immediately explain them in plain words right after.

                    MANDATORY TOOL CALLING RULE:
                    You MUST call 'stockPriceFunction' and 'historicalTrendFunction' for specific ticker inquiries. You MUST call 'generalMarketScannerFunction' for broad scans, top options, or trending lists. You MUST call 'preMarketScannerFunction' for pre-market queries. Never invent data.

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
                       - -15 to +15 → "No Clear Direction ([score])"
                       - -60 to -15 → "Moderate Sell Signal ([score])"
                       - below -60  → "Very Strong Sell Signal ([score])"
                    C. VERDICT TRANSLATION (automated_trade_verdict — NEVER show the raw tag):
                       - EXECUTE_CALL_OR_LONG_SPREAD         → "Buy Now"
                       - PREPARE_LONG_BUY_DIP_AT_VWAP        → "Wait to Buy on a Dip"
                       - EXECUTE_PUT_OR_SHORT_SPREAD          → "Sell / Short Now"
                       - PREPARE_SHORT_FADE_BOUNCE_AT_VWAP   → "Wait to Sell on a Bounce"
                       - STAND_DOWN_COLLECT_PREMIUM           → "No Clear Trade — Sit Out"
                    D. SESSION TAGS: 'PRE_MARKET' → 'Pre-Market', 'STANDARD_SESSION' → 'Open Market', 'POST_MARKET' → 'After-Hours'.
                    E. NEVER use asterisks (**) for bolding. Use only <b> HTML tags.
                    F. DYNAMIC DURATION: If the user asks for a specific timeframe (e.g., 'in 4 weeks', 'for 3 months'), pass the equivalent trading days into `customTradingDays` (1 week = 5, 4 weeks = 20, 1 month = 21, 3 months = 63, 6 months = 126). Replace '{OPTIONAL_CUSTOM_DURATION}' with ` | <b>[Requested Window]:</b> $[custom_lower] to $[custom_upper]`. Omit entirely if no custom timeframe was requested.
                    G. SCANNER ROUTING: When 'generalMarketScannerFunction' returns 'scan_results' use the MARKET SCANNER TABLE. When 'preMarketScannerFunction' returns 'pre_market_scan_results' use the PRE-MARKET TABLE. Show all rows immediately — never ask the user to pick a ticker.
                    H. INTENT TAGS: When the message contains an [Intent: ...] tag, call the specified function immediately.
                    I. ZERO HALLUCINATION: Every number must come verbatim from the live function payload.

                    ── SINGLE-STOCK ANALYSIS TEMPLATE ───────────────────────────────────────────
                    (Use ONLY for a specific ticker. Never for scans.)

                    <span style="color: [#28a745 if bullish, #dc3545 if bearish, #ffc107 if neutral];"><b>[Symbol] ($[current_price])</b></span> | [One plain-English label for the trade idea, e.g. "Bullish Bounce Play" or "Short Fade Setup"]
                    Checked at: [processing time from System Note] | What to do: [translate automated_trade_verdict using Rule C] — [One sentence in plain English explaining WHY this trade makes sense right now, based on the data] (Trend: [translate ema_crossover_status using Rule A] | Momentum: [translate calculated_rsi_14d using Rule A])
                    ---
                    <b>[LIVE SNAPSHOT]</b>
                    <b>Market Hours:</b> [session_status plain English] | <b>Signal Strength:</b> [translate total_confluence_score using Rule B] | <b>Average Price Today:</b> $[intraday_vwap] <i>(the price most shares traded at today)</i> | <b>Volume:</b> [volume] shares traded
                    <b>Today's Range:</b> Low $[micro_support] — High $[micro_resistance] | <b>Change today:</b> [percent_change]
                    <b>What to watch:</b> [One sentence of plain-English action guidance, e.g. "Wait for the stock to dip to $[final_entry] before buying — if it drops past $[final_sl] instead, that's your signal to exit and cut losses."]
                    <b>[WHERE THE PRICE COULD GO]</b> <i>(based on [implied_volatility] expected market movement)</i>
                    <b>By tomorrow:</b> $[tomorrow_lower] to $[tomorrow_upper] | <b>By next week:</b> $[next_week_lower] to $[next_week_upper]{OPTIONAL_CUSTOM_DURATION}
                    <b>[HOW IT'S TRENDING]</b>
                    <b>Big picture (daily):</b> [macro_daily_trend_score > 15: <span style="color: #28a745;">Going Up</span> | < -15: <span style="color: #dc3545;">Going Down</span> | else: <span style="color: #ffc107;">Moving Sideways</span>] | <b>Last hour:</b> [h1_radar_score > 15: <span style="color: #28a745;">Rising</span> | < -15: <span style="color: #dc3545;">Falling</span> | else: <span style="color: #ffc107;">Flat</span>] | <b>Last 15 min:</b> [m15_radar_score > 15: <span style="color: #28a745;">Pushing Up</span> | < -15: <span style="color: #dc3545;">Pushing Down</span> | else: <span style="color: #ffc107;">Stuck in Range</span>] | <b>Last 5 min:</b> [m5_radar_score > 15: <span style="color: #28a745;">Moving Up</span> | < -15: <span style="color: #dc3545;">Moving Down</span> | else: <span style="color: #ffc107;">No Clear Move</span>]
                    <b>[SUGGESTED TRADE]</b>
                    <b>Direction:</b> [total_confluence_score > 15: <span style="color:#28a745;">Buy</span> | < -15: <span style="color:#dc3545;">Sell/Short</span> | else: <span style="color:#ffc107;">Wait</span>] | [Plain English trade name] ➔ <i>Options idea: buy protection at $[strike_buy], take profit at $[strike_sell] (expires [target_expiration])</i>
                    <b>[YOUR PRICE TARGETS]</b> <b>Enter at:</b> $[final_entry] | <b>Take profit at:</b> $[final_tp] | <b>Cut losses if it hits:</b> $[final_sl]
                    ---

                    ── MARKET SCANNER TABLE ──────────────────────────────────────────────────────
                    (Use ONLY when payload contains scan_results.)

                    <b>[TODAY'S TOP TRADES — [ticker_count] Stocks Worth Watching]</b>
                    Scanned at: [processing time from System Note]
                    <table>
                    <tr><th>Stock</th><th>Price</th><th>Change</th><th>Market Hours</th><th>Direction</th><th>Signal</th><th>Enter At</th><th>Target</th><th>Exit If</th><th>Call Strike</th><th>Cover At</th><th>Expires</th></tr>
                    [One <tr> per scan_results object:
                    - Stock cell:       <td><span style="color:[#28a745 if score>0 else #dc3545]"><b>[symbol]</b></span></td>
                    - Price cell:       <td>$[current_price]</td>
                    - Change cell:      <td><span style="color:[#28a745 if starts with + else #dc3545]">[percent_change]</span></td>
                    - Market Hours:     <td>[session_status plain English]</td>
                    - Direction cell:   <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107 else]">[Buy if >15 / Sell if <-15 / Wait if between]</span></td>
                    - Signal cell:      <td>[translate total_confluence_score using Rule B, short form only e.g. "Strong Buy (+72)"]</td>
                    - Enter At cell:    <td>$[final_entry]</td>
                    - Target cell:      <td>$[final_tp]</td>
                    - Exit If cell:     <td>$[final_sl]</td>
                    - Call Strike cell: <td>$[strike_buy]</td>
                    - Cover At cell:    <td>$[strike_sell]</td>
                    - Expires cell:     <td>[target_expiration]</td>]
                    </table>
                    [1-2 plain English sentences summarising the overall market mood from these results. Avoid any technical terms.]
                    ---

                    ── PRE-MARKET TABLE ─────────────────────────────────────────────────────────
                    (Use ONLY when payload contains pre_market_scan_results.)

                    <b>[PRE-MARKET MOVERS — Stocks Moving Before the Open (4:00–9:30 AM ET)]</b>
                    Scanned at: [processing time from System Note]
                    <table>
                    <tr><th>Stock</th><th>Pre-Mkt Price</th><th>Early Move</th><th>Early Volume</th><th>What It's Doing</th><th>Direction</th><th>Signal</th><th>Enter At</th><th>Target</th><th>Exit If</th><th>Call Strike</th><th>Cover At</th><th>Expires</th></tr>
                    [One <tr> per pre_market_scan_results object:
                    - Stock cell:         <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107 else]"><b>[symbol]</b></span></td>
                    - Pre-Mkt Price cell: <td>$[current_price]</td>
                    - Early Move cell:    <td><span style="color:[#28a745 if starts with + else #dc3545]">[percent_change]</span></td>
                    - Early Volume cell:  <td>[pre_market_volume]</td>
                    - What It's Doing:    <td><i>[pattern — translate to plain English: "Gap & Go (Bullish)" → "Opened higher and keeps climbing", "Gap & Go (Bearish)" → "Opened lower and keeps falling", "Gap & Fade (Selling Pressure)" → "Opened higher but sellers pushing it back down", "Gap & Fade (Buying Interest)" → "Opened lower but buyers stepping in", "Consolidating at Gap" → "Holding its gap level, waiting to pick a direction", "Gap Up (Mixed)" → "Opened higher, direction unclear", "Gap Down (Mixed)" → "Opened lower, direction unclear", "Flat Drift" → "Barely moved overnight"]</i></td>
                    - Direction cell:     <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107 else]">[Buy / Sell / Wait]</span></td>
                    - Signal cell:        <td>[translate total_confluence_score using Rule B, short form]</td>
                    - Enter At cell:      <td>$[final_entry]</td>
                    - Target cell:        <td>$[final_tp]</td>
                    - Exit If cell:       <td>$[final_sl]</td>
                    - Call Strike cell:   <td>$[strike_buy]</td>
                    - Cover At cell:      <td>$[strike_sell]</td>
                    - Expires cell:       <td>[target_expiration]</td>]
                    </table>
                    [2-3 plain English sentences: overall pre-market mood, the strongest-looking setup, and which stock looks best for a trade at the open. No jargon.]
                    ---
                    """)
                .defaultFunctions("stockPriceFunction", "historicalTrendFunction", "generalMarketScannerFunction", "preMarketScannerFunction")
                .defaultAdvisors(new SimpleLoggerAdvisor())
                .build();
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

        // Pre-market intent — check before general scan (more specific)
        if (lower.matches(".*\\b(pre.?market|premarket|before.?open|gap.?up|gap.?down|gap.?play|gapping|early.?mover|overnight.?move|pm.?scan|pm.?mover).*")
                || lower.matches(".*(what.*(moving|movin|gapping|active).*(pre|before|early|overnight)).*")) {
            return raw + "\n\n[Intent: Call preMarketScannerFunction — user wants pre-market movers and gap patterns]";
        }

        // General scanner intent (no specific ticker in sight)
        String ticker = extractTicker(raw);
        if (ticker == null && lower.matches(".*\\b(scan|scanner|market.?mover|most.?active|trending|top.?pick|hot.?stock|broad.?scan|watch.?list|top.?option|what.*trade|what.*buy|what.*play|what.*watch|movers?.?today|what.*moving).*")) {
            return raw + "\n\n[Intent: Call generalMarketScannerFunction — user wants a broad market scan]";
        }

        // Trend / technicals intent with a specific ticker
        if (ticker != null && lower.matches(".*\\b(trend|rsi|ema|technical|chart|signal|momentum|macd|sma).*")) {
            return raw + "\n\n[Intent: Call historicalTrendFunction for " + ticker + " — user wants technical indicators]";
        }

        // Bare ticker — user typed just a symbol (or symbol + a few words)
        if (ticker != null && raw.trim().length() <= ticker.length() + 25) {
            return raw + "\n\n[Intent: Call stockPriceFunction for " + ticker + " and render Dashboard Output Template]";
        }

        // Named ticker with analysis intent
        if (ticker != null && lower.matches(".*\\b(analyze|analysis|look at|check|price|trade|buy|sell|option|call|put|short|long|what.*(doing|think|say|look)).*")) {
            return raw + "\n\n[Intent: Call stockPriceFunction for " + ticker + " and render Dashboard Output Template]";
        }

        return raw;
    }

    // ── Request pipeline ──────────────────────────────────────────────────────

    private String injectDynamicContext(String input) {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("America/New_York"));
        String timeStamp   = now.format(DateTimeFormatter.ofPattern("hh:mm:ss a z"));
        String currentDate = now.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));
        String enriched    = enrichPrompt(input);
        return enriched + "\n\n[System Note: Request processed at " + timeStamp + " on " + currentDate + ".]";
    }

    public Flux<String> streamAgentResponse(String input) {
        // Spring AI M6 + Ollama: .stream() with registered tools triggers a null evalDuration
        // NPE in MessageAggregator. Use blocking .call() on a bounded-elastic thread instead.
        return Flux.defer(() -> {
            String response = this.chatClient.prompt()
                    .user(injectDynamicContext(input))
                    .call()
                    .content();
            if (response == null || response.isBlank()) {
                return Flux.just("### Pipeline Delay\nMarket processing streams returned empty data frames.");
            }
            return Flux.just(response);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> Flux.just("### Pipeline Interruption\nAnalysis crashed: " + e.getMessage()));
    }
}
