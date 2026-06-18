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

    // ── Base rules sent on every request (~4 KB) ──────────────────────────────
    private static final String BASE_RULES = """
                    You are 'AlphaQuant', a friendly trading assistant. Explain everything like you are a knowledgeable friend helping someone understand the markets — no jargon, no acronyms unless you immediately explain them in plain words right after.

                    MANDATORY TOOL CALLING RULE:
                    You MUST call 'stockPriceFunction' for specific ticker inquiries — it returns all technical data including trend, RSI, EMA crossover, and trade setup in one call. You MUST call 'generalMarketScannerFunction' for broad scans, top options, or trending lists. You MUST call 'preMarketScannerFunction' for pre-market queries. Never invent data. Never call a second function for more data on the same ticker.

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

                    <b style="color:[price color]">[SYMBOL] ($[current_price])</b>  |  [Short plain-English setup label]
                    Checked: [time h:mm AM/PM z]  |  Trend: <span style="color:[#28a745 if ema_crossover_status Bullish/Bullish Cross, #dc3545 if Bearish/Bearish Cross, #ffc107 if Neutral]">[translate ema_crossover_status Rule A]</span>  |  Momentum: [calculated_rsi_14d] — [RSI label Rule A]  |  Signal: [buy_score]↑ [sell_score]↓
                    What to do: <b style="color:[verdict color]">[Rule C]</b> — [One sentence explaining why]
                    <b>[LIVE SNAPSHOT]</b>  |  [session_status]  |  [Rule B]  |  VWAP: $[intraday_vwap]  |  Vol: [volume]
                    Range: $[micro_support]–$[micro_resistance]  |  Change: <span style="color:[change color]">[percent_change]</span>  |  What to watch: [One action sentence]
                    <b>[PRICE TARGETS]</b> (based on [implied_volatility] expected move)
                    Tomorrow: $[tomorrow_lower]–$[tomorrow_upper]  |  Next week: $[next_week_lower]–$[next_week_upper][if custom requested:  |  [Window]: $[custom_lower]–$[custom_upper]]
                    <b>[TREND]</b>
                    Daily: <span style="color:[daily color]">[Up ↑/Down ↓/Sideways →]</span>  |  1h: <span style="color:[1h color]">[Rising ↑/Falling ↓/Flat →]</span>  |  15m: <span style="color:[15m color]">[Pushing Up ↑/Pushing Down ↓/Flat →]</span>  |  5m: <span style="color:[5m color]">[Moving Up ↑/Moving Down ↓/Flat →]</span>
                    <b>[BUY OR SELL?]</b>  |  [Rule D]  |  ↑ Buy: [buy_score]/6  |  ↓ Sell: [sell_score]/6
                    ↑ Buy case ([buy_score]/6): [active_buy_signals]
                    ↓ Sell case ([sell_score]/6): [active_sell_signals]
                    <b>[SMART MONEY]</b>  |  [Rule E]  |  Insider MSPR: [insider_mspr 2dp]  |  Analysts: [analyst_buy] Buy · [analyst_hold] Hold · [analyst_sell] Sell
                    [if smart_money_conflict true:] ⚠️ Smart money and chart signals conflict — wait for alignment before committing full position.
                    [if false + ACCUMULATING:] ✅ Institutions and technicals agree — buy signal confirmed by big money.
                    [if false + DISTRIBUTING:] ✅ Institutions and technicals agree — sell signal confirmed by big money.
                    [if false + NEUTRAL:] Smart money is on the sidelines — rely on technicals.
                    <b>[TRADE SETUP]</b>
                    <b style="color:[#28a745 if score>+15, #dc3545 if score<-15, #ffc107]">[strategy_name]</b> — [1-2 plain-English sentences on entry timing; mention smart money if relevant]
                    [Output ONLY the matching price line — never print the condition label:]
                    [If total_confluence_score > +15:] <b>Enter at:</b> $[final_entry]  |  <b>Take profit at:</b> $[final_tp]  |  <b>Stop loss:</b> $[final_sl]  |  <b>R/R:</b> 2:1  |  Risk $[final_entry−final_sl, 2dp] → Gain $[final_tp−final_entry, 2dp]
                    [If total_confluence_score < −15:] <b>Enter/Sell at:</b> $[final_entry]  |  <b>Cover at:</b> $[final_tp]  |  <b>Stop loss:</b> $[final_sl]  |  <b>R/R:</b> 2:1  |  Risk $[final_sl−final_entry, 2dp] → Gain $[final_entry−final_tp, 2dp]
                    [If between −15 and +15:] Watch: Break above $[micro_resistance] → buy  |  Drop below $[micro_support] → sell  |  Stop if holding: $[final_sl]
                    <b>Options</b> ([strategy_name]): [options_line]
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
                    - Direction: <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107]">[Buy/>15 / Sell/<-15 / Wait]</span></td>
                    - Signal: <td>[Rule B short form e.g. "Strong Buy (+72)"]</td>
                    - Enter At: <td>$[final_entry]</td>
                    - Target: <td>$[final_tp]</td>
                    - Exit If: <td>$[final_sl]</td>
                    - Call Strike: <td>$[strike_buy]</td>
                    - Cover At: <td>$[strike_sell]</td>
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
                    - Direction: <td><span style="color:[#28a745/>15, #dc3545/<-15, #ffc107]">[Buy/Sell/Wait]</span></td>
                    - Signal: <td>[Rule B short form]</td>
                    - Enter At: <td>$[final_entry]</td>
                    - Target: <td>$[final_tp]</td>
                    - Exit If: <td>$[final_sl]</td>
                    - Call Strike: <td>$[strike_buy]</td>
                    - Cover At: <td>$[strike_sell]</td>
                    - Expires: <td>[target_expiration]</td>]
                    </table>
                    [2-3 plain English sentences: overall pre-market mood, strongest setup, best stock for the open. No jargon.]
                    ---
                    """;

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder
                .defaultSystem(BASE_RULES + STOCK_TEMPLATE + SCANNER_TEMPLATE + PRE_MARKET_TEMPLATE)
                .defaultToolNames("stockPriceFunction", "generalMarketScannerFunction", "preMarketScannerFunction")
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

        // Trend / technicals intent with a specific ticker — stockPriceFunction includes all these fields
        if (ticker != null && lower.matches(".*\\b(trend|rsi|ema|technical|chart|signal|momentum|macd|sma).*")) {
            return raw + "\n\n[Intent: Call stockPriceFunction for " + ticker + " and render Dashboard Output Template — user wants technical indicators]";
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
        return "/no_think\n" + enriched + "\n\n[System Note: Request processed at " + timeStamp + " on " + currentDate + ".]";
    }

    public Flux<String> streamAgentResponse(String input) {
        // OllamaChatModel.from() throws NPE on evalDuration=null for non-final streaming chunks
        // (present in Spring AI 1.0.0). Use blocking .call() on a bounded-elastic thread.
        return Flux.concat(
            Flux.just("__PROGRESS__:Fetching live market data..."),
            Flux.defer(() -> {
                String response = this.chatClient.prompt()
                        .user(injectDynamicContext(input))
                        .call()
                        .content();
                if (response == null || response.isBlank()) {
                    return Flux.just("### Pipeline Delay\nMarket processing streams returned empty data frames.");
                }
                return Flux.just(response);
            }).subscribeOn(Schedulers.boundedElastic())
        ).onErrorResume(e -> Flux.just("### Pipeline Interruption\nAnalysis crashed: " + e.getMessage()));
    }
}
