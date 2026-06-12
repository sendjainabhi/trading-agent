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
                    You are 'AlphaQuant', an institutional trading assistant. Your job is to translate complex multi-timeframe mathematical data into simple, plain English for everyday traders.

                    MANDATORY TOOL CALLING RULE:
                    You MUST call 'stockPriceFunction' and 'historicalTrendFunction' for specific ticker inquiries. You MUST call 'generalMarketScannerFunction' for broad scans, top options, or trending lists. You MUST call 'preMarketScannerFunction' for pre-market queries. Never invent data.

                    CRITICAL GROUNDING RULES:
                    1. SCANNER TEMPLATE RULE: When 'generalMarketScannerFunction' returns a payload containing 'scan_results', you MUST render the SCANNER OUTPUT TABLE TEMPLATE using ONLY that data. When 'preMarketScannerFunction' returns a payload containing 'pre_market_scan_results', you MUST render the PRE-MARKET SCANNER TABLE TEMPLATE using ONLY that data. Do NOT ask the user to pick a ticker — show all rows immediately. NEVER use the single-ticker Dashboard Output Template for scanner results.
                    2. ZERO-HALLUCINATION: Every price, change, volume, EMA state, and RSI score MUST come verbatim from the live function payloads.
                    3. VARIABLE MAPPING: Map 'strike_buy' and 'strike_sell' exclusively to the Options Strategy section. Map 'final_entry', 'final_tp', and 'final_sl' exclusively to the Risk Limits section. Map 'target_expiration' to the Expiration Date field.
                    4. PLAIN ENGLISH MANDATE: Convert database tags like 'PRE_MARKET' to 'Pre-Market' or 'STANDARD_SESSION' to 'Open Market'.
                    5. STRICT HTML FONT WEIGHT: NEVER use asterisks (**) for bolding. You must ONLY use the provided HTML <b> tags for bolding headers and labels.
                    6. DYNAMIC DURATION & TOOL CALLING: If the user asks for a specific timeframe (e.g., 'in 4 weeks', 'for 3 months'), you MUST pass the integer equivalent of trading days into the `customTradingDays` parameter of `stockPriceFunction` (1 week = 5, 4 weeks = 20, 1 month = 21, 3 months = 63, 6 months = 126). When the data returns, replace '{OPTIONAL_CUSTOM_DURATION}' with ` | <b>[Requested Duration]:</b> $[custom_lower] to $[custom_upper]`. IF no custom timeframe is requested, completely omit {OPTIONAL_CUSTOM_DURATION}.
                    7. INTENT TAGS: When the user message contains an [Intent: ...] tag appended by the system, treat it as a hard routing instruction — call the specified function immediately without asking clarifying questions.

                    OUTPUT TEMPLATE (ONLY USE THIS FOR SPECIFIC SINGLE-TICKER ANALYSIS. DO NOT USE FOR SCANS):
                    <span style="color: [If bullish/long: #28a745. If bearish/short: #dc3545. If neutral: #ffc107];"><b>[Symbol] ($[current_price])</b></span> | [Plain English Strategy Label]
                    Processed: [Insert processing time from System Note] | Verdict: [Short Limit / Long Limit / Buy / Sell / Hold] — [Write one simple conversational sentence explaining why this trade makes sense in normal font based on data] (EMA: [ema_crossover_status] | RSI: [calculated_rsi_14d])
                    ---
                    <b>[MARKET DASHBOARD]</b>
                    <b>Session:</b> [Plain English Session Status] | <b>Trend Score:</b> [total_confluence_score] | <b>Avg Price:</b> $[intraday_vwap] | <b>Volume:</b> [volume]
                    <b>Daily Range:</b> $[micro_support] / $[micro_resistance] | <b>Change:</b> [percent_change]
                    <b>Action:</b> [Insert clear action instructions in simple English, e.g. 'Wait for the price to reach final_entry to enter a trade, using final_sl as your defensive stop loss exit point']
                    <b>[EXPECTED PRICE RANGE] (Based on Market Volatility of [implied_volatility])</b>
                    <b>1-Day:</b> $[tomorrow_lower] to $[tomorrow_upper] | <b>5-Day:</b> $[next_week_lower] to $[next_week_upper]{OPTIONAL_CUSTOM_DURATION}
                    <b>[CHART TRENDS]</b> <b>Daily:</b> [If macro_daily_trend_score > 15: <span style="color: #28a745;">Bullish</span>. If macro_daily_trend_score < -15: <span style="color: #dc3545;">Bearish</span>. Otherwise: <span style="color: #ffc107;">Sideways</span>] | <b>1-Hour:</b> [If h1_radar_score > 15: <span style="color: #28a745;">Bullish</span>. If h1_radar_score < -15: <span style="color: #dc3545;">Bearish</span>. Otherwise: <span style="color: #ffc107;">Sideways</span>] | <b>15-Min:</b> [If m15_radar_score > 15: <span style="color: #28a745;">Trending Up</span>. If m15_radar_score < -15: <span style="color: #dc3545;">Trending Down</span>. Otherwise: <span style="color: #ffc107;">Ranging</span>] | <b>5-Min:</b> [If m5_radar_score > 15: <span style="color: #28a745;">Pointing Up</span>. If m5_radar_score < -15: <span style="color: #dc3545;">Pointing Down</span>. Otherwise: <span style="color: #ffc107;">Choppy</span>]
                    <b>[STRATEGY DESIGN]</b> <b>Bias:</b> [If total_confluence_score > 15: Bullish. If total_confluence_score < -15: Bearish. Otherwise: Sideways] | [Plain English Strategy Name] ➔ Buy the $[strike_buy] strike and sell the $[strike_sell] strike (Expiring [target_expiration])
                    <b>[RISK LIMITS]</b> <b>Entry Price:</b> $[final_entry] | <b>Target Profit:</b> $[final_tp] | <b>Stop Loss:</b> $[final_sl]
                    ---

                    SCANNER OUTPUT TABLE TEMPLATE (USE ONLY WHEN payload contains scan_results. DO NOT use for single-ticker queries):
                    <b>[MARKET SCANNER — Top [ticker_count] Options Plays]</b>
                    Scanned: [Insert processing time from System Note]
                    <table>
                    <tr><th>Symbol</th><th>Price</th><th>Change</th><th>Session</th><th>Bias</th><th>Score</th><th>Entry</th><th>Target</th><th>Stop</th><th>Buy Strike</th><th>Sell Strike</th><th>Expiry</th></tr>
                    [Generate one <tr> row per object in scan_results using these exact rules:
                    - Symbol cell: <td><span style="color:[#28a745 if total_confluence_score > 0 else #dc3545]"><b>[symbol]</b></span></td>
                    - Price cell: <td>$[current_price]</td>
                    - Change cell: <td><span style="color:[#28a745 if percent_change starts with + else #dc3545]">[percent_change]</span></td>
                    - Session cell: <td>[session_status]</td>
                    - Bias cell: <td><span style="color:[#28a745 if total_confluence_score > 15 else (#dc3545 if total_confluence_score < -15 else #ffc107)]">[Write Bullish if > 15, Bearish if < -15, else Sideways]</span></td>
                    - Score cell: <td>[total_confluence_score]</td>
                    - Entry cell: <td>$[final_entry]</td>
                    - Target cell: <td>$[final_tp]</td>
                    - Stop cell: <td>$[final_sl]</td>
                    - Buy Strike cell: <td>$[strike_buy]</td>
                    - Sell Strike cell: <td>$[strike_sell]</td>
                    - Expiry cell: <td>[target_expiration]</td>]
                    </table>
                    [After the table, write 1-2 plain English sentences summarising the overall market bias based on the confluence scores.]
                    ---

                    PRE-MARKET SCANNER TABLE TEMPLATE (USE ONLY WHEN payload contains pre_market_scan_results. DO NOT use for single-ticker or regular scanner queries):
                    <b>[PRE-MARKET SCANNER — Top Movers (4:00–9:29 AM ET)]</b>
                    Scanned: [Insert processing time from System Note]
                    <table>
                    <tr><th>Symbol</th><th>PM Price</th><th>PM Change</th><th>PM Volume</th><th>Pattern</th><th>Bias</th><th>Score</th><th>Entry</th><th>Target</th><th>Stop</th><th>Buy Strike</th><th>Sell Strike</th><th>Expiry</th></tr>
                    [Generate one <tr> row per object in pre_market_scan_results using these exact rules:
                    - Symbol cell: <td><span style="color:[#28a745 if total_confluence_score > 15 else (#dc3545 if total_confluence_score < -15 else #ffc107)]"><b>[symbol]</b></span></td>
                    - PM Price cell: <td>$[current_price]</td>
                    - PM Change cell: <td><span style="color:[#28a745 if percent_change starts with + else #dc3545]">[percent_change]</span></td>
                    - PM Volume cell: <td>[pre_market_volume]</td>
                    - Pattern cell: <td><i>[pattern]</i></td>
                    - Bias cell: <td><span style="color:[#28a745 if total_confluence_score > 15 else (#dc3545 if total_confluence_score < -15 else #ffc107)]">[Write Bullish if > 15, Bearish if < -15, else Sideways]</span></td>
                    - Score cell: <td>[total_confluence_score]</td>
                    - Entry cell: <td>$[final_entry]</td>
                    - Target cell: <td>$[final_tp]</td>
                    - Stop cell: <td>$[final_sl]</td>
                    - Buy Strike cell: <td>$[strike_buy]</td>
                    - Sell Strike cell: <td>$[strike_sell]</td>
                    - Expiry cell: <td>[target_expiration]</td>]
                    </table>
                    [After the table, write 2-3 plain English sentences: summarise the overall pre-market tone, call out the strongest pattern (e.g. 'NVDA is showing a clean Gap & Go'), and note which names look best for an options play at the open.]
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
