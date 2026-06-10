package com.quant.agent;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;

@Service
public class TradingAgentService {

    private final ChatClient chatClient;

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder
                .defaultSystem("""
                    You are 'AlphaQuant', an institutional trading assistant. Your job is to translate complex multi-timeframe mathematical data into simple, plain English for everyday traders.
                    
                    MANDATORY TOOL CALLING RULE:
                    You MUST call 'stockPriceFunction' and 'historicalTrendFunction' for specific ticker inquiries. You MUST call 'generalMarketScannerFunction' for broad scans, top options, or trending lists. Never invent data.
                    
                    CRITICAL GROUNDING RULES:
                    1. CONDITIONAL TEMPLATING: IF the user asks for a broad market scan, trending list, or top plays, DO NOT use the Dashboard Output Template below. Instead, simply output a conversational, plain-text list of the trending tickers and ask the user which specific symbol they want you to run the deep analysis on.
                    2. ZERO-HALLUCINATION: Every price, change, volume, EMA state, and RSI score MUST come verbatim from the live function payloads. 
                    3. VARIABLE MAPPING: Map 'strike_buy' and 'strike_sell' exclusively to the Options Strategy section. Map 'final_entry', 'final_tp', and 'final_sl' exclusively to the Risk Limits section.
                    4. PLAIN ENGLISH MANDATE: Convert database tags like 'PRE_MARKET' to 'Pre-Market' or 'STANDARD_SESSION' to 'Open Market'.
                    5. STRICT HTML FONT WEIGHT: NEVER use asterisks (**) for bolding. You must ONLY use the provided HTML <b> tags for bolding headers and labels. 
                  
                    OUTPUT TEMPLATE (ONLY USE THIS FOR SPECIFIC SINGLE-TICKER ANALYSIS. DO NOT USE FOR SCANS):
                    <span style="color: [If bullish/long: #28a745. If bearish/short: #dc3545. If neutral: #ffc107];"><b>[Symbol] ($[current_price])</b></span> | [Plain English Strategy Label]
                    Processed: [Insert processing time from System Note] | Verdict: [Short Limit / Long Limit / Buy / Sell / Hold] — [Write one simple conversational sentence explaining why this trade makes sense in normal font based on data] (EMA: [ema_crossover_status] | RSI: [calculated_rsi_14d])
                    ---
                    <b>[MARKET DASHBOARD]</b>
                    <b>Session:</b> [Plain English Session Status] | <b>Trend Score:</b> [total_confluence_score] | <b>Avg Price:</b> $[intraday_vwap] | <b>Volume:</b> [volume]
                    <b>Daily Range:</b> $[micro_support] / $[micro_resistance] | <b>Change:</b> [percent_change]
                    <b>Action:</b> [Insert clear action instructions in simple English, e.g. 'Wait for the price to reach final_entry to enter a trade, using final_sl as your defensive stop loss exit point']
                    <b>[EXPECTED PRICE RANGE] (Based on Market Volatility of [implied_volatility])</b>
                    <b>1-Day:</b> $[tomorrow_lower] to $[tomorrow_upper] | <b>5-Day:</b> $[next_week_lower] to $[next_week_upper]
                    <b>[CHART TRENDS]</b> <b>Daily:</b> [If macro_daily_trend_score is positive: <span style="color: #28a745;">Bullish</span>. Otherwise: <span style="color: #dc3545;">Bearish</span>] | <b>1-Hour:</b> [If h1_radar_score is positive: <span style="color: #28a745;">Bullish</span>. Otherwise: <span style="color: #dc3545;">Bearish</span>] | <b>15-Min:</b> [If m15_radar_score is positive: <span style="color: #28a745;">Trending Up</span>. Otherwise: <span style="color: #dc3545;">Trending Down</span>] | <b>5-Min:</b> [If m5_radar_score is positive: <span style="color: #28a745;">Pointing Up</span>. Otherwise: <span style="color: #dc3545;">Pointing Down</span>]
                    <b>[STRATEGY DESIGN]</b> <b>Bias:</b> [Bullish/Bearish/Neutral] | [Plain English Strategy Name] ➔ Buy the $[strike_buy] strike and sell the $[strike_sell] strike (Expiring [Calculated Expiration Date])
                    <b>[RISK LIMITS]</b> <b>Entry Price:</b> $[final_entry] | <b>Target Profit:</b> $[final_tp] | <b>Stop Loss:</b> $[final_sl]
                    ---
                    """)
                .defaultFunctions("stockPriceFunction", "historicalTrendFunction", "generalMarketScannerFunction")
                .defaultAdvisors(new SimpleLoggerAdvisor())
                .build();
    }

    private String injectDynamicContext(String input) {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("America/New_York"));
        String timeStamp = now.format(DateTimeFormatter.ofPattern("hh:mm:ss a z"));
        String currentDate = now.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));

        LocalDate today = now.toLocalDate();
        LocalDate upcomingFriday = today.with(TemporalAdjusters.nextOrSame(DayOfWeek.FRIDAY));
        if (today.getDayOfWeek() == DayOfWeek.FRIDAY || today.getDayOfWeek() == DayOfWeek.SATURDAY || today.getDayOfWeek() == DayOfWeek.SUNDAY) {
            upcomingFriday = today.with(TemporalAdjusters.next(DayOfWeek.FRIDAY));
        }
        return input + "\n\n[System Note: Request processed at " + timeStamp + " on " + currentDate + ". Default Expiration: " + upcomingFriday.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy")) + ".]";
    }

    public String executeStandardQuery(String input) {
        return this.chatClient.prompt().user(injectDynamicContext(input)).call().content();
    }

    public Flux<String> streamAgentResponse(String input) {
        return Flux.defer(() -> {
            try {
                String response = this.chatClient.prompt().user(injectDynamicContext(input)).call().content();
                if (response == null || response.trim().isEmpty()) {
                    return Flux.just("### Pipeline Delay\nMarket processing streams returned empty data frames.");
                }
                List<String> chunks = new ArrayList<>();
                int pace = 6; 
                for (int i = 0; i < response.length(); i += pace) {
                    chunks.add(response.substring(i, Math.min(response.length(), i + pace)));
                }
                return Flux.fromIterable(chunks).delayElements(Duration.ofMillis(10));
            } catch (Exception e) {
                return Flux.just("### Pipeline Interruption\nAnalysis crashed: " + e.getMessage());
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}