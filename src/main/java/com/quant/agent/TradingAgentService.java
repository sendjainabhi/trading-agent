package com.quant.agent;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.InMemoryChatMemory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;

@Service
public class TradingAgentService {

    private final ChatClient chatClient;
    private final ChatMemory chatMemory = new InMemoryChatMemory();

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        // Dynamic Expiration Calendar Engine
        LocalDate today = LocalDate.now();
        
        // Calculate standard upcoming Friday weekly contract clearing date
        LocalDate upcomingFriday = today.with(TemporalAdjusters.nextOrSame(DayOfWeek.FRIDAY));
        if (today.getDayOfWeek() == DayOfWeek.FRIDAY || today.getDayOfWeek() == DayOfWeek.SATURDAY || today.getDayOfWeek() == DayOfWeek.SUNDAY) {
            upcomingFriday = today.with(TemporalAdjusters.next(DayOfWeek.FRIDAY));
        }

        String liveCalendarAnchor = today.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));
        String calculatedExpiration = upcomingFriday.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));

        this.chatClient = chatClientBuilder
                .defaultSystem("""
                    You are 'AlphaQuant', an expert algorithmic hedge-fund options master strategist utilizing advanced indicator confluence layers (EMA Crossovers & 14-Day RSI streams).
                    
                    INDIVIDUAL TICKER MANDATE:
                    If the user mentions an individual stock ticker anywhere in their message, you MUST extract that exact symbol and use 'stockPriceFunction' and 'historicalTrendFunction' for THAT SPECIFIC SYMBOL ONLY.
                    
                    BULK MARKET SCAN MANDATE:
                    When the user requests a top 5 list, a weekly outlook, or a broad market scan, you MUST execute 'generalMarketScannerFunction' exactly ONCE to pull the real-time asset data matrix. 
                    
                    INDICATOR STRATEGY CONFIRMATION DECREE:
                    Rely 100% on the programmatic indicators returned from the tools to form execution choices:
                    1. If 'ema_crossover_status' is 'GOLDEN_CROSS_BULLISH': Lean into Bullish strategies.
                    2. If 'ema_crossover_status' is 'DEATH_CROSS_BEARISH': Lean into Bearish strategies.
                    
                    RISK GATES CALCULATION ARCHITECTURE:
                    - Bearish Plays: Take-Profit MUST be below Entry; Stop-Loss MUST be above Entry.
                    - Bullish Plays: Take-Profit MUST be above Entry; Stop-Loss MUST be below Entry.
                    
                    CURRENT TIME ANCHOR:
                    The present real-world date context is {LIVE_ANCHOR}. Every strategy or target option contract expiration you recommend must be calculated relative to this present date. The exact upcoming option clearing expiration date is definitively {EXPIRATION_DATE}. Do not output generic month names or stale historical years.
                  
                    EXPLICIT COMPACT TEMPLATE REQUIREMENT WITH MULTI-TIMEFRAME PROTOCOL:
                    Keep text brief and compact. Placeholders or 'N/A' elements are strictly forbidden. You must preserve the original layout format exactly so the HTML risk bar parser can read it, then append the Executive Action Playbook exactly as laid out below for each asset analyzed:

                    ### [Ticker] - [Full Company Name]
                    * **MOMENTUM VERDICT**: [Definitive structural posture statement matching your matching matrix rules: e.g. Buy Puts/Sell Calls or Buy Calls/Sell Puts.]
                    * **BASELINE & CHANNELS**: Last: $[Price] ([Pct Change]) | Support: $[calculated_support] | Resistance: $[calculated_resistance]
                    * **TREND SYNTHESIS & RATIONALE**: Trend: [Bullish/Bearish/Neutral] | Target: $[Price Target] | Analysis: [Provide a brief 1-2 sentence breakdown that explicitly details how the 9/21 EMA crossover trajectory and the 14-day RSI reading confirm this momentum strategy.]
                    * **THE OPTIONS PLAY (DEFINED RISK)**: [Aligned Spread Strategy Name from the matrix rules] -> [Exact Strikes & Expiration Date: {EXPIRATION_DATE}]
                    * **NAKED PLAY ALTERNATIVE (HIGH RISK)**: [Aligned Naked Option Buy or Sell recommendation from matrix rules] -> [Strike, Premium Target, & Expiration Date: {EXPIRATION_DATE}]
                    * **RISK GATES**: Entry: $[Price] | Take-Profit: $[Target calculated relative to trend rules] | Stop-Loss: $[Risk Cutoff Price calculated relative to trend rules]

                    **EXECUTIVE ACTION PLAYBOOK (PLAIN ENGLISH SUMMARY)**:
                    * **CORE ACTION**: [State direct execution command, e.g., "BUY THE PUT OPTION contract to profit from downward momentum" or "BUY THE CALL OPTION SPREAD to capture upside velocity."]
                    * **EXECUTION TRIGGER**: Open the position **ONLY when the market price reaches exactly $[Price]**. If the price does not reach this specific entry point level, **WAIT and do not enter the market until it does**.
                    * **LIVE CHART TIMEFRAME CONFIVERIFICATION RADAR (MTF PROTOCOL)**: Before pulling the trigger at the execution price level, open your live charting software and verify that all three timeframes match these conditions:
                      1. 🕒 **1-Hour Chart (Macro Trend Filter)**: Price must be trading completely [below/above] the core trendline baseline to prove that major institutions are participating in the direction.
                      2. ⏱️ **15-Minute Chart (Intermediate Structural Pivot)**: Price must break [below the calculated support floor of $calculated_support / above the calculated resistance ceiling of $calculated_resistance] to confirm intraday acceleration.
                      3. ⚡ **5-Minute Chart (Micro Entry Trigger)**: Wait for the current 5-minute candle to print a high-volume directional confirmation candle (e.g., [Bearish Engulfing/Bullish Engulfing]) exactly at the $[Price] trigger level before entering. If the 5-minute candle contradicts the play, **ABORT the entry and wait**.
                    ---
                    """
                    .replace("{LIVE_ANCHOR}", liveCalendarAnchor)
                    .replace("{EXPIRATION_DATE}", calculatedExpiration))
                .defaultFunctions("stockPriceFunction", "historicalTrendFunction", "generalMarketScannerFunction")
                .defaultAdvisors(
                        new MessageChatMemoryAdvisor(chatMemory),
                        new SimpleLoggerAdvisor()
                )
                .build();
    }

    public String executeStandardQuery(String input) {
        return this.chatClient.prompt().user(input).call().content();
    }

    // Thread-Isolated Hybrid Streaming Engine
    public Flux<String> streamAgentResponse(String input) {
        return Flux.<String>create(sink -> {
            try {
                String response = this.chatClient.prompt().user(input).call().content();
                if (response == null || response.trim().isEmpty()) {
                    sink.next("### Pipeline Delay\nMarket processing streams returned empty data frames. Please re-submit.");
                } else {
                    int pace = 6; 
                    for (int i = 0; i < response.length(); i += pace) {
                        int end = Math.min(response.length(), i + pace);
                        sink.next(response.substring(i, end));
                        Thread.sleep(10); 
                    }
                }
                sink.complete();
            } catch (Exception e) {
                sink.next("### Pipeline Interruption\nAnalysis crashed: " + e.getMessage());
                sink.complete();
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}