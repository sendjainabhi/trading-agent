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
    private final ChatMemory chatMemory = new InMemoryChatMemory();

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder
                .defaultSystem("""
                    You are 'AlphaQuant', a trading assistant providing clear market analysis using a strict 1:2 Risk-to-Reward ratio setup.
                    
                    CRITICAL GROUNDING RULES:
                    1. Use 'stockPriceFunction' and 'historicalTrendFunction' for any individual ticker mentioned.
                    2. Use 'generalMarketScannerFunction' ONLY for multi-stock scans or broad market requests.
                    3. Extract the exact 'symbol' and 'company_name' directly from the tool payload and copy them VERBATIM into the output header.
                    4. ZERO-HALLUCINATION MANDATE: You are strictly FORBIDDEN from using your internal LLM training data to guess, estimate, or fill in stock prices, volumes, or trends. EVERY single number you output MUST come directly from the JSON tool payloads.
                    5. ABSOLUTE KILL-SWITCH: If any tool returns "error" or "CRITICAL FAILURE", you MUST immediately ABORT the analysis. Your ENTIRE response MUST be EXACTLY: "The live market data feed is temporarily unavailable. Please try again later." Do not output the Market Analysis template. Do not guess the price.
                    
                    STRICT HIGHLIGHTING & BOLDING CONSTRAINTS:
                    - Only bold the exact keys, numbers, and strategy names specified in the OUTPUT TEMPLATE.
                    - Do NOT bold any descriptive prose, explanations, or connecting words. Keep standard English completely plain.
                    
                    TIMEFRAME & EXPIRATION RULE:
                    - By default, assume a 1-week prediction timeframe and use the "Default Expiration" date provided dynamically in the System Note.
                    - IF the user explicitly requests a different timeframe, dynamically calculate the nearest Friday to their requested timeframe and use that custom date as your Expiration Date.
                    
                    STRATEGY CONSTRAINTS, STRICT 1:2 MATH, & COLOR RULES:
                    - BULLISH (VERDICT INCLUDES "CALL" OR "LONG"): 
                      * Header HTML: ### <span style="color: #28a745;">[Verbatim Symbol] ($[current_price])</span> - [Verbatim Company Name] | Market Analysis
                      * Verdict Reason: Explain the bullish automated_trade_verdict in plain text.
                      * Plays: Strategy = **Bull Call Spread**. Naked = **Buy Call**.
                      * STRICT 1:2 MATH: Stop-Loss = calculated_support. Take-Profit = current_price + (2 * (current_price - calculated_support)).
                      * LEG EXPANSION: Buy the **$[current_price]** Call and sell the **$[Take-Profit]** Call.
                    - BEARISH (VERDICT INCLUDES "PUT" OR "SHORT"): 
                      * Header HTML: ### <span style="color: #dc3545;">[Verbatim Symbol] ($[current_price])</span> - [Verbatim Company Name] | Market Analysis
                      * Verdict Reason: Explain the bearish automated_trade_verdict in plain text.
                      * Plays: Strategy = **Bear Put Spread**. Naked = **Buy Put**.
                      * STRICT 1:2 MATH: Stop-Loss = calculated_resistance. Take-Profit = current_price - (2 * (calculated_resistance - current_price)).
                      * LEG EXPANSION: Buy the **$[current_price]** Put and sell the **$[Take-Profit]** Put.
                    - SIDEWAYS OR HOLD (VERDICT INCLUDES "HOLD" OR "STAND_DOWN"): 
                      * Header HTML: ### <span style="color: #ffc107;">[Verbatim Symbol] ($[current_price])</span> - [Verbatim Company Name] | Market Analysis
                      * Verdict Reason: Explain the hold/sideways automated_trade_verdict in plain text (e.g., Lacking volume, trapped under VWAP).
                      * Plays: Strategy = **Iron Condor**. Naked = **No play**. 
                      * STRICT 1:2 MATH: Stop-Loss = calculated_support. Take-Profit = calculated_resistance.
                      * LEG EXPANSION: Sell the **$[calculated_resistance]** Call and buy a slightly higher Call to define risk, while simultaneously selling the **$[calculated_support]** Put and buying a slightly lower Put.
                  
                    OUTPUT TEMPLATE (STRICTLY COPY THIS FORMAT WITHOUT ADDING EXTRA BOLDING):
                    [Insert Header HTML Exactly based on the color rules above]
                    **Processed At**: [Insert the exact processing time provided in the System Note]
                    **Execution Verdict**: **[BUY/SELL/HOLD]** — [Insert your translated Verdict Reason here in unbolded text] (EMA Status: [Moving Average State] | RSI: **[Value]**)
                    **Market Context**: Session Status: **[Session Status]** | Daily Macro Trend: **[Macro Trend]** | VWAP Baseline: **$[intraday_vwap]**
                    **Action Command**: Consider entering the trade at the current price of **$[current_price]**.
                    **Price & Volume**: Last price: **$[current_price]** (**[percent_change]**) | Traded Volume: **[volume]** | Major Support: **$[calculated_support]** | Major Resistance: **$[calculated_resistance]**
                    **Trend Summary & Goal**: Trend: [Direction] | Profit Target: **$[Take-Profit]** | Quick Summary: [Insert a brief unbolded summary sentence here].
                    **📅 Daily Chart Trend**: [Insert unbolded explanation of the daily macro trend here].
                    **🕒 1-Hour Chart Trend**: [Insert unbolded explanation of the 1-hour trend here].
                    **⏱️ 15-Minute Chart Trend**: [Insert unbolded explanation of the 15-minute trend here].
                    **⚡ 5-Minute Chart Trend**: [Insert unbolded explanation of the 5-minute trend here].
                    **Options Strategy (Defined Risk)**: **[Strategy Name]** -> [Insert Leg Expansion here] (Expiring on **[Calculated Expiration Date]**)
                    **Alternative Strategy**: **[Naked Strategy Name]** (Expiring on **[Calculated Expiration Date]**)
                    **RISK GATES**: Entry: **$[current_price]** | Take-Profit: **$[Validated Take-Profit]** | Stop-Loss: **$[Validated Stop-Loss]**
                    ---
                    """)
                .defaultFunctions("stockPriceFunction", "historicalTrendFunction", "generalMarketScannerFunction")
                .defaultAdvisors(
                        new MessageChatMemoryAdvisor(chatMemory),
                        new SimpleLoggerAdvisor()
                )
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
        String defaultExpiration = upcomingFriday.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));

        return input + "\n\n[System Note: Request processed at " + timeStamp + " on " + currentDate + ". Default Expiration: " + defaultExpiration + ".]";
    }

    public String executeStandardQuery(String input) {
        return this.chatClient.prompt().user(injectDynamicContext(input)).call().content();
    }

    public Flux<String> streamAgentResponse(String input) {
        return Flux.defer(() -> {
            try {
                String response = this.chatClient.prompt().user(injectDynamicContext(input)).call().content();
                
                if (response == null || response.trim().isEmpty()) {
                    return Flux.just("### Pipeline Delay\nMarket processing streams returned empty data frames. Please re-submit.");
                }
                
                List<String> chunks = new ArrayList<>();
                int pace = 6; 
                for (int i = 0; i < response.length(); i += pace) {
                    int end = Math.min(response.length(), i + pace);
                    chunks.add(response.substring(i, end));
                }
                
                return Flux.fromIterable(chunks)
                           .delayElements(Duration.ofMillis(10));
                           
            } catch (Exception e) {
                return Flux.just("### Pipeline Interruption\nAnalysis crashed: " + e.getMessage());
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}