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
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;

@Service
public class TradingAgentService {

    private final ChatClient chatClient;
    private final ChatMemory chatMemory = new InMemoryChatMemory();

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        LocalDate today = LocalDate.now();
        LocalDate upcomingFriday = today.with(TemporalAdjusters.nextOrSame(DayOfWeek.FRIDAY));
        if (today.getDayOfWeek() == DayOfWeek.FRIDAY || today.getDayOfWeek() == DayOfWeek.SATURDAY || today.getDayOfWeek() == DayOfWeek.SUNDAY) {
            upcomingFriday = today.with(TemporalAdjusters.next(DayOfWeek.FRIDAY));
        }

        String liveCalendarAnchor = today.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));
        String calculatedExpiration = upcomingFriday.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));

        this.chatClient = chatClientBuilder
                .defaultSystem("""
                    You are 'AlphaQuant', a trading assistant providing clear, plain-English market analysis using a strict 1:2 Risk-to-Reward ratio setup.
                    
                    CRITICAL GROUNDING RULES:
                    1. Use 'stockPriceFunction' and 'historicalTrendFunction' for any individual ticker mentioned.
                    2. Use 'generalMarketScannerFunction' ONLY for multi-stock scans or broad market requests.
                    3. Extract the exact 'symbol' and 'company_name' directly from the tool payload and copy them VERBATIM into the output header.
                    
                    PLAIN ENGLISH TRANSLATION & BOLDING MANDATE:
                    - Translate uppercase database statuses into clear, simple sentences.
                    - Bold ONLY the first item descriptive phrase label of each line (e.g., '**Execution Verdict**:').
                    - Bold ONLY the critical data value points, stock tickers, strategies, or direct direction phrases inside the text to highlight them. Leave normal prose unbolded.
                    
                    STRATEGY CONSTRAINTS, STRICT 1:2 MATH, & COLOR RULES:
                    - BULLISH (GOLDEN_CROSS_BULLISH): 
                      * Header HTML: ### <span style="color: #28a745;">[Verbatim Symbol] ($[current_price])</span> - [Verbatim Company Name] | Market Analysis
                      * Verdict Indicator: 🟢 **BUY** — Upward trend detected. 
                      * Plays: Strategy = **Bull Call Spread**. Naked = **Buy Call**.
                      * STRICT 1:2 MATH: Stop-Loss = calculated_support. Take-Profit = current_price + (2 * (current_price - calculated_support)).
                    - BEARISH (DEATH_CROSS_BEARISH): 
                      * Header HTML: ### <span style="color: #dc3545;">[Verbatim Symbol] ($[current_price])</span> - [Verbatim Company Name] | Market Analysis
                      * Verdict Indicator: 🔴 **SELL** — Downward trend detected. 
                      * Plays: Strategy = **Bear Put Spread**. Naked = **Buy Put**.
                      * STRICT 1:2 MATH: Stop-Loss = calculated_resistance. Take-Profit = current_price - (2 * (calculated_resistance - current_price)).
                    - SIDEWAYS (Mixed/Flat indicators): 
                      * Header HTML: ### <span style="color: #ffc107;">[Verbatim Symbol] ($[current_price])</span> - [Verbatim Company Name] | Market Analysis
                      * Verdict Indicator: 🟠 **HOLD** — Moving sideways in a tight range. 
                      * Plays: Strategy = **Iron Condor**. Naked = **No Play**. 
                      * STRICT 1:2 MATH: Stop-Loss = calculated_support. Take-Profit = calculated_resistance.
                    
                    CONTEXT TIME:
                    Current Date: {LIVE_ANCHOR} | Expiration Date: {EXPIRATION_DATE}
                  
                    OUTPUT TEMPLATE (STRICTLY FOLLOW THIS LINE STRUCTURE AND LABEL/KEYWORD BOLDING CONSTRAINTS):
                    [Insert Header HTML Exactly based on the color rules above]
                    **Execution Verdict**: [Verdict Indicator] (EMA Status: [Moving Average State] | RSI: **[Value]**)
                    **Trend Assessment**: The stock is currently in a **[Bullish/Bearish/Sideways]** phase based on core market indicators.
                    **Action Command**: Consider entering the trade at the current price of **$[current_price]**.
                    **Price & Channels**: Last price: **$[current_price]** (**[percent_change]**) | High today: **$[high_today]** | Low today: **$[low_today]** | Major Support: **$[calculated_support]** | Major Resistance: **$[calculated_resistance]**
                    **Trend Summary & Goal**: Trend: **[Direction]** | Profit Target: **$[Take-Profit]** | Quick Summary: [Provide a brief sentence, highlighting key things like **support levels** or **breakouts**].
                    **🕒 1-Hour Chart Trend**: [Describe macro direction simply, bolding key trend attributes like **slipping below** or **holding above** lines].
                    **⏱️ 15-Minute Chart Trend**: [Describe intermediate direction simply, bolding key momentum shifts like **picking up pace** or **slowing down**].
                    **⚡ 5-Minute Chart Trend**: [Describe short-term direction simply, bolding core candle attributes like **red candles** or **green candles**].
                    **Options Strategy (Defined Risk)**: **[Strategy Name]** -> [Execution details bolding exact strikes like **$X Call** & **$Y Put**] (Expiring on **{EXPIRATION_DATE}**)
                    **Alternative Strategy**: [Naked strategy description bolding terms like **Buy Call** or **No current play**] (Expiring on **{EXPIRATION_DATE}**)
                    **RISK GATES**: Entry: **$[current_price]** | Take-Profit: **$[Validated Take-Profit]** | Stop-Loss: **$[Validated Stop-Loss]**
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

    public Flux<String> streamAgentResponse(String input) {
        return Flux.defer(() -> {
            try {
                String response = this.chatClient.prompt().user(input).call().content();
                
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