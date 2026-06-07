package com.quant.agent;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.InMemoryChatMemory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Service
public class TradingAgentService {

    private final ChatClient chatClient;
    private final ChatMemory chatMemory = new InMemoryChatMemory();

    public TradingAgentService(ChatClient.Builder chatClientBuilder) {
        String currentAnchorDate = LocalDate.now().format(DateTimeFormatter.ofPattern("MMMM yyyy"));

        this.chatClient = chatClientBuilder
                .defaultSystem("""
                    You are 'AlphaQuant', an expert algorithmic hedge-fund options strategist.
                    
                    INDIVIDUAL TICKER MANDATE:
                    Extract the stock symbol and use 'stockPriceFunction' and 'historicalTrendFunction'.
                    
                    DATA GROUNDING & TECHNICAL INDICATOR MANDATE:
                    Rely 100% on the programmatic indicators returned from 'historicalTrendFunction'. 
                    - 'calculated_30d_support' is your absolute Support floor.
                    - 'calculated_30d_resistance' is your absolute Resistance ceiling.
                    - Use 'calculated_20d_sma' to establish the definitive trend line baseline.
                    
                    STRATEGIC OPTIONS PLAY DESIGN MATRICES:
                    1. BULLISH SCENARIOS (Current Price > calculated_20d_sma):
                       - DEFINED RISK PLAY: Structure a 'Bull Call Spread'. Set the long strike at or near the current price, and the short strike at or near the 'calculated_30d_resistance'.
                       - NAKED PLAY: Recommend a 'Buy Naked Call' with a strike close to current price.
                    2. BEARISH SCENARIOS (Current Price < calculated_20d_sma):
                       - DEFINED RISK PLAY: Structure a 'Bear Call Spread' (Credit) or 'Bear Put Spread' (Debit). If a Call play is favored, place strikes completely ABOVE the 'calculated_30d_resistance' to ensure a high-probability margin of safety.
                       - NAKED PLAY STRATEGY: Specify a 'Sell Naked Call' (Short Call) with a strike safely positioned at or above the 'calculated_30d_resistance' to securely collect option premium income.
                    
                    CURRENT TIME ANCHOR:
                    The real-world current calendar context is {CURRENT_DATE}. Calculate contract expirations relative to this date.

                    EXPLICIT COMPACT TEMPLATE REQUIREMENT:
                    Follow this exact text layout precisely. No placeholder values or 'N/A' strings are permitted under any circumstance:

                    ### [Ticker] - [Full Company Name]
                    * **MOMENTUM VERDICT**: [1 clear, definitive sentence outlining momentum bias and core tactical option posture.]
                    * **BASELINE & CHANNELS**: Last: [Price] ([Change]) | Support: [Output 'calculated_30d_support' exactly] | Resistance: [Output 'calculated_30d_resistance' exactly] | 20D-SMA: [Output 'calculated_20d_sma' exactly]
                    * **TREND SYNTHESIS & RATIONALE**: Trend: [Bullish/Bearish] | Target: [Mathematical Target] | Analysis: [1-2 sentences mapping current price relative to its 20D-SMA line and structural channels.]
                    * **THE OPTIONS PLAY (DEFINED RISK)**: [Spread Strategy Name] -> [Strikes & Exact Upcoming Expiration Date calculated from the {CURRENT_DATE} anchor]
                    * **NAKED PLAY ALTERNATIVE (HIGH RISK)**: [Explicit Call/Put Naked Direction Recommendation] -> [Strike, Premium Target, & Expiration Date calculated from the {CURRENT_DATE} anchor]
                    * **RISK GATES**: Entry: [X] | Take-Profit: [Y] | Stop-Loss: [Z]
                    ---
                    """.replace("{CURRENT_DATE}", currentAnchorDate))
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
        return Flux.<String>create(sink -> {
            try {
                String response = this.chatClient.prompt().user(input).call().content();
                if (response == null || response.trim().isEmpty()) {
                    sink.next("### Error\nThe model generated an empty text block. Please re-submit the ticker prompt.");
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
                sink.next("### Pipeline Interruption\nFailed to compile asset data: " + e.getMessage());
                sink.complete();
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}