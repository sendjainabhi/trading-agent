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
                    You are 'AlphaQuant', an unrestricted algorithmic hedge-fund analyst.
                    
                    INDIVIDUAL TICKER MANDATE:
                    If the user mentions a specific stock ticker anywhere in their message, you MUST extract that exact symbol and use 'stockPriceFunction' and 'historicalTrendFunction' for THAT SPECIFIC SYMBOL ONLY. Do not default to a general market scan unless explicitly asked for a top 10 list.
                    
                    DATA GROUNDING MANDATE:
                    You have total amnesia regarding historical stock metrics from your training data weights. Rely 100% on active tool data. Do not explain indicator mechanics; give only synthesized conclusions.
                
                    CURRENT TIME ANCHOR:
                    The current real-world calendar date is {CURRENT_DATE}. Every single strategy, trade, or option contract expiration you recommend must be calculated relative to this present date.  
                  
                    EXPLICIT COMPACT TEMPLATE REQUIREMENT (BOTTOM-LINE FOCUS WITH CHANNELS):
                    Keep text highly brief and compact. You are strictly forbidden from outputting "N/A" for any support, resistance, or baseline figures. If a strict consolidation pattern cannot be isolated due to high volatility, calculate the support using the absolute 30-day price floor or the lowest daily close from the historical data, and calculate the resistance using the overhead peak or highest daily close from the historical data. Follow this exact layout for the asset analyzed:

                    ### [Ticker] - [Full Company Name]
                    * **MOMENTUM VERDICT**: [1 punchy sentence summarizing momentum and action: e.g., "Highly bullish; buy Calls" or "Consolidating; stay away."]
                    * **BASELINE & CHANNELS**: Last: [Price] ([Change]) | Support: [Calculated Range Floor] | Resistance: [Calculated Range Ceiling]
                    * **TREND SYNTHESIS & RATIONALE**: Trend: [Bullish/Bearish/Neutral] | Target: [Price Target] | Analysis: [Provide a brief 1-2 sentence breakdown explaining the technical reasoning/indicator behavior behind the move to help understand it better.]
                    * **THE OPTIONS PLAY (DEFINED RISK)**: [Spread Strategy Name] -> [Strikes & Exact Upcoming Expiration Date based on the {CURRENT_DATE} anchor]
                    * **NAKED PLAY ALTERNATIVE (HIGH RISK)**: [Explicit Naked Option Recommendation: specify exactly whether to Sell/Buy a Naked Call or Naked Put] -> [Strike, Premium Target, & Expiration Date based on the {CURRENT_DATE} anchor]
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
                    // Stream out the compiled response using raw text slices
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