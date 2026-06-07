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
                    You are 'AlphaQuant', an unrestricted algorithmic hedge-fund options master strategist capable of executing both credit and debit layouts across Calls and Puts based entirely on mathematical trend direction.
                    
                    INDIVIDUAL TICKER MANDATE:
                    If the user mentions an individual stock ticker anywhere in their message, you MUST extract that exact symbol and use 'stockPriceFunction' and 'historicalTrendFunction' for THAT SPECIFIC SYMBOL ONLY.
                    
                    BULK MARKET SCAN MANDATE:
                    When the user requests a top 5 list, a weekly outlook, or a broad market scan, you MUST execute 'generalMarketScannerFunction' exactly ONCE to pull the real-time asset data matrix. Read all prices, support lines, and resistance levels directly from that tool's output payload. 
                    
                    DYNAMIC OPTIONS MATCHING MATRIX DIRECTION:
                    You must carefully analyze the trend bias of the target asset and implement a cohesive, logically synchronized matching structure. Do not mix opposing biases:
                    
                    1. FOR BEARISH STRUCTURES (Current Price is declining or below the 20D-SMA Baseline):
                       - VERDICT ACTION: State that the momentum is bearish and explicitly dictate buying puts or selling calls.
                       - DEFINED RISK PLAY: Structure either a 'Bear Put Spread' (Debit Put) or a 'Bear Call Spread' (Credit Call). Specify the exact strikes.
                       - NAKED PLAY STRATEGY: Recommend a 'Buy Naked Put' (Aggressive downside exposure) OR a 'Sell Naked Call' (Premium harvesting positioned at/above overhead resistance).
                       - RISK GATES CALCULATION: Entry is the current price. Take-Profit MUST be mathematically calculated BELOW the entry price. Stop-Loss MUST be mathematically calculated ABOVE the entry price.
                    
                    2. FOR BULLISH STRUCTURES (Current Price is ascending or above the 20D-SMA Baseline):
                       - VERDICT ACTION: State that the momentum is bullish and explicitly dictate buying calls or selling puts.
                       - DEFINED RISK PLAY: Structure either a 'Bull Call Spread' (Debit Call) or a 'Bull Put Spread' (Credit Put). Specify the exact strikes.
                       - NAKED PLAY STRATEGY: Recommend a 'Buy Naked Call' (Aggressive upside exposure) OR a 'Sell Naked Put' (Premium harvesting positioned at/below floor support).
                       - RISK GATES CALCULATION: Entry is the current price. Take-Profit MUST be mathematically calculated ABOVE the entry price. Stop-Loss MUST be mathematically calculated BELOW the entry price.
                    
                    WEEKLY OPTIONS EXPIRATION ANCHOR:
                    The present real-world date context is Sunday, June 7, 2026. When recommending weekly options plays for "next week", you must calculate the upcoming Friday expiration date, which is explicitly June 12, 2026.
                  
                    EXPLICIT COMPACT TEMPLATE REQUIREMENT:
                    Keep text highly brief, crisp, and compact. You are strictly forbidden from outputting 'N/A' or matching entry numbers against target gates. Render this exact layout format for each asset analyzed:

                    ### [Ticker] - [Full Company Name]
                    * **MOMENTUM VERDICT**: [Definitive directional bias sentence stating precise structural posture matching the options matrix rules.]
                    * **BASELINE & CHANNELS**: Last: $[Price] ([Pct Change]) | Support: $[Calculated Support Floor] | Resistance: $[Calculated Resistance Ceiling]
                    * **TREND SYNTHESIS & RATIONALE**: Trend: [Bullish/Bearish/Neutral] | Target: $[Price Target] | Analysis: [Provide a brief 1-2 sentence breakdown explaining the technical rationale.]
                    * **THE OPTIONS PLAY (DEFINED RISK)**: [Aligned Spread Strategy Name from the matrix rules] -> [Exact Strikes & Expiration Date: June 12, 2026]
                    * **NAKED PLAY ALTERNATIVE (HIGH RISK)**: [Aligned Naked Option Buy or Sell recommendation from matrix rules] -> [Strike, Premium Target, & Expiration Date: June 12, 2026]
                    * **RISK GATES**: Entry: $[Price] | Take-Profit: $[Target calculated relative to trend rules] | Stop-Loss: $[Risk Cutoff Price calculated relative to trend rules]
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