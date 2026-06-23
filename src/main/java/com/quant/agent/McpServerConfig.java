package com.quant.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;

import java.util.function.Function;

/**
 * Spring MCP tool registrations.
 *
 * This class contains ONLY @Bean definitions — all computation logic lives in
 * {@link TechnicalAnalysisService}. Each bean is a thin wrapper that delegates
 * immediately to the service, keeping this file at ~100 lines and easy to audit.
 *
 * NOTE: @Bean methods MUST remain in a @Configuration class for Spring AI to
 * register them as MCP tools; they cannot be moved to the @Service.
 */
@Configuration
public class McpServerConfig {

    private static final Logger log = LoggerFactory.getLogger(McpServerConfig.class);

    // ── Request records ───────────────────────────────────────────────────────
    // Kept here because Spring AI resolves tool parameter schemas from the
    // record types referenced in the @Bean method signatures.

    /** Used by stockPriceFunction — split schema so the AI always passes customTradingDays. */
    public record PriceRequest(String symbol, Integer customTradingDays) {}

    /** Used by historicalTrendFunction — symbol only. */
    public record TrendRequest(String symbol) {}

    /** Used by scanner functions that require no input parameters. */
    public record EmptyRequest() {}

    // ── Service injection ─────────────────────────────────────────────────────

    private final TechnicalAnalysisService analysisService;

    public McpServerConfig(TechnicalAnalysisService analysisService) {
        this.analysisService = analysisService;
    }

    // ── MCP tool beans ────────────────────────────────────────────────────────

    @Bean
    @Description("USE THIS tool to fetch market data. CRITICAL: If the user asks for a specific timeframe (e.g. 'in 4 weeks', '3 months'), you MUST pass the equivalent trading days into 'customTradingDays' (1 week=5, 4 weeks=20, 3 months=63).")
    public Function<PriceRequest, String> stockPriceFunction() {
        return request -> {
            String ticker = request.symbol().replaceAll("[\"']", "").trim().toUpperCase();
            int customDays = request.customTradingDays() != null ? request.customTradingDays() : 0;
            log.info("[TOOL] stockPriceFunction called — ticker={}, customDays={}", ticker, customDays);
            return analysisService.analyzeStock(ticker, customDays);
        };
    }

    @Bean
    @Description("USE THIS tool to get historical trend data, RSI, and EMA crossover status. Requires only the ticker symbol.")
    public Function<TrendRequest, String> historicalTrendFunction() {
        return request -> {
            String ticker = request.symbol().replaceAll("[\"']", "").trim().toUpperCase();
            return analysisService.analyzeHistoricalTrend(ticker);
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks for top options plays, trending tickers, market movers, or a broad market scan. Returns full multi-timeframe analysis and options levels for the top 5 trending US stocks — render results as a table.")
    public Function<EmptyRequest, String> generalMarketScannerFunction() {
        return request -> {
            log.info("[TOOL] generalMarketScannerFunction called");
            try {
                return analysisService.scanMarket();
            } catch (Exception e) {
                return "{\"error\":\"CRITICAL FAILURE: Exception parsing scanner streams.\"}";
            }
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks for bearish movers, biggest losers, stocks falling, or downside plays. Fetches today's day-losers from Yahoo Finance, runs full MTF analysis, and returns the top 5 with the most negative confluence score.")
    public Function<EmptyRequest, String> bearishScannerFunction() {
        return request -> {
            log.info("[TOOL] bearishScannerFunction called");
            try {
                return analysisService.scanBearish();
            } catch (Exception e) {
                return "{\"error\":\"bearish scanner failed.\"}";
            }
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks for bullish movers, biggest gainers, stocks rising, or upside plays. Fetches today's day-gainers from Yahoo Finance, runs full MTF analysis, and returns the top 5 with the most positive confluence score.")
    public Function<EmptyRequest, String> bullishScannerFunction() {
        return request -> {
            log.info("[TOOL] bullishScannerFunction called");
            try {
                return analysisService.scanBullish();
            } catch (Exception e) {
                return "{\"error\":\"bullish scanner failed.\"}";
            }
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks for swing trades, swing scan, swing plays, range-bound stocks, consolidating stocks, or stocks at support/resistance. Scans high-volume stocks (>500K) for SWING_LONG (near support), SWING_SHORT (near resistance), or RANGE_PLAY (Iron Condor) setups using real swing high/low detection from daily bars.")
    public Function<EmptyRequest, String> swingScannerFunction() {
        return request -> {
            log.info("[TOOL] swingScannerFunction called");
            try {
                return analysisService.scanSwing();
            } catch (Exception e) {
                log.error("[TOOL] swingScannerFunction failed: {}", e.getMessage());
                return "{\"error\":\"swing scanner failed.\"}";
            }
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks about pre-market movers, gap plays, what to watch before the open, or pre-market scanner. Scans a curated watchlist for pre-market price movement and pattern (Gap & Go, Gap & Fade, Consolidating). Returns top movers with full options analysis — render results as a pre-market table.")
    public Function<EmptyRequest, String> preMarketScannerFunction() {
        return request -> {
            log.info("[TOOL] preMarketScannerFunction called");
            try {
                return analysisService.scanPreMarket();
            } catch (Exception e) {
                return "{\"error\":\"CRITICAL FAILURE: Pre-market scan failed.\"}";
            }
        };
    }

    @Bean
    @Description("USE THIS tool — and ONLY this tool — when the user mentions 'wheel', 'wheel strategy', 'wheel scan', 'wheel picks', 'cash-secured put', 'CSP', or 'sell puts for income'. Do NOT use generalMarketScannerFunction for these requests. Scans stocks and ETFs $3–$80, tries weekly → 2-week → monthly expiry in order until one hits ≥1%/week premium.")
    public Function<EmptyRequest, String> wheelStrategyScannerFunction() {
        return request -> {
            log.info("[TOOL] wheelStrategyScannerFunction called");
            return analysisService.scanWheelStrategy();
        };
    }
}
