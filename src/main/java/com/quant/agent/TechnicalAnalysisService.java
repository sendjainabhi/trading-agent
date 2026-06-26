package com.quant.agent;

import org.springframework.stereotype.Service;

/**
 * Thin facade — delegates all work to {@link StockAnalysisEngine} and {@link MarketScannerService}.
 * Kept for backward compatibility with {@link McpServerConfig} @Bean lambdas.
 */
@Service
public class TechnicalAnalysisService {

    // ── Injected collaborators ────────────────────────────────────────────────

    private final StockAnalysisEngine engine;
    private final MarketScannerService scannerService;

    public TechnicalAnalysisService(StockAnalysisEngine engine, MarketScannerService scannerService) {
        this.engine = engine;
        this.scannerService = scannerService;
    }

    // ── Single-stock analysis ─────────────────────────────────────────────────

    /** Full intraday + MTF analysis for a single ticker. */
    public String analyzeStock(String ticker, int customDays) {
        return engine.analyzeStock(ticker, customDays);
    }

    /** Historical 60-day trend: RSI + EMA crossover. */
    public String analyzeHistoricalTrend(String ticker) {
        return engine.analyzeHistoricalTrend(ticker);
    }

    // ── Scanner facade methods ────────────────────────────────────────────────

    /** General market scan — top 5 most-active by absolute confluence score. */
    public String scanMarket() {
        try { return scannerService.scanMarket(); } catch (Exception e) { return "{\"error\":\"scan failed.\"}"; }
    }

    /** Watchlist scan — runs full MTF analysis on a caller-supplied list of tickers. */
    public String scanWatchlist(String tickersCsv) {
        try { return scannerService.scanWatchlist(tickersCsv); } catch (Exception e) { return "{\"error\":\"watchlist scan failed.\"}"; }
    }

    /** Bearish scan — top 5 day-losers by most negative confluence score. */
    public String scanBearish() {
        try { return scannerService.scanBearish(); } catch (Exception e) { return "{\"error\":\"bearish scanner failed.\"}"; }
    }

    /** Bullish scan — top 5 day-gainers by most positive confluence score. */
    public String scanBullish() {
        try { return scannerService.scanBullish(); } catch (Exception e) { return "{\"error\":\"bullish scanner failed.\"}"; }
    }

    /** Swing scan — tickers with confirmed SWING_LONG / SWING_SHORT / RANGE_PLAY signals. */
    public String scanSwing() {
        try { return scannerService.scanSwing(); } catch (Exception e) { return "{\"error\":\"swing scanner failed.\"}"; }
    }

    /** Pre-market scan — top movers from the curated watchlist before regular session open. */
    public String scanPreMarket() {
        try { return scannerService.scanPreMarket(); } catch (Exception e) { return "{\"error\":\"pre-market scan failed.\"}"; }
    }

    /** Wheel strategy scan — stocks with IV > 30% and ≥1%/week put premium. */
    public String scanWheelStrategy() {
        return scannerService.scanWheelStrategy();
    }

    /** Sector rotation scan — ranks 11 SPDR sector ETFs by momentum, RS vs SPY, and volume trend. */
    public String scanSectorRotation() {
        try { return scannerService.scanSectorRotation(); } catch (Exception e) { return "{\"error\":\"Sector rotation scan failed.\"}"; }
    }

    /** Squeeze scanner — ADX < 15 + IV rank < 35 signals a volatility coil before a breakout. */
    public String scanSqueeze() {
        try { return scannerService.scanSqueeze(); } catch (Exception e) { return "{\"error\":\"Squeeze scanner failed.\"}"; }
    }

    /** Earnings plays scanner — stocks 1–7 days from earnings with elevated IV rank. */
    public String scanEarningsPlays() {
        try { return scannerService.scanEarningsPlays(); } catch (Exception e) { return "{\"error\":\"Earnings plays scanner failed.\"}"; }
    }

    /** Failed breakdown scanner — SWING_LONG + bullish RSI divergence = reversal snap-back setup. */
    public String scanFailedBreakdown() {
        try { return scannerService.scanFailedBreakdown(); } catch (Exception e) { return "{\"error\":\"Failed breakdown scanner failed.\"}"; }
    }
}
