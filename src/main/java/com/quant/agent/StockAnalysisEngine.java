package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Core multi-timeframe (MTF) analysis engine.
 * Fetches bar data, computes indicators via {@link IndicatorUtils}, and builds JSON result strings.
 */
@Service
public class StockAnalysisEngine {

    // ── Scan cache (TTL = 30 s) ───────────────────────────────────────────────

    record CachedScan(String json, Instant cachedAt) {}
    final ConcurrentHashMap<String, CachedScan> scanCache = new ConcurrentHashMap<>();
    static final long SCAN_CACHE_TTL_SECONDS = 30;

    // ── Analysis cache (TTL = 60 s, keyed by ticker:customDays) ─────────────
    final ConcurrentHashMap<String, CachedScan> analysisCache = new ConcurrentHashMap<>();
    static final long ANALYSIS_CACHE_TTL_SECONDS = 60;

    // ── Finnhub rate limiter (free tier: 60 calls/min — cap at 55) ───────────
    private static final Semaphore FINNHUB_LIMITER = new Semaphore(55, true);
    static {
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "finnhub-rate-refill");
            t.setDaemon(true);
            return t;
        }).scheduleAtFixedRate(() -> {
            int toRelease = 55 - FINNHUB_LIMITER.availablePermits();
            if (toRelease > 0) FINNHUB_LIMITER.release(toRelease);
        }, 1, 1, TimeUnit.MINUTES);
    }

    // ── Fields ────────────────────────────────────────────────────────────────

    final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${market.provider.api-key}")
    String finnhubKey;

    // ── Injected collaborators ────────────────────────────────────────────────

    final AlpacaClient alpacaClient;
    final MarketClockService marketClockService;

    public StockAnalysisEngine(AlpacaClient alpacaClient, MarketClockService marketClockService) {
        this.alpacaClient = alpacaClient;
        this.marketClockService = marketClockService;
    }

    private CompletableFuture<HttpResponse<String>> finnhubAsync(String url) {
        if (finnhubKey == null || finnhubKey.isBlank() || !FINNHUB_LIMITER.tryAcquire()) {
            return CompletableFuture.completedFuture(null);
        }
        return alpacaClient.httpClient.sendAsync(
            HttpRequest.newBuilder().uri(URI.create(url))
                .timeout(Duration.ofSeconds(3)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }

    // ── Scan result extraction helpers ────────────────────────────────────────

    /** Parses {@code total_confluence_score} from a ticker JSON result string. */
    public double extractConfluenceScore(String tickerJson) {
        try {
            return objectMapper.readTree(tickerJson).path("total_confluence_score").asDouble(0.0);
        } catch (Exception e) {
            return 0.0;
        }
    }

    /** Returns swing_trade_signal from a ticker JSON result ("SWING_LONG", "SWING_SHORT", "RANGE_PLAY", or "NONE"). */
    public String extractSwingSignal(String tickerJson) {
        try {
            return objectMapper.readTree(tickerJson).path("swing_trade_signal").asText("NONE");
        } catch (Exception e) {
            return "NONE";
        }
    }

    /** Parses the numeric {@code percent_change} value from a pre-market ticker JSON string. */
    public double extractPreMarketChangePct(String json) {
        try {
            String raw = objectMapper.readTree(json).path("percent_change").asText("0%");
            return Double.parseDouble(raw.replace("%", "").replace("+", "").trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    public double extractAdxValue(String json) {
        try { return objectMapper.readTree(json).path("adx_value").asDouble(25.0); } catch (Exception e) { return 25.0; }
    }

    public double extractIvRank(String json) {
        try { return objectMapper.readTree(json).path("iv_rank").asDouble(50.0); } catch (Exception e) { return 50.0; }
    }

    public int extractEarningsDaysAway(String json) {
        try { return objectMapper.readTree(json).path("earnings_days_away").asInt(999); } catch (Exception e) { return 999; }
    }

    public boolean extractEarningsFlag(String json) {
        try { return objectMapper.readTree(json).path("earnings_flag").asBoolean(false); } catch (Exception e) { return false; }
    }

    public String extractRsiDivergence(String json) {
        try { return objectMapper.readTree(json).path("rsi_divergence").asText("NONE"); } catch (Exception e) { return "NONE"; }
    }

    public String extractUnusualOptions(String json) {
        try { return objectMapper.readTree(json).path("unusual_options_activity").asText("NONE"); } catch (Exception e) { return "NONE"; }
    }

    // ── Core multi-timeframe alignment engine ─────────────────────────────────

    /**
     * Fetches bar data across D1 / H1 / M15 / M5 timeframes, computes all indicators,
     * and returns a JSON fragment (starts with a comma) suitable for embedding in a ticker object.
     */
    public String processIntradayMtfAlignment(String ticker, double currentPrice, double highToday, double lowToday, long totalVolume, double priorClose, int customDays) throws Exception {
        ZonedDateTime nowET = ZonedDateTime.now(ZoneId.of("America/New_York"));

        String lookbackD1 = nowET.minusDays(380).format(DateTimeFormatter.ISO_INSTANT); // 380d → ~252 trading bars for 52-week range
        String lookback90Days = nowET.minusDays(90).format(DateTimeFormatter.ISO_INSTANT); // 90d for SPY regime + beta
        String lookback10Days = nowET.minusDays(10).format(DateTimeFormatter.ISO_INSTANT);
        String lookback5Days = nowET.minusDays(5).format(DateTimeFormatter.ISO_INSTANT);

        double dailyScore = (currentPrice >= priorClose) ? 100.0 : -100.0;
        double h1Score = dailyScore;
        double m15Score = dailyScore;
        double m5Score = dailyScore;

        double vwap = currentPrice;
        double microSupport = (lowToday > 0.0) ? lowToday : currentPrice * 0.99;
        double microResistance = (highToday > 0.0) ? highToday : currentPrice * 1.01;
        double avgVolume30d = 0.0;
        double hrv = 0.24;
        double atr14 = 0.0;
        double rsi14 = 50.0;
        double sma20 = 0.0;
        double macdH1 = 0.0;
        String emaCrossoverStatus  = "Neutral";
        double calculatedSupport    = currentPrice * 0.96;
        double calculatedResistance = currentPrice * 1.04;

        String sessionStatus = marketClockService.toPlainEnglish();

        LocalDate today = nowET.toLocalDate();
        String earningsFrom = today.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String earningsTo   = today.plusDays(Math.max(customDays > 0 ? customDays : 14, 14)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        double adxValue = 25.0;
        String adxTrend = "Neutral";
        String adxDirection = "FLAT";
        String rsiDivergence = "NONE";
        double minHrv = Double.MAX_VALUE, maxHrv = 0.0;
        double ivRank = 50.0;
        String ivRegime = "NORMAL";
        int tfAgreement = 0;
        double finnhubBullishPct = -1.0;
        String finnhubNewsSentLabel = "N/A";
        double priorDayHigh = 0.0;
        double priorDayLow  = 0.0;
        double vwapUpper = 0.0;
        double vwapLower = 0.0;
        double swingSupport = 0.0;
        double swingResistance = 0.0;
        int swingSupportStrength = 0;
        int swingResistanceStrength = 0;
        double spyScore = 0.0;
        String spyTrend = "Neutral";
        double vixLevel = 20.0;
        String marketRegime = "NEUTRAL";
        boolean earningsFlag = false;
        String earningsDate = "";
        int earningsDaysAway = 999;
        double yearHigh = 0.0, yearLow = Double.MAX_VALUE, weekRangePosition = 50.0;
        double[] stockLogReturns = new double[0];
        double beta = 1.0;
        String unusualOptionsActivity = "NONE";
        boolean earningsIvInflated = false;
        int confidenceScore = 50;
        String confidenceLabel = "Moderate";
        int maStackScore = 0;
        String maStackLabel = "Unconfirmed";
        double ema8d = 0.0, ema21d = 0.0, ema50d = 0.0, ema200d = 0.0;
        String weeklyBias = "NEUTRAL";
        double weeklyEma20 = 0.0, weeklyEma50 = 0.0;
        String marketBreadth = "NEUTRAL";
        double breadthRs = 0.0;
        double spy20dReturn = 0.0;
        int compositeTrendScore = 50;
        String compositeTrendLabel = "Neutral";

        String insiderFrom = nowET.minusMonths(3).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String insiderTo   = nowET.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        // ── Async HTTP requests ───────────────────────────────────────────────
        CompletableFuture<HttpResponse<String>> futureD1 = alpacaClient.httpClient.sendAsync(alpacaClient.buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Day&start=" + lookbackD1 + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureH1 = alpacaClient.httpClient.sendAsync(alpacaClient.buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Hour&start=" + lookback10Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureM15 = alpacaClient.httpClient.sendAsync(alpacaClient.buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=15Min&start=" + lookback5Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureM5 = alpacaClient.httpClient.sendAsync(alpacaClient.buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=5Min&start=" + lookback5Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureOptions = alpacaClient.httpClient.sendAsync(alpacaClient.buildAlpacaBaseRequest("/v1beta1/options/snapshots/" + ticker + "?feed=indicative&strike_price_gte=" + (currentPrice * 0.98) + "&strike_price_lte=" + (currentPrice * 1.02) + "&limit=50"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureNews = alpacaClient.httpClient.sendAsync(
                HttpRequest.newBuilder()
                        .uri(URI.create("https://data.alpaca.markets/v1beta1/news?symbols=" + ticker + "&limit=20&sort=desc"))
                        .header("APCA-API-KEY-ID", alpacaClient.apiKey != null ? alpacaClient.apiKey : "")
                        .header("APCA-API-SECRET-KEY", alpacaClient.apiSecret != null ? alpacaClient.apiSecret : "")
                        .header("accept", "application/json")
                        .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureEarnings =
                finnhubAsync("https://finnhub.io/api/v1/calendar/earnings?symbol=" + ticker
                        + "&from=" + earningsFrom + "&to=" + earningsTo + "&token=" + finnhubKey);
        CompletableFuture<HttpResponse<String>> futureSpy = alpacaClient.httpClient.sendAsync(
                alpacaClient.buildAlpacaRequest("/bars?symbols=SPY&timeframe=1Day&start=" + lookback90Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureVix = alpacaClient.httpClient.sendAsync(
                HttpRequest.newBuilder()
                        .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1d&range=5d"))
                        .header("User-Agent", "Mozilla/5.0")
                        .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureInsider =
                finnhubAsync("https://finnhub.io/api/v1/stock/insider-sentiment?symbol=" + ticker
                        + "&from=" + insiderFrom + "&to=" + insiderTo + "&token=" + finnhubKey);
        CompletableFuture<HttpResponse<String>> futureRec =
                finnhubAsync("https://finnhub.io/api/v1/stock/recommendation?symbol=" + ticker
                        + "&token=" + finnhubKey);
        CompletableFuture<HttpResponse<String>> futureFhSentiment =
                finnhubAsync("https://finnhub.io/api/v1/news-sentiment?symbol=" + ticker
                        + "&token=" + finnhubKey);
        String lookback2Years = nowET.minusDays(730).format(DateTimeFormatter.ISO_INSTANT);
        CompletableFuture<HttpResponse<String>> futureWeekly = alpacaClient.httpClient.sendAsync(
                alpacaClient.buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Week&start=" + lookback2Years + "&feed=iex"),
                HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureRsp = alpacaClient.httpClient.sendAsync(
                alpacaClient.buildAlpacaRequest("/bars?symbols=RSP&timeframe=1Day&start=" + lookback90Days + "&feed=iex"),
                HttpResponse.BodyHandlers.ofString());

        CompletableFuture.allOf(futureD1, futureH1, futureM15, futureM5, futureOptions, futureNews, futureEarnings, futureSpy, futureVix, futureInsider, futureRec, futureFhSentiment, futureWeekly, futureRsp).join();

        // ── D1 bars processing ────────────────────────────────────────────────
        if (futureD1.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureD1.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && tickerNode.size() >= 22) {
                sma20 = IndicatorUtils.calculateSmaFromBars(tickerNode, 20);
                rsi14 = IndicatorUtils.calculateRsiFromBars(tickerNode, 14);
                atr14 = IndicatorUtils.calculateAtrFromBars(tickerNode, 14);
                avgVolume30d = IndicatorUtils.calculateAvgVolumeFromBars(tickerNode, 30);

                // EMA crossover (SMA9 vs SMA21) — previously required a separate historicalTrendFunction call
                int d1sz = tickerNode.size();
                double sma9d = IndicatorUtils.calculateSmaFromBars(tickerNode, 9);
                double sma21d = IndicatorUtils.calculateSmaFromBars(tickerNode, 21);
                double prevSma9 = 0, prevSma21 = 0;
                if (d1sz >= 10) { for (int i = d1sz - 10; i < d1sz - 1; i++) prevSma9  += tickerNode.get(i).path("c").asDouble(); prevSma9  /= 9.0; }
                if (d1sz >= 22) { for (int i = d1sz - 22; i < d1sz - 1; i++) prevSma21 += tickerNode.get(i).path("c").asDouble(); prevSma21 /= 21.0; }
                if      (sma9d > sma21d && prevSma9 <= prevSma21) emaCrossoverStatus = "Bullish Cross";
                else if (sma9d < sma21d && prevSma9 >= prevSma21) emaCrossoverStatus = "Bearish Cross";
                else if (sma9d > sma21d)                          emaCrossoverStatus = "Bullish";
                else                                               emaCrossoverStatus = "Bearish";

                calculatedSupport    = atr14 > 0 ? currentPrice - (1.5 * atr14) : currentPrice * 0.96;
                calculatedResistance = atr14 > 0 ? currentPrice + (1.5 * atr14) : currentPrice * 1.04;
                // ADX (Average Directional Index) — trend strength from daily bars
                adxValue = d1sz >= 40 ? IndicatorUtils.calculateAdxFromBars(tickerNode, 14) : 25.0;
                adxTrend = adxValue >= 25 ? "Trending" : "Choppy";
                // ADX direction: rising = trend strengthening, falling = trend exhausting
                if (d1sz >= 45) {
                    double adxPrev = IndicatorUtils.calculateAdxAtOffset(tickerNode, 14, 5);
                    adxDirection = adxValue - adxPrev > 2.0 ? "RISING"
                                 : adxPrev - adxValue > 2.0 ? "FALLING" : "FLAT";
                }
                // RSI divergence: price going up but RSI weakening = hidden reversal warning
                if (d1sz >= 20) {
                    double rsi5ago = IndicatorUtils.calculateRsiAtOffset(tickerNode, 14, 5);
                    double price5ago = tickerNode.get(d1sz - 6).path("c").asDouble();
                    double pctChg5d = price5ago > 0 ? (currentPrice - price5ago) / price5ago * 100 : 0;
                    double rsiChg5d = rsi14 - rsi5ago;
                    if      (pctChg5d >  2.0 && rsiChg5d < -5.0) rsiDivergence = "BEARISH_DIV";
                    else if (pctChg5d < -2.0 && rsiChg5d >  5.0) rsiDivergence = "BULLISH_DIV";
                }
                // Rolling HRV distribution — used for IV rank, computed here while tickerNode is in scope
                if (d1sz >= 42) {
                    for (int off : new int[]{35, 28, 21, 14, 7, 0}) {
                        if (d1sz > off + 21) {
                            double sumLr2 = 0;
                            for (int j = d1sz - off - 20; j < d1sz - off; j++) {
                                double c2 = tickerNode.get(j).path("c").asDouble();
                                double pc2 = tickerNode.get(j - 1).path("c").asDouble();
                                if (c2 > 0 && pc2 > 0) { double lr = Math.log(c2 / pc2); sumLr2 += lr * lr; }
                            }
                            double rv = Math.sqrt(sumLr2 / 20.0 * 252.0);
                            if (rv < minHrv) minHrv = rv;
                            if (rv > maxHrv) maxHrv = rv;
                        }
                    }
                }
                // Prior day high/low — key institutional support/resistance levels
                if (d1sz >= 2) {
                    priorDayHigh = tickerNode.get(d1sz - 2).path("h").asDouble();
                    priorDayLow  = tickerNode.get(d1sz - 2).path("l").asDouble();
                }
                // 52-week high/low range position (0% = at 52W low, 100% = at 52W high)
                int barsForYear = Math.min(d1sz, 252);
                for (int i = d1sz - barsForYear; i < d1sz; i++) {
                    double h = tickerNode.get(i).path("h").asDouble(0);
                    double l = tickerNode.get(i).path("l").asDouble(Double.MAX_VALUE);
                    if (h > yearHigh) yearHigh = h;
                    if (l > 0 && l < yearLow) yearLow = l;
                }
                if (yearHigh > yearLow && yearLow < Double.MAX_VALUE) {
                    weekRangePosition = Math.max(0, Math.min(100,
                        (currentPrice - yearLow) / (yearHigh - yearLow) * 100));
                }
                // Stock log returns — used for beta vs SPY (last 60 trading days)
                if (d1sz >= 22) {
                    int retN = Math.min(60, d1sz - 1);
                    stockLogReturns = new double[retN];
                    for (int i = 0; i < retN; i++) {
                        int idx = d1sz - retN + i;
                        double c  = tickerNode.get(idx).path("c").asDouble();
                        double pc = tickerNode.get(idx - 1).path("c").asDouble();
                        stockLogReturns[i] = (c > 0 && pc > 0) ? Math.log(c / pc) : 0;
                    }
                }
                // 20-day Historical Realized Volatility from daily log returns
                if (d1sz >= 22) {
                    double sumLogRets = 0;
                    for (int i = d1sz - 20; i < d1sz; i++) {
                        double c = tickerNode.get(i).path("c").asDouble();
                        double pc = tickerNode.get(i - 1).path("c").asDouble();
                        if (c > 0 && pc > 0) { double lr = Math.log(c / pc); sumLogRets += lr * lr; }
                    }
                    hrv = Math.min(Math.sqrt(sumLogRets / 20.0 * 252.0), 0.60);
                }
                double priceVsSmaScore = sma20 > 0 ? IndicatorUtils.normalizeScore((currentPrice - sma20) / sma20, 2500.0) : 0.0;
                double rsiSignalScore = (rsi14 - 50.0) * 2.0;
                dailyScore = (priceVsSmaScore * 0.60) + (rsiSignalScore * 0.40);
                if (atr14 > 0 && lowToday <= 0.0) {
                    microSupport = currentPrice - (1.5 * atr14);
                }
                if (atr14 > 0 && highToday <= 0.0) {
                    microResistance = currentPrice + (1.5 * atr14);
                }

                // Swing High/Low detection — scan daily bars for local peaks/troughs (5-bar window)
                if (d1sz >= 10) {
                    List<Double> swingHighsList = new ArrayList<>();
                    List<Double> swingLowsList  = new ArrayList<>();
                    int win = 2;
                    for (int i = win; i < d1sz - win; i++) {
                        double h = tickerNode.get(i).path("h").asDouble();
                        double l = tickerNode.get(i).path("l").asDouble();
                        boolean isHigh = true, isLow = true;
                        for (int j = 1; j <= win; j++) {
                            if (tickerNode.get(i - j).path("h").asDouble() >= h) isHigh = false;
                            if (tickerNode.get(i + j).path("h").asDouble() >= h) isHigh = false;
                            if (tickerNode.get(i - j).path("l").asDouble() <= l) isLow  = false;
                            if (tickerNode.get(i + j).path("l").asDouble() <= l) isLow  = false;
                        }
                        if (isHigh) swingHighsList.add(h);
                        if (isLow)  swingLowsList.add(l);
                    }
                    double cp = currentPrice;
                    double sr = swingHighsList.stream()
                        .filter(h -> h > cp * 1.003)
                        .min(Double::compareTo).orElse(0.0);
                    double ss = swingLowsList.stream()
                        .filter(l -> l < cp * 0.997)
                        .max(Double::compareTo).orElse(0.0);
                    swingResistance = sr;
                    swingSupport    = ss;
                    if (ss > 0) {
                        double tol = ss * 0.02;
                        swingSupportStrength = (int) swingLowsList.stream()
                            .filter(l -> Math.abs(l - ss) <= tol).count();
                    }
                    if (sr > 0) {
                        double tol = sr * 0.02;
                        swingResistanceStrength = (int) swingHighsList.stream()
                            .filter(h -> Math.abs(h - sr) <= tol).count();
                    }
                }
                // MA stack score — how many of EMA8>EMA21>EMA50>EMA200 are aligned (0–4)
                if (d1sz >= 50) {
                    ema8d   = IndicatorUtils.calculateEmaFromBars(tickerNode, 8);
                    ema21d  = IndicatorUtils.calculateEmaFromBars(tickerNode, 21);
                    ema50d  = IndicatorUtils.calculateEmaFromBars(tickerNode, 50);
                    ema200d = d1sz >= 200 ? IndicatorUtils.calculateEmaFromBars(tickerNode, 200) : 0.0;
                    if (currentPrice > ema8d)               maStackScore++;
                    if (ema8d        > ema21d)              maStackScore++;
                    if (ema21d       > ema50d)              maStackScore++;
                    if (ema200d > 0 && ema50d > ema200d)    maStackScore++;
                    // If EMA200 unavailable cap at 3
                    int maxStack = ema200d > 0 ? 4 : 3;
                    maStackLabel = (maStackScore == maxStack)         ? "Full Bull"
                                 : (maStackScore >= maxStack - 1)     ? "Strong"
                                 : (maStackScore == 2)                ? "Mixed"
                                 : (maStackScore == 1)                ? "Weak"
                                 :                                      "Full Bear";
                }
            }
        }

        // ── H1 bars processing ────────────────────────────────────────────────
        if (futureH1.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureH1.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && tickerNode.size() >= 26) {
                double ema12h = IndicatorUtils.calculateEmaFromBars(tickerNode, 12);
                double ema26h = IndicatorUtils.calculateEmaFromBars(tickerNode, 26);
                double macdH = ema12h - ema26h;
                macdH1 = macdH;
                double rsiH1 = IndicatorUtils.calculateRsiFromBars(tickerNode, 9);
                double macdScore = currentPrice > 0 ? IndicatorUtils.normalizeScore(macdH / currentPrice, 5000.0) : 0.0;
                double priceVsEmaScore = ema26h > 0 ? IndicatorUtils.normalizeScore((currentPrice - ema26h) / ema26h, 2500.0) : 0.0;
                double rsiH1Score = (rsiH1 - 50.0) * 2.0;
                h1Score = (priceVsEmaScore * 0.40) + (macdScore * 0.35) + (rsiH1Score * 0.25);
            }
        }

        // ── M15 bars processing ───────────────────────────────────────────────
        if (futureM15.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureM15.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && tickerNode.size() >= 21) {
                double sma9m15 = IndicatorUtils.calculateSmaFromBars(tickerNode, 9);
                double sma21m15 = IndicatorUtils.calculateSmaFromBars(tickerNode, 21);
                double rsiM15 = IndicatorUtils.calculateRsiFromBars(tickerNode, 9);
                int sz15 = tickerNode.size();
                double c1m15 = tickerNode.get(sz15 - 1).path("c").asDouble();
                double c2m15 = tickerNode.get(sz15 - 2).path("c").asDouble();
                double c3m15 = sz15 >= 3 ? tickerNode.get(sz15 - 3).path("c").asDouble() : c2m15;
                double smaCrossScore = sma21m15 > 0 ? IndicatorUtils.normalizeScore((sma9m15 - sma21m15) / sma21m15, 3000.0) : 0.0;
                double rsiM15Score = (rsiM15 - 50.0) * 2.0;
                double momentumScore = (c1m15 > c2m15 && c2m15 > c3m15) ? 40.0 : (c1m15 < c2m15 && c2m15 < c3m15) ? -40.0 : 0.0;
                m15Score = (smaCrossScore * 0.50) + (rsiM15Score * 0.30) + (momentumScore * 0.20);
            }
        }

        // ── M5 bars + VWAP calculation ────────────────────────────────────────
        if (futureM5.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureM5.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && !tickerNode.isEmpty()) {
                int size = tickerNode.size();
                JsonNode lastCandle = tickerNode.get(size - 1);

                long lastCandleTime = Instant.parse(lastCandle.path("t").asText()).getEpochSecond();
                long startOfDayEpoch = Instant.ofEpochSecond(lastCandleTime).atZone(ZoneId.of("America/New_York")).toLocalDate().atStartOfDay(ZoneId.of("America/New_York")).toEpochSecond();

                double cumulativeTPV = 0; double cumulativeVol = 0;
                List<double[]> tpvPairs = new ArrayList<>();
                for(int i = 0; i < size; i++) {
                    JsonNode bar = tickerNode.get(i);
                    if(Instant.parse(bar.path("t").asText()).getEpochSecond() >= startOfDayEpoch) {
                        double h = bar.path("h").asDouble();
                        double l = bar.path("l").asDouble();
                        double tp = (h + l + bar.path("c").asDouble()) / 3.0;
                        long v = bar.path("v").asLong();
                        cumulativeTPV += tp * v;
                        cumulativeVol += v;
                        tpvPairs.add(new double[]{tp, v});
                    }
                }
                if (cumulativeVol > 0) {
                    vwap = cumulativeTPV / cumulativeVol;
                    double sumDevSq = 0;
                    for (double[] pair : tpvPairs) { double dev = pair[0] - vwap; sumDevSq += pair[1] * dev * dev; }
                    double vwapStd = Math.sqrt(sumDevSq / cumulativeVol);
                    vwapUpper = vwap + vwapStd;
                    vwapLower = vwap - vwapStd;
                }
                double m5c1 = lastCandle.path("c").asDouble();
                double m5c2 = size >= 2 ? tickerNode.get(size - 2).path("c").asDouble() : m5c1;
                double m5c3 = size >= 3 ? tickerNode.get(size - 3).path("c").asDouble() : m5c2;
                double vwapDistScore = vwap > 0 ? IndicatorUtils.normalizeScore((currentPrice - vwap) / vwap, 3000.0) : 0.0;
                double m5Momentum = (m5c1 > m5c2 && m5c2 > m5c3) ? 50.0 : (m5c1 < m5c2 && m5c2 < m5c3) ? -50.0 : (m5c1 > m5c2) ? 20.0 : -20.0;
                m5Score = (vwapDistScore * 0.55) + (m5Momentum * 0.45);
            }
        }

        if (Math.abs(currentPrice - vwap) / currentPrice > 0.08) {
            vwap = currentPrice;
        }

        // ── Weekly bars processing — higher-timeframe trend bias ─────────────────
        try {
            if (futureWeekly.get().statusCode() == 200) {
                JsonNode wNode = objectMapper.readTree(futureWeekly.get().body()).path("bars").path(ticker);
                if (wNode.isArray() && wNode.size() >= 21) {
                    int wsz = wNode.size();
                    double wClose = wNode.get(wsz - 1).path("c").asDouble();
                    weeklyEma20 = IndicatorUtils.calculateEmaFromBars(wNode, 20);
                    weeklyEma50 = wsz >= 50 ? IndicatorUtils.calculateEmaFromBars(wNode, 50) : 0.0;
                    // Slope proxy: compare last 4 weekly closes to 4 closes before that
                    double wRecent4 = 0, wPrior4 = 0;
                    for (int i = wsz - 4; i < wsz;     i++) wRecent4 += wNode.get(i).path("c").asDouble();
                    for (int i = wsz - 8; i < wsz - 4; i++) wPrior4  += wNode.get(i).path("c").asDouble();
                    boolean wSlope = wRecent4 > wPrior4;
                    boolean wAboveEma20 = wClose > weeklyEma20;
                    boolean wEma20AboveEma50 = weeklyEma50 > 0 && weeklyEma20 > weeklyEma50;
                    if      (wAboveEma20 && wEma20AboveEma50 && wSlope)  weeklyBias = "BULLISH";
                    else if (wAboveEma20 && wEma20AboveEma50)             weeklyBias = "BULLISH";
                    else if (!wAboveEma20 && !wEma20AboveEma50 && !wSlope) weeklyBias = "BEARISH";
                    else if (!wAboveEma20 && !wEma20AboveEma50)            weeklyBias = "BEARISH";
                    else                                                   weeklyBias = "NEUTRAL";
                }
            }
        } catch (Exception ignored) {}

        // ── News sentiment scoring ────────────────────────────────────────────
        double sentimentScore = 0.0;
        try {
            if (futureNews.get().statusCode() == 200) {
                JsonNode newsRoot = objectMapper.readTree(futureNews.get().body());
                JsonNode articles = newsRoot.path("news");
                if (articles.isArray() && !articles.isEmpty()) {
                    Set<String> bullishKw = Set.of("beat","beats","upgrade","upgraded","record","surge",
                            "surges","growth","raises","strong","exceed","exceeds","bullish",
                            "profit","gains","rally","outperform","expansion","raised","positive","buy");
                    Set<String> bearishKw = Set.of("miss","misses","downgrade","downgraded","cut","cuts",
                            "concern","concerns","lawsuit","investigation","warn","warns","bearish",
                            "loss","losses","decline","disappoints","underperform","layoffs",
                            "recall","fraud","penalty","sell");
                    int bullCount = 0, bearCount = 0, articlesProcessed = 0;
                    for (JsonNode article : articles) {
                        // Time-decay: newer articles get more weight
                        String createdAt = article.path("created_at").asText("");
                        double ageWeight = 1.0;
                        if (!createdAt.isEmpty()) {
                            try {
                                long ageMinutes = (System.currentTimeMillis() / 1000 -
                                        Instant.parse(createdAt).getEpochSecond()) / 60;
                                ageWeight = ageMinutes < 30 ? 2.0 : ageMinutes < 120 ? 1.5 : 1.0;
                            } catch (Exception ignored2) {}
                        }
                        String text = (article.path("headline").asText("") + " "
                                + article.path("summary").asText("")).toLowerCase();
                        for (String w : text.split("\\W+")) {
                            if (bullishKw.contains(w)) bullCount += (int)(ageWeight * 1);
                            else if (bearishKw.contains(w)) bearCount += (int)(ageWeight * 1);
                        }
                        articlesProcessed++;
                    }
                    int total = bullCount + bearCount;
                    if (total > 0) {
                        double buzzMultiplier = Math.min(1.5, 1.0 + (articlesProcessed - 1) * 0.05);
                        sentimentScore = Math.max(-100.0, Math.min(100.0,
                                ((double)(bullCount - bearCount) / total) * buzzMultiplier * 100.0));
                    }
                }
            }
        } catch (Exception ignored) {}

        // Blend Alpaca keyword score with Finnhub structured bullish/bearish percentage
        try {
            if (futureFhSentiment.get() != null && futureFhSentiment.get().statusCode() == 200) {
                JsonNode fhRoot = objectMapper.readTree(futureFhSentiment.get().body());
                double bull = fhRoot.path("sentiment").path("bullishPercent").asDouble(-1);
                if (bull >= 0) {
                    finnhubBullishPct    = bull * 100.0;
                    finnhubNewsSentLabel = finnhubBullishPct >= 65 ? "Bullish"
                                        : finnhubBullishPct <= 35 ? "Bearish" : "Neutral";
                    double fhScore = (finnhubBullishPct - 50.0) * 2.0; // normalize to ±100
                    // Finnhub scored data (65%) outweighs Alpaca keyword guessing (35%)
                    sentimentScore = (fhScore * 0.65) + (sentimentScore * 0.35);
                    sentimentScore = Math.max(-100, Math.min(100, sentimentScore));
                }
            }
        } catch (Exception ignored) {}

        // ── Smart money: insider sentiment + analyst consensus ────────────────
        double smartMoneyScore = 0.0;
        double insiderMspr    = 0.0;
        int    insiderBuys    = 0, insiderSells = 0;
        int    analystBuy     = 0, analystHold = 0, analystSell = 0;

        try {
            if (futureInsider.get() != null && futureInsider.get().statusCode() == 200) {
                JsonNode insiderNode = objectMapper.readTree(futureInsider.get().body());
                JsonNode data = insiderNode.path("data");
                if (data.isArray() && !data.isEmpty()) {
                    double totalMspr = 0; int cnt = 0;
                    for (JsonNode month : data) {
                        totalMspr += month.path("mspr").asDouble(0);
                        int ch = month.path("change").asInt(0);
                        if (ch > 0) insiderBuys++; else if (ch < 0) insiderSells++;
                        cnt++;
                    }
                    if (cnt > 0) {
                        insiderMspr = totalMspr / cnt;
                        // Finnhub MSPR is in -100 to +100; scale to ±50 contribution
                        smartMoneyScore += Math.max(-50.0, Math.min(50.0, insiderMspr * 0.5));
                    }
                }
            }
        } catch (Exception ignored) {}

        try {
            if (futureRec.get() != null && futureRec.get().statusCode() == 200) {
                JsonNode recArray = objectMapper.readTree(futureRec.get().body());
                if (recArray.isArray() && !recArray.isEmpty()) {
                    JsonNode latest = recArray.get(0);
                    analystBuy  = latest.path("strongBuy").asInt(0) + latest.path("buy").asInt(0);
                    analystHold = latest.path("hold").asInt(0);
                    analystSell = latest.path("strongSell").asInt(0) + latest.path("sell").asInt(0);
                    int total   = analystBuy + analystHold + analystSell;
                    if (total > 0) {
                        // Net analyst conviction, scaled to ±50 contribution
                        smartMoneyScore += Math.max(-50.0, Math.min(50.0,
                                ((double)(analystBuy - analystSell) / total) * 100.0));
                    }
                }
            }
        } catch (Exception ignored) {}

        smartMoneyScore = Math.max(-100.0, Math.min(100.0, smartMoneyScore));

        String smartMoneyVerdict = smartMoneyScore > 25 ? "ACCUMULATING" :
                                   smartMoneyScore < -25 ? "DISTRIBUTING" : "NEUTRAL";

        // Timeframe agreement: how many of D1/H1/15M/5M align on direction (-4 to +4)
        boolean d1TrendUp   = "Bullish".equals(emaCrossoverStatus) || "Bullish Cross".equals(emaCrossoverStatus);
        boolean d1TrendDown = "Bearish".equals(emaCrossoverStatus) || "Bearish Cross".equals(emaCrossoverStatus);
        if (d1TrendUp)   tfAgreement++; else if (d1TrendDown) tfAgreement--;
        if (h1Score  >  15) tfAgreement++; else if (h1Score  < -15) tfAgreement--;
        if (m15Score >  15) tfAgreement++; else if (m15Score < -15) tfAgreement--;
        if (m5Score  >  15) tfAgreement++; else if (m5Score  < -15) tfAgreement--;

        // Weighted confluence — smart money gets 12%, news sentiment 8%
        double totalConfluenceScore;
        if (customDays > 10) {
            totalConfluenceScore = (dailyScore * 0.55) + (h1Score * 0.25) + (sentimentScore * 0.08) + (smartMoneyScore * 0.12);
        } else if (customDays > 3) {
            totalConfluenceScore = (dailyScore * 0.39) + (h1Score * 0.24) + (m15Score * 0.17) + (sentimentScore * 0.08) + (smartMoneyScore * 0.12);
        } else {
            totalConfluenceScore = (dailyScore * 0.31) + (h1Score * 0.24) + (m15Score * 0.16) + (m5Score * 0.09) + (sentimentScore * 0.08) + (smartMoneyScore * 0.12);
        }

        // Amplify or dampen conviction based on today's volume vs 30-day average
        if (avgVolume30d > 0 && totalVolume > 0) {
            double volRatio = (double) totalVolume / avgVolume30d;
            double volumeMultiplier = volRatio > 2.0 ? 1.20 : volRatio > 1.3 ? 1.10 : volRatio < 0.4 ? 0.80 : volRatio < 0.7 ? 0.90 : 1.0;
            totalConfluenceScore = Math.max(-100.0, Math.min(100.0, totalConfluenceScore * volumeMultiplier));
        }

        // Alignment check: if smart money strongly contradicts technicals, dampen conviction
        boolean smConflict = (smartMoneyScore > 30 && totalConfluenceScore < -15)
                          || (smartMoneyScore < -30 && totalConfluenceScore > 15);
        if (smConflict) {
            totalConfluenceScore *= 0.70; // reduce by 30% when big money disagrees with chart
        }

        // ── Earnings event awareness ──────────────────────────────────────────
        try {
            if (futureEarnings.get() != null && futureEarnings.get().statusCode() == 200) {
                JsonNode earningsRoot = objectMapper.readTree(futureEarnings.get().body());
                JsonNode earningsData = earningsRoot.path("earningsCalendar");
                if (earningsData.isArray()) {
                    for (JsonNode event : earningsData) {
                        String dateStr = event.path("date").asText("");
                        if (!dateStr.isEmpty()) {
                            LocalDate eventDate = LocalDate.parse(dateStr);
                            int daysAway = (int)(eventDate.toEpochDay() - today.toEpochDay());
                            LocalDate earningsToDate = LocalDate.parse(earningsTo);
                            if (daysAway >= 0 && !eventDate.isAfter(earningsToDate)) {
                                earningsFlag = true;
                                earningsDate = dateStr;
                                earningsDaysAway = daysAway;
                                break;
                            }
                        }
                    }
                }
            }
        } catch (Exception ignored) {}

        // ── Market regime: SPY trend + VIX fear gauge ─────────────────────────
        try {
            if (futureSpy.get().statusCode() == 200) {
                JsonNode spyBars = objectMapper.readTree(futureSpy.get().body()).path("bars").path("SPY");
                if (spyBars.isArray() && spyBars.size() >= 22) {
                    double spySma20 = IndicatorUtils.calculateSmaFromBars(spyBars, 20);
                    double spyRsi   = IndicatorUtils.calculateRsiFromBars(spyBars, 14);
                    double spyPrice = spyBars.get(spyBars.size() - 1).path("c").asDouble();
                    double smaScore = spySma20 > 0 ? ((spyPrice - spySma20) / spySma20) * 250.0 : 0;
                    double rsiScore = (spyRsi - 50.0) * 1.5;
                    spyScore = Math.max(-100, Math.min(100, (smaScore * 0.6) + (rsiScore * 0.4)));
                    spyTrend = spyScore > 20 ? "Bull" : spyScore < -20 ? "Bear" : "Neutral";
                    // Beta vs SPY — covariance of stock returns over SPY variance (last 60 bars)
                    if (stockLogReturns.length >= 20) {
                        int n = Math.min(stockLogReturns.length, spyBars.size() - 1);
                        double[] spyRets = new double[n];
                        int spySz = spyBars.size();
                        if (spySz >= 21) {
                            double spyLatest = spyBars.get(spySz - 1).path("c").asDouble();
                            double spy20ago  = spyBars.get(spySz - 21).path("c").asDouble();
                            spy20dReturn = spy20ago > 0 ? (spyLatest - spy20ago) / spy20ago * 100.0 : 0.0;
                        }
                        for (int i = 0; i < n; i++) {
                            int idx = spySz - n + i;
                            double c  = spyBars.get(idx).path("c").asDouble();
                            double pc = spyBars.get(idx - 1).path("c").asDouble();
                            spyRets[i] = (c > 0 && pc > 0) ? Math.log(c / pc) : 0;
                        }
                        double stockMean = 0, spyMean = 0;
                        for (int i = 0; i < n; i++) { stockMean += stockLogReturns[i]; spyMean += spyRets[i]; }
                        stockMean /= n; spyMean /= n;
                        double cov = 0, spyVar = 0;
                        for (int i = 0; i < n; i++) {
                            cov    += (stockLogReturns[i] - stockMean) * (spyRets[i] - spyMean);
                            spyVar += (spyRets[i] - spyMean) * (spyRets[i] - spyMean);
                        }
                        if (spyVar > 0) beta = Math.max(-5.0, Math.min(5.0, cov / spyVar));
                    }
                }
            }
        } catch (Exception ignored) {}

        try {
            if (futureVix.get().statusCode() == 200) {
                JsonNode vixMeta = objectMapper.readTree(futureVix.get().body())
                        .path("chart").path("result").path(0).path("meta");
                vixLevel = vixMeta.path("regularMarketPrice").asDouble(20.0);
            }
        } catch (Exception ignored) {}

        // Compute final regime label and apply multiplier to confluenceScore
        double regimeMultiplier = 1.0;
        if (vixLevel > 30) {
            regimeMultiplier = totalConfluenceScore > 0 ? 0.65 : 1.15;
            marketRegime = "HIGH FEAR — VIX " + String.format("%.1f", vixLevel);
        } else if (vixLevel > 22 && spyScore < -15) {
            regimeMultiplier = totalConfluenceScore > 0 ? 0.80 : 1.10;
            marketRegime = "RISK-OFF — VIX " + String.format("%.1f", vixLevel) + " · SPY " + spyTrend;
        } else if (spyScore > 20 && vixLevel < 20) {
            regimeMultiplier = totalConfluenceScore > 0 ? 1.10 : 0.90;
            marketRegime = "BULL MARKET — VIX " + String.format("%.1f", vixLevel) + " · SPY " + spyTrend;
        } else {
            marketRegime = "NEUTRAL — VIX " + String.format("%.1f", vixLevel) + " · SPY " + spyTrend;
        }
        totalConfluenceScore = Math.max(-100.0, Math.min(100.0, totalConfluenceScore * regimeMultiplier));
        String regimeNote = vixLevel > 30 ? "⚠️ HIGH FEAR — reduce position size, widen stops"
                          : (vixLevel > 22 && spyScore < -15) ? "📊 Elevated volatility — be selective"
                          : "✅ Normal conditions";

        // ── Market breadth via RSP (equal-weight S&P 500) ────────────────────────
        try {
            if (futureRsp.get().statusCode() == 200) {
                JsonNode rspBars = objectMapper.readTree(futureRsp.get().body()).path("bars").path("RSP");
                if (rspBars.isArray() && rspBars.size() >= 21) {
                    int rspSz = rspBars.size();
                    double rspLatest = rspBars.get(rspSz - 1).path("c").asDouble();
                    double rsp20ago  = rspBars.get(rspSz - 21).path("c").asDouble();
                    double rsp20dReturn = rsp20ago > 0 ? (rspLatest - rsp20ago) / rsp20ago * 100.0 : 0.0;
                    breadthRs = rsp20dReturn - spy20dReturn; // +ve = broad participation, -ve = narrow leadership
                    if      (breadthRs >  0.5) marketBreadth = "STRONG";
                    else if (breadthRs < -0.5) marketBreadth = "NARROW";
                    else                       marketBreadth = "NEUTRAL";
                }
            }
        } catch (Exception ignored) {}

        // ── Composite trend score (0–100): weekly + MA stack + ADX slope + breadth + TF ──
        {
            int raw = 0;
            // Weekly bias: most important — ±30
            if      ("BULLISH".equals(weeklyBias)) raw += 30;
            else if ("BEARISH".equals(weeklyBias)) raw -= 30;
            // MA stack: ±20 (centered at 2)
            raw += (maStackScore - 2) * 10;
            // ADX slope: trend strengthening vs exhausting — ±10
            if      ("RISING".equals(adxDirection))  raw += 10;
            else if ("FALLING".equals(adxDirection)) raw -= 10;
            // Market breadth: ±10
            if      ("STRONG".equals(marketBreadth)) raw += 10;
            else if ("NARROW".equals(marketBreadth)) raw -= 10;
            // TF agreement (-4 to +4) → ±8
            raw += tfAgreement * 2;
            // Normalize to 0–100 (raw range: -78 to +78)
            compositeTrendScore = (int) Math.round(Math.max(0, Math.min(100, (raw + 78.0) / 156.0 * 100.0)));
            compositeTrendLabel = compositeTrendScore >= 70 ? "Strong Uptrend"
                                : compositeTrendScore >= 55 ? "Uptrend"
                                : compositeTrendScore >= 45 ? "Neutral"
                                : compositeTrendScore >= 30 ? "Downtrend"
                                :                             "Strong Downtrend";
        }

        // ── Swing trade opportunity ───────────────────────────────────────────
        // Evaluated after confluence score is finalised
        String swingTradeSignal = "NONE";
        double swingEntry = 0, swingTarget = 0, swingStop = 0;
        String swingStrategy = "";
        String swingNote = "";
        if (swingSupport > 0 && swingResistance > 0) {
            double distToSupport    = (currentPrice - swingSupport)    / currentPrice;
            double distToResistance = (swingResistance - currentPrice) / currentPrice;
            boolean nearSupport    = distToSupport    >= 0 && distToSupport    <= 0.04;
            boolean nearResistance = distToResistance >= 0 && distToResistance <= 0.04;
            // Range-bound: price between swing levels, ADX low → intraday range play
            boolean rangeBound = adxValue < 20 && distToSupport > 0.01 && distToResistance > 0.01;
            if (nearSupport && totalConfluenceScore > -40) {
                swingTradeSignal = "SWING_LONG";
                swingEntry   = Math.round(swingSupport * 1.005 * 100.0) / 100.0;
                swingTarget  = swingResistance;
                swingStop    = Math.round(swingSupport * 0.97  * 100.0) / 100.0;
                swingStrategy = "Bull Call Debit Spread";
                swingNote    = String.format("Price is %.1f%% above swing support — buy near $%.2f, target $%.2f", distToSupport * 100, swingSupport, swingResistance);
            } else if (nearResistance && totalConfluenceScore < 40) {
                swingTradeSignal = "SWING_SHORT";
                swingEntry   = Math.round(swingResistance * 0.995 * 100.0) / 100.0;
                swingTarget  = swingSupport;
                swingStop    = Math.round(swingResistance * 1.03  * 100.0) / 100.0;
                swingStrategy = "Bear Put Debit Spread";
                swingNote    = String.format("Price is %.1f%% below swing resistance — short near $%.2f, target $%.2f", distToResistance * 100, swingResistance, swingSupport);
            } else if (rangeBound) {
                swingTradeSignal = "RANGE_PLAY";
                swingEntry   = swingSupport;
                swingTarget  = swingResistance;
                swingStop    = Math.round(swingSupport * 0.97 * 100.0) / 100.0;
                swingStrategy = "Iron Condor";
                swingNote    = String.format("ADX %.0f — stock ranging between $%.2f and $%.2f, sell both ends", adxValue, swingSupport, swingResistance);
            }
        }

        // ── Implied volatility + unusual options activity from snapshots ──────
        double impliedVolatility = 0.0;
        if (futureOptions.get() != null && futureOptions.get().statusCode() == 200) {
            JsonNode snapshots = objectMapper.readTree(futureOptions.get().body()).path("snapshots");
            double totalIv = 0, totalOi = 0;
            double callVol = 0, putVol = 0, callOi = 0, putOi = 0;
            if (snapshots.isObject()) {
                var iterator = snapshots.fields();
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    String sym = entry.getKey();
                    JsonNode contract = entry.getValue();
                    double iv = contract.path("implied_volatility").asDouble(0);
                    double oi = Math.max(1.0, contract.path("open_interest").asDouble(1.0));
                    double vol = contract.path("greeks").path("delta").asDouble(0) != 0
                        ? contract.path("day").path("volume").asDouble(0) : 0;
                    // Accept only realistic ATM IV range; OI-weighted average
                    if (iv > 0.05 && iv < 0.80) {
                        totalIv += iv * oi;
                        totalOi += oi;
                    }
                    // Track call vs put volume and OI for unusual activity detection
                    boolean isCall = sym.length() > 6 && sym.charAt(sym.length() - 9) == 'C';
                    if (isCall) { callVol += vol; callOi += oi; }
                    else        { putVol  += vol; putOi  += oi; }
                }
            }
            if (totalOi > 0) impliedVolatility = totalIv / totalOi;
            // Unusual activity: volume/OI ratio > 2× on either side = smart money positioning
            double callVolOiRatio = callOi > 0 ? callVol / callOi : 0;
            double putVolOiRatio  = putOi  > 0 ? putVol  / putOi  : 0;
            if      (callVolOiRatio > 2.0 && putVolOiRatio <= 1.0) unusualOptionsActivity = "UNUSUAL_CALL_BUYING";
            else if (putVolOiRatio  > 2.0 && callVolOiRatio <= 1.0) unusualOptionsActivity = "UNUSUAL_PUT_BUYING";
            else if (callVolOiRatio > 2.0 && putVolOiRatio  > 2.0)  unusualOptionsActivity = "UNUSUAL_BOTH";
        }

        // Blend options IV with 20-day HRV: 55% options, 45% realized — anchors to actual movement.
        // If options data unavailable, fall straight back to HRV.
        if (impliedVolatility > 0) {
            impliedVolatility = (impliedVolatility * 0.55) + (hrv * 0.45);
        } else {
            impliedVolatility = hrv;
        }
        impliedVolatility = Math.min(impliedVolatility, 0.55); // hard cap at 55%

        // IV rank: where current IV sits in the recent realized vol distribution (0 = cheap, 100 = expensive)
        if (maxHrv > minHrv && maxHrv > 0 && minHrv < Double.MAX_VALUE) {
            ivRank = Math.min(100, Math.max(0, (impliedVolatility - minHrv) / (maxHrv - minHrv) * 100));
        }
        ivRegime = ivRank > 70 ? "HIGH" : ivRank < 30 ? "LOW" : "NORMAL";
        earningsIvInflated = earningsFlag && earningsDaysAway <= 5 && ivRank > 60;

        // Realized vol is the actual driver of expected move (IV overstates by ~15%)
        double realizedVolEstimate = impliedVolatility * 0.85;

        double oneDayExpectedMove = currentPrice * realizedVolEstimate * Math.sqrt(1.0 / 252.0);
        double fiveDayExpectedMove = currentPrice * realizedVolEstimate * Math.sqrt(5.0 / 252.0);

        // When no custom timeframe requested, default to 15 trading days so the range is always populated
        int effectiveCustomDays = customDays > 0 ? customDays : 15;
        double customExpectedMove = currentPrice * realizedVolEstimate * Math.sqrt(effectiveCustomDays / 252.0);

        double rangeAnchor = currentPrice;
        if (totalConfluenceScore <= -40.0) {
            rangeAnchor = currentPrice - (oneDayExpectedMove * 0.35);
        } else if (totalConfluenceScore >= 40.0) {
            rangeAnchor = currentPrice + (oneDayExpectedMove * 0.35);
        }

        double tomorrowUpper = rangeAnchor + oneDayExpectedMove;
        double tomorrowLower = rangeAnchor - oneDayExpectedMove;
        double nextWeekUpper = rangeAnchor + fiveDayExpectedMove;
        double nextWeekLower = rangeAnchor - fiveDayExpectedMove;

        double customUpper = rangeAnchor + customExpectedMove;
        double customLower = rangeAnchor - customExpectedMove;

        // ── ATR-based trade sizing ────────────────────────────────────────────
        // Scale ATR multiplier and R:R to the trade timeframe.
        // Daily ATR is the natural unit of market noise — stop must sit outside it.
        double atrStopMult;
        double rrRatio = Math.abs(totalConfluenceScore) >= 70 ? 3.5
                       : Math.abs(totalConfluenceScore) >= 40 ? 2.5
                       : 1.8;
        if (customDays == 0) {
            atrStopMult = 1.0;   // intraday: 1× ATR stop
        } else if (customDays <= 5) {
            atrStopMult = 1.25;  // short swing: 1.25× ATR
        } else if (customDays <= 21) {
            atrStopMult = 1.5;   // medium swing: 1.5× ATR
        } else {
            atrStopMult = 2.0;   // longer-term position: 2× ATR
        }

        // Stop distance — ATR-based with 1% floor; fall back to IV move if ATR unavailable
        double minStopFloor   = currentPrice * 0.01;
        double stopDistance   = atr14 > 0
                ? Math.max(atr14 * atrStopMult, minStopFloor)
                : Math.max(oneDayExpectedMove * 1.5, minStopFloor);
        double targetDistance = stopDistance * rrRatio;

        double dynamicEntry = currentPrice;
        double dynamicSl    = currentPrice;
        double dynamicTp    = currentPrice;
        String dynamicVerdict;

        if (totalConfluenceScore >= 70.0) {
            // Strong bull signal — enter now at market, hold for full target
            dynamicVerdict = "EXECUTE_CALL_OR_LONG_SPREAD";
            dynamicEntry   = currentPrice;
            dynamicTp      = dynamicEntry + targetDistance;
            dynamicSl      = dynamicEntry - stopDistance;

        } else if (totalConfluenceScore >= 15.0) {
            // Bullish bias — wait for a dip to VWAP before entering
            dynamicVerdict = "PREPARE_LONG_BUY_DIP_AT_VWAP";
            dynamicEntry   = (vwap > 0 && vwap < currentPrice) ? vwap : currentPrice - (stopDistance * 0.3);
            dynamicTp      = dynamicEntry + targetDistance;
            dynamicSl      = dynamicEntry - stopDistance;

        } else if (totalConfluenceScore <= -70.0) {
            // Strong bear signal — enter short now at market
            dynamicVerdict = "EXECUTE_PUT_OR_SHORT_SPREAD";
            dynamicEntry   = currentPrice;
            dynamicTp      = dynamicEntry - targetDistance;
            dynamicSl      = dynamicEntry + stopDistance;

        } else if (totalConfluenceScore <= -15.0) {
            // Bearish bias — wait for a bounce to VWAP before fading
            dynamicVerdict = "PREPARE_SHORT_FADE_BOUNCE_AT_VWAP";
            dynamicEntry   = (vwap > 0 && vwap > currentPrice) ? vwap : currentPrice + (stopDistance * 0.3);
            dynamicTp      = dynamicEntry - targetDistance;
            dynamicSl      = dynamicEntry + stopDistance;

        } else {
            // Sideways — sell premium within the expected daily range
            double premiumWidth = atr14 > 0 ? atr14 * 0.75 : oneDayExpectedMove;
            dynamicVerdict = "STAND_DOWN_COLLECT_PREMIUM";
            dynamicEntry   = currentPrice;
            dynamicTp      = currentPrice + premiumWidth;
            dynamicSl      = currentPrice - premiumWidth;
        }

        double strikeBuy = IndicatorUtils.getNearestOptionStrike(dynamicEntry);
        double strikeSell = IndicatorUtils.getNearestOptionStrike(dynamicTp);

        // Realistic debit spread width: ~5–10% of price, capped at 1× ATR, minimum $2.50
        double rawSpreadWidth = currentPrice >= 300 ? 10.0
                              : currentPrice >= 100 ? 5.0
                              : currentPrice >= 50  ? 2.50
                              :                       1.0;
        if (atr14 > 0) rawSpreadWidth = Math.min(rawSpreadWidth, atr14 * 0.5);
        double spreadWidth = Math.max(rawSpreadWidth, 2.50);

        // Short leg of the debit spread — a few strikes from entry, not at the full TP
        double spreadShortStrike;
        if (totalConfluenceScore >= 15.0) {
            spreadShortStrike = IndicatorUtils.getNearestOptionStrike(strikeBuy + spreadWidth);   // call spread: sell OTM call above
        } else if (totalConfluenceScore <= -15.0) {
            spreadShortStrike = IndicatorUtils.getNearestOptionStrike(strikeBuy - spreadWidth);   // put spread:  sell OTM put below
        } else {
            spreadShortStrike = strikeSell; // iron condor uses IC legs below
        }

        // Iron Condor legs — used when verdict is STAND_DOWN_COLLECT_PREMIUM
        double icWing = atr14 > 0 ? atr14 * 0.5 : oneDayExpectedMove * 0.5;
        double icPutSell  = IndicatorUtils.getNearestOptionStrike(microSupport);
        double icPutBuy   = IndicatorUtils.getNearestOptionStrike(microSupport - icWing);
        double icCallSell = IndicatorUtils.getNearestOptionStrike(microResistance);
        double icCallBuy  = IndicatorUtils.getNearestOptionStrike(microResistance + icWing);

        // Credit spread legs — Option C for both directions
        // Bull Put Credit Spread (bullish + HIGH IV): sell put at support, buy lower put
        double creditPutSell = IndicatorUtils.getNearestOptionStrike(microSupport);
        double creditPutBuy  = IndicatorUtils.getNearestOptionStrike(microSupport - spreadWidth);
        // Bear Call Credit Spread (bearish + HIGH IV): sell call at resistance, buy higher call
        double creditCallSell = IndicatorUtils.getNearestOptionStrike(microResistance);
        double creditCallBuy  = IndicatorUtils.getNearestOptionStrike(microResistance + spreadWidth);

        // ── Symmetric buy / sell signal checklist (6 signals each direction) ──
        // Buy and sell are scored independently so both directions are equally rigorous.
        int buyScore = 0, sellScore = 0;

        // 1. Long-term trend: price vs 20-day moving average
        boolean aboveSma20 = sma20 > 0 && currentPrice > sma20;
        boolean belowSma20 = sma20 > 0 && currentPrice < sma20;
        if (aboveSma20) buyScore++;
        if (belowSma20) sellScore++;

        // 2. RSI momentum
        //    Buy:  45–72 = healthy upward momentum, not yet overbought
        //    Sell: < 45  = momentum fading  OR  > 72 = overbought, due for pullback
        boolean rsiBullish, rsiBearish;
        if ("Bullish".equals(emaCrossoverStatus) || "Bullish Cross".equals(emaCrossoverStatus)) {
            rsiBullish = rsi14 >= 50 && rsi14 <= 80;
            rsiBearish = rsi14 < 50  || rsi14 > 85;
        } else if ("Bearish".equals(emaCrossoverStatus) || "Bearish Cross".equals(emaCrossoverStatus)) {
            rsiBullish = rsi14 >= 40 && rsi14 <= 60;
            rsiBearish = rsi14 < 40  || rsi14 > 60;
        } else {
            rsiBullish = rsi14 >= 45 && rsi14 <= 72;
            rsiBearish = rsi14 < 45  || rsi14 > 72;
        }
        if (rsiBullish) buyScore++;
        if (rsiBearish) sellScore++;

        // 3. MACD (1-hour EMA12 vs EMA26): direction of short-term price pressure
        boolean macdBullish = macdH1 > 0;
        boolean macdBearish = macdH1 < 0;
        if (macdBullish) buyScore++;
        if (macdBearish) sellScore++;

        // 4. Price vs today's average trade price (VWAP)
        boolean aboveVwap = vwap > 0 && currentPrice > vwap * 1.001;
        boolean belowVwap = vwap > 0 && currentPrice < vwap * 0.999;
        if (aboveVwap) buyScore++;
        if (belowVwap) sellScore++;

        // 5. Hourly trend direction
        boolean hourlyRising  = h1Score > 15;
        boolean hourlyFalling = h1Score < -15;
        if (hourlyRising)  buyScore++;
        if (hourlyFalling) sellScore++;

        // 6. Volume confirms the directional move (high volume + price direction = conviction)
        boolean highVolume        = avgVolume30d > 0 && totalVolume > avgVolume30d * 1.2;
        boolean volConfirmsBuy    = highVolume && currentPrice >= priorClose;
        boolean volConfirmsSell   = highVolume && currentPrice <  priorClose;
        if (volConfirmsBuy)  buyScore++;
        if (volConfirmsSell) sellScore++;

        int netSignal = buyScore - sellScore;
        String buyStrength;
        if      (netSignal >= 4)  buyStrength = "STRONG_BUY";
        else if (netSignal >= 2)  buyStrength = "BUY";
        else if (netSignal >= -1) buyStrength = "WATCH";
        else if (netSignal >= -3) buyStrength = "SELL";
        else                      buyStrength = "STRONG_SELL";

        // Pre-computed human-readable signal sentences — model outputs these verbatim, no translation needed
        StringBuilder buySignalsStr  = new StringBuilder();
        StringBuilder sellSignalsStr = new StringBuilder();
        if (aboveSma20)      buySignalsStr.append("price is above the 20-day average, ");
        if (belowSma20)      sellSignalsStr.append("price is below the 20-day average, ");
        if (rsiBullish)      buySignalsStr.append("RSI momentum is healthy, ");
        if (rsiBearish)      sellSignalsStr.append("RSI is weak or overbought, ");
        if (macdBullish)     buySignalsStr.append("MACD is rising, ");
        if (macdBearish)     sellSignalsStr.append("MACD is falling, ");
        if (aboveVwap)       buySignalsStr.append("price is above today's average (VWAP), ");
        if (belowVwap)       sellSignalsStr.append("price is below today's average (VWAP), ");
        if (hourlyRising)    buySignalsStr.append("hourly trend is up, ");
        if (hourlyFalling)   sellSignalsStr.append("hourly trend is down, ");
        if (volConfirmsBuy)  buySignalsStr.append("volume confirms the move");
        if (volConfirmsSell) sellSignalsStr.append("volume confirms the move");
        String activeBuySignals  = buySignalsStr.length()  > 0
                ? buySignalsStr.toString().replaceAll(",\\s*$", "")
                : "No buy signals active";
        String activeSellSignals = sellSignalsStr.length() > 0
                ? sellSignalsStr.toString().replaceAll(",\\s*$", "")
                : "No sell signals active";

        LocalDate expDate;
        if (customDays == 0) {
            expDate = today.with(TemporalAdjusters.next(DayOfWeek.FRIDAY));
        } else {
            int calendarDaysToAdd = (int)(customDays * 1.45);
            expDate = today.plusDays(calendarDaysToAdd).with(TemporalAdjusters.nextOrSame(DayOfWeek.FRIDAY));
            if (!expDate.isAfter(today)) {
                expDate = today.with(TemporalAdjusters.next(DayOfWeek.FRIDAY));
            }
        }
        String targetExpiration = expDate.format(DateTimeFormatter.ofPattern("MMMM dd, yyyy"));

        // Position sizing — based on $200 max risk per trade
        int suggestedShares    = stopDistance > 0 ? (int) Math.floor(200.0 / stopDistance) : 0;
        int suggestedContracts = (spreadWidth * 100.0) > 0 ? Math.max(1, (int) Math.floor(200.0 / (spreadWidth * 100.0))) : 1;
        String rrDisplay = String.format("%.1f", rrRatio);

        // Trend-override gate — prevents strategy from contradicting dominant D1 trend:
        // - Short-term bearish confluence score but D1 uptrend with TF agreement → force neutral
        // - Short-term bullish confluence score but D1 downtrend with TF agreement → force neutral
        // - Within 10 days of earnings → always neutral (binary event risk)
        boolean forceNeutral = (earningsFlag && earningsDaysAway <= 10)
            || (totalConfluenceScore <= -15.0 && d1TrendUp   && tfAgreement >= 2)
            || (totalConfluenceScore >= 15.0  && d1TrendDown && tfAgreement <= -2);

        // Strong-momentum gate: single-leg only when confluence is high AND IV is low (cheap options worth owning outright)
        boolean strongBull = totalConfluenceScore >= 30.0 && compositeTrendScore >= 60 && !"HIGH".equals(ivRegime);
        boolean strongBear = totalConfluenceScore <= -30.0 && compositeTrendScore <= 40 && !"HIGH".equals(ivRegime);

        // ── Strategy ladder — A (1-leg), B (2-leg debit), C (2-leg credit) ────────
        String strategyA = "", optionsLineA = "";
        String strategyB = "", optionsLineB = "";
        String strategyC = "", optionsLineC = "";
        String recommendedStrategy = "B"; // default
        String strategyName;
        String optionsLine;

        if (forceNeutral || (totalConfluenceScore > -15.0 && totalConfluenceScore < 15.0)) {
            // Neutral — single Iron Condor, no ladder
            strategyName = "Iron Condor";
            optionsLine  = "Sell $" + String.format("%.2f", icPutSell)
                         + " Put · Buy $" + String.format("%.2f", icPutBuy)
                         + " Put · Sell $" + String.format("%.2f", icCallSell)
                         + " Call · Buy $" + String.format("%.2f", icCallBuy)
                         + " Call · Expires " + targetExpiration;
            strategyA = strategyB = strategyC = "Iron Condor";
            optionsLineA = optionsLineB = optionsLineC = optionsLine;

        } else if (totalConfluenceScore >= 15.0) {
            // ── BULLISH ladder ──
            // A: Long Call — high conviction + LOW IV
            strategyA    = "Long Call";
            optionsLineA = "Buy 1× $" + String.format("%.2f", strikeBuy)
                         + " Call · Expires " + targetExpiration
                         + " · Premium paid = max loss, unlimited upside";

            // B: Bull Call Debit Spread — defined risk directional
            strategyB    = "Bull Call Spread";
            optionsLineB = "Buy 1× $" + String.format("%.2f", strikeBuy)
                         + " Call · Sell 1× $" + String.format("%.2f", spreadShortStrike)
                         + " Call · Expires " + targetExpiration;

            // C: Bull Put Credit Spread — HIGH IV, collect premium, profit if stock stays above support
            strategyC    = "Bull Put Credit Spread";
            optionsLineC = "Sell 1× $" + String.format("%.2f", creditPutSell)
                         + " Put · Buy 1× $" + String.format("%.2f", creditPutBuy)
                         + " Put · Expires " + targetExpiration
                         + " · Collect premium, profit if price stays above $" + String.format("%.2f", creditPutSell);

            if (strongBull)               { recommendedStrategy = "A"; }
            else if ("HIGH".equals(ivRegime)) { recommendedStrategy = "C"; }
            else                           { recommendedStrategy = "B"; }

            // Primary (backward compat)
            strategyName = "A".equals(recommendedStrategy) ? strategyA
                         : "C".equals(recommendedStrategy) ? strategyC : strategyB;
            optionsLine  = "A".equals(recommendedStrategy) ? optionsLineA
                         : "C".equals(recommendedStrategy) ? optionsLineC : optionsLineB;

        } else {
            // ── BEARISH ladder ──
            // A: Long Put — high conviction + LOW IV
            strategyA    = "Long Put";
            optionsLineA = "Buy 1× $" + String.format("%.2f", strikeBuy)
                         + " Put · Expires " + targetExpiration
                         + " · Premium paid = max loss, unlimited downside profit";

            // B: Bear Put Debit Spread — defined risk directional
            strategyB    = "Bear Put Spread";
            optionsLineB = "Buy 1× $" + String.format("%.2f", strikeBuy)
                         + " Put · Sell 1× $" + String.format("%.2f", spreadShortStrike)
                         + " Put · Expires " + targetExpiration;

            // C: Bear Call Credit Spread — HIGH IV, collect premium, profit if stock stays below resistance
            strategyC    = "Bear Call Credit Spread";
            optionsLineC = "Sell 1× $" + String.format("%.2f", creditCallSell)
                         + " Call · Buy 1× $" + String.format("%.2f", creditCallBuy)
                         + " Call · Expires " + targetExpiration
                         + " · Collect premium, profit if price stays below $" + String.format("%.2f", creditCallSell);

            if (strongBear)               { recommendedStrategy = "A"; }
            else if ("HIGH".equals(ivRegime)) { recommendedStrategy = "C"; }
            else                           { recommendedStrategy = "B"; }

            // Primary (backward compat)
            strategyName = "A".equals(recommendedStrategy) ? strategyA
                         : "C".equals(recommendedStrategy) ? strategyC : strategyB;
            optionsLine  = "A".equals(recommendedStrategy) ? optionsLineA
                         : "C".equals(recommendedStrategy) ? optionsLineC : optionsLineB;
        }

        // Confidence score: percentage of signals agreeing with the final verdict direction
        {
            boolean bullVerdict = totalConfluenceScore >= 15.0 && !forceNeutral;
            boolean bearVerdict = totalConfluenceScore <= -15.0 && !forceNeutral;
            int aligned = 0, total = 8;
            if (bullVerdict) {
                if (aboveSma20)     aligned++;
                if (rsiBullish)     aligned++;
                if (macdBullish)    aligned++;
                if (aboveVwap)      aligned++;
                if (hourlyRising)   aligned++;
                if (volConfirmsBuy) aligned++;
                if (d1TrendUp)      aligned++;
                if (tfAgreement >= 2) aligned++;
            } else if (bearVerdict) {
                if (belowSma20)      aligned++;
                if (rsiBearish)      aligned++;
                if (macdBearish)     aligned++;
                if (belowVwap)       aligned++;
                if (hourlyFalling)   aligned++;
                if (volConfirmsSell) aligned++;
                if (d1TrendDown)     aligned++;
                if (tfAgreement <= -2) aligned++;
            } else {
                aligned = 4; // neutral = middle confidence
            }
            confidenceScore = (int)((double) aligned / total * 100);
            confidenceLabel = confidenceScore >= 75 ? "High" : confidenceScore >= 50 ? "Moderate" : "Low";
        }

        // Win probability: estimated % chance the trade reaches its target
        // Built from 10 weighted signals (max raw = 40pts → capped to 35–82% realistic range)
        int winProbability = 50;
        String winProbabilityLabel = "Moderate";
        {
            boolean bullTrade = totalConfluenceScore >= 15.0 && !forceNeutral;
            boolean bearTrade = totalConfluenceScore <= -15.0 && !forceNeutral;
            if (forceNeutral || (!bullTrade && !bearTrade)) {
                // Neutral / premium-selling — IV rank gives inherent edge
                winProbability = 50 + (int)(ivRank * 0.18);
                winProbability = Math.max(50, Math.min(70, winProbability));
            } else {
                int pts = 0;
                if (bullTrade) {
                    if (aboveSma20)                                    pts += 4;
                    if (rsiBullish)                                    pts += 4;
                    if (macdBullish)                                   pts += 4;
                    if (aboveVwap)                                     pts += 3;
                    if (hourlyRising)                                  pts += 3;
                    if (volConfirmsBuy)                                pts += 3;
                    if (d1TrendUp)                                     pts += 3;
                    if (tfAgreement >= 2)                              pts += 3;
                    if ("BULLISH".equals(weeklyBias))                  pts += 5;
                    if (maStackScore >= 3)                             pts += 3;
                    if ("RISING".equals(adxDirection) && adxValue>=25) pts += 2;
                    if ("STRONG".equals(marketBreadth))                pts += 2;
                    if ("NARROW".equals(marketBreadth))                pts -= 3;
                } else {
                    if (belowSma20)                                    pts += 4;
                    if (rsiBearish)                                    pts += 4;
                    if (macdBearish)                                   pts += 4;
                    if (belowVwap)                                     pts += 3;
                    if (hourlyFalling)                                 pts += 3;
                    if (volConfirmsSell)                               pts += 3;
                    if (d1TrendDown)                                   pts += 3;
                    if (tfAgreement <= -2)                             pts += 3;
                    if ("BEARISH".equals(weeklyBias))                  pts += 5;
                    if (maStackScore <= 1)                             pts += 3;
                    if ("RISING".equals(adxDirection) && adxValue>=25) pts += 2;
                    if ("NARROW".equals(marketBreadth))                pts += 2;
                    if ("STRONG".equals(marketBreadth))                pts -= 3;
                }
                winProbability = 42 + pts;
                if (earningsFlag && earningsDaysAway <= 3) winProbability -= 10;
                if ("HIGH".equals(ivRegime)) winProbability += 3;
                winProbability = Math.max(35, Math.min(82, winProbability));
            }
            winProbabilityLabel = winProbability >= 72 ? "High Conviction"
                                : winProbability >= 62 ? "Moderate-High"
                                : winProbability >= 52 ? "Moderate"
                                : winProbability >= 42 ? "Low-Moderate"
                                :                        "Low — size down";
        }

        return String.format(",\"session_status\":\"%s\",\"macro_daily_trend_score\":%.1f,\"h1_radar_score\":%.1f,\"m15_radar_score\":%.1f,\"m5_radar_score\":%.1f,\"total_confluence_score\":%.1f,\"intraday_vwap\":%.2f,\"micro_support\":%.2f,\"micro_resistance\":%.2f,\"implied_volatility\":\"%.2f%%\",\"tomorrow_upper\":%.2f,\"tomorrow_lower\":%.2f,\"next_week_upper\":%.2f,\"next_week_lower\":%.2f,\"custom_upper\":%.2f,\"custom_lower\":%.2f,\"custom_days\":%d,\"automated_trade_verdict\":\"%s\",\"final_entry\":%.2f,\"final_tp\":%.2f,\"final_sl\":%.2f,\"strike_buy\":%.2f,\"spread_short_strike\":%.2f,\"strike_sell\":%.2f,\"target_expiration\":\"%s\",\"strategy_name\":\"%s\",\"options_line\":\"%s\",\"ema_crossover_status\":\"%s\",\"calculated_rsi_14d\":%.1f,\"calculated_support\":%.2f,\"calculated_resistance\":%.2f"
                + ",\"buy_strength\":\"%s\",\"buy_score\":%d,\"sell_score\":%d,\"rsi_14d\":%.1f"
                + ",\"active_buy_signals\":\"%s\",\"active_sell_signals\":\"%s\""
                + ",\"smart_money_score\":%.1f,\"smart_money_verdict\":\"%s\",\"smart_money_conflict\":%b"
                + ",\"insider_mspr\":%.4f,\"insider_buys\":%d,\"insider_sells\":%d"
                + ",\"analyst_buy\":%d,\"analyst_hold\":%d,\"analyst_sell\":%d"
                + ",\"market_regime\":\"%s\",\"regime_note\":\"%s\",\"vix_level\":%.1f,\"spy_trend\":\"%s\""
                + ",\"adx_value\":%.1f,\"adx_trend\":\"%s\",\"adx_direction\":\"%s\""
                + ",\"rsi_divergence\":\"%s\""
                + ",\"iv_rank\":%.0f,\"iv_regime\":\"%s\""
                + ",\"tf_agreement\":%d"
                + ",\"news_sentiment\":\"%s\""
                + ",\"prior_day_high\":%.2f,\"prior_day_low\":%.2f"
                + ",\"vwap_upper_1sd\":%.2f,\"vwap_lower_1sd\":%.2f"
                + ",\"earnings_flag\":%b,\"earnings_date\":\"%s\",\"earnings_days_away\":%d"
                + ",\"suggested_shares\":%d,\"suggested_contracts\":%d,\"rr_ratio\":\"%s\""
                + ",\"swing_support\":%.2f,\"swing_resistance\":%.2f"
                + ",\"swing_support_strength\":%d,\"swing_resistance_strength\":%d"
                + ",\"swing_trade_signal\":\"%s\",\"swing_entry\":%.2f,\"swing_target\":%.2f,\"swing_stop\":%.2f"
                + ",\"swing_strategy\":\"%s\",\"swing_note\":\"%s\""
                + ",\"year_high\":%.2f,\"year_low\":%.2f,\"week_range_position\":%.1f"
                + ",\"beta\":%.2f"
                + ",\"unusual_options_activity\":\"%s\""
                + ",\"earnings_iv_inflated\":%b"
                + ",\"confidence_score\":%d,\"confidence_label\":\"%s\""
                + ",\"weekly_bias\":\"%s\",\"weekly_ema20\":%.2f,\"weekly_ema50\":%.2f"
                + ",\"ma_stack_score\":%d,\"ma_stack_label\":\"%s\""
                + ",\"market_breadth\":\"%s\",\"breadth_rs\":%.2f"
                + ",\"composite_trend_score\":%d,\"composite_trend_label\":\"%s\""
                + ",\"win_probability\":%d,\"win_probability_label\":\"%s\""
                + ",\"ema8\":%.2f,\"ema21\":%.2f,\"ema50\":%.2f,\"ema200\":%.2f"
                + ",\"strategy_a\":\"%s\",\"options_line_a\":\"%s\""
                + ",\"strategy_b\":\"%s\",\"options_line_b\":\"%s\""
                + ",\"strategy_c\":\"%s\",\"options_line_c\":\"%s\""
                + ",\"recommended_strategy\":\"%s\"",
                sessionStatus, dailyScore, h1Score, m15Score, m5Score, totalConfluenceScore, vwap, microSupport, microResistance, impliedVolatility * 100, tomorrowUpper, tomorrowLower, nextWeekUpper, nextWeekLower, customUpper, customLower, effectiveCustomDays, dynamicVerdict, dynamicEntry, dynamicTp, dynamicSl, strikeBuy, spreadShortStrike, strikeSell, targetExpiration, strategyName, optionsLine, emaCrossoverStatus, rsi14, calculatedSupport, calculatedResistance,
                buyStrength, buyScore, sellScore, rsi14,
                activeBuySignals, activeSellSignals,
                smartMoneyScore, smartMoneyVerdict, smConflict,
                insiderMspr, insiderBuys, insiderSells,
                analystBuy, analystHold, analystSell,
                marketRegime, regimeNote, vixLevel, spyTrend,
                adxValue, adxTrend, adxDirection,
                rsiDivergence,
                ivRank, ivRegime,
                tfAgreement,
                finnhubNewsSentLabel,
                priorDayHigh, priorDayLow,
                vwapUpper, vwapLower,
                earningsFlag, earningsDate, earningsDaysAway,
                suggestedShares, suggestedContracts, rrDisplay,
                swingSupport, swingResistance,
                swingSupportStrength, swingResistanceStrength,
                swingTradeSignal, swingEntry, swingTarget, swingStop,
                swingStrategy, swingNote,
                yearHigh, yearLow <= Double.MAX_VALUE / 2 ? yearLow : 0.0, weekRangePosition,
                beta,
                unusualOptionsActivity,
                earningsIvInflated,
                confidenceScore, confidenceLabel,
                weeklyBias, weeklyEma20, weeklyEma50,
                maStackScore, maStackLabel,
                marketBreadth, breadthRs,
                compositeTrendScore, compositeTrendLabel,
                winProbability, winProbabilityLabel,
                ema8d, ema21d, ema50d, ema200d,
                strategyA, optionsLineA.replace("\"", "'"),
                strategyB, optionsLineB.replace("\"", "'"),
                strategyC, optionsLineC.replace("\"", "'"),
                recommendedStrategy);
    }

    // ── Single-ticker scan (Yahoo snapshot + full MTF) — cached 30 s ─────────

    /**
     * Fetches Yahoo + full MTF analysis for one ticker — shared by both stockPriceFunction and the scanner.
     * Results are cached for {@link #SCAN_CACHE_TTL_SECONDS} seconds.
     */
    public String scanTicker(String ticker) {
        CachedScan cached = scanCache.get(ticker);
        if (cached != null && Instant.now().minusSeconds(SCAN_CACHE_TTL_SECONDS).isBefore(cached.cachedAt())) {
            return cached.json();
        }
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/" + ticker + "?includePrePost=true&interval=1m&range=1d"))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .GET()
                    .build();
            HttpResponse<String> res = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return null;

            JsonNode root       = objectMapper.readTree(res.body());
            JsonNode resultNode = root.path("chart").path("result").get(0);
            if (resultNode == null || resultNode.isMissingNode()) return null;
            JsonNode meta = resultNode.path("meta");

            double regularPrice = meta.path("regularMarketPrice").asDouble();
            double priorClose   = meta.path("chartPreviousClose").asDouble();
            long   vol          = meta.path("regularMarketVolume").asLong(0);
            double highToday    = meta.path("regularMarketDayHigh").asDouble(regularPrice);
            double lowToday     = meta.path("regularMarketDayLow").asDouble(regularPrice);

            double currentPrice = regularPrice;
            JsonNode closes = resultNode.path("indicators").path("quote").get(0).path("close");
            if (closes != null && closes.isArray() && !closes.isEmpty()) {
                for (int i = closes.size() - 1; i >= 0; i--) {
                    if (!closes.get(i).isNull()) { currentPrice = closes.get(i).asDouble(); break; }
                }
            }

            currentPrice = alpacaClient.alpacaStreamService.getLatestQuote(ticker)
                    .map(AlpacaStreamService.LiveQuote::price)
                    .filter(p -> p > 0)
                    .orElse(currentPrice);

            if (currentPrice <= 0) return null;

            double percentChange = (priorClose > 0) ? ((currentPrice - priorClose) / priorClose) * 100.0 : 0.0;
            String pctString = String.format("%s%.2f%%", (percentChange >= 0 ? "+" : ""), percentChange);

            String mtf  = processIntradayMtfAlignment(ticker, currentPrice, highToday, lowToday, vol, priorClose, 0);
            String base = String.format("{\"symbol\":\"%s\",\"current_price\":%.2f,\"percent_change\":\"%s\",\"volume\":\"%s\"}",
                    ticker, currentPrice, pctString, String.format("%,d", vol));
            // mtf starts with a comma — strip the closing brace from base and append
            String result = base.substring(0, base.length() - 1) + mtf + "}";
            scanCache.put(ticker, new CachedScan(result, Instant.now()));
            return result;
        } catch (Exception e) {
            return null;
        }
    }

    // ── Pre-market scanner helpers ────────────────────────────────────────────

    /** Fetches pre-market bars for a ticker and returns a JSON result, or null if no significant activity. */
    public String scanPreMarketTicker(String ticker) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/" + ticker + "?includePrePost=true&interval=5m&range=1d"))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .timeout(Duration.ofSeconds(6))
                    .GET().build();
            HttpResponse<String> res = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return null;

            JsonNode root = objectMapper.readTree(res.body());
            JsonNode resultNode = root.path("chart").path("result").get(0);
            if (resultNode == null || resultNode.isMissingNode()) return null;

            JsonNode meta = resultNode.path("meta");
            double priorClose = meta.path("chartPreviousClose").asDouble();
            if (priorClose <= 0) return null;

            // Regular session start timestamp (Unix seconds) for today — bars before this are pre-market
            long marketOpenTs = meta.path("currentTradingPeriod").path("regular").path("start").asLong(Long.MAX_VALUE);

            JsonNode timestamps = resultNode.path("timestamp");
            JsonNode quoteNode  = resultNode.path("indicators").path("quote").get(0);
            JsonNode closesArr  = quoteNode.path("close");
            JsonNode opensArr   = quoteNode.path("open");
            JsonNode volumesArr = quoteNode.path("volume");

            if (!timestamps.isArray() || timestamps.size() == 0) return null;

            List<double[]> preBars = new ArrayList<>(); // [open, close]
            long totalPreVolume = 0;
            double pmHigh = 0, pmLow = Double.MAX_VALUE;

            for (int i = 0; i < timestamps.size(); i++) {
                long ts = timestamps.get(i).asLong();
                if (ts >= marketOpenTs) break;
                if (i >= closesArr.size() || i >= opensArr.size()) break;
                double c = closesArr.get(i).asDouble(0);
                double o = opensArr.get(i).asDouble(0);
                long v  = (i < volumesArr.size()) ? volumesArr.get(i).asLong(0) : 0;
                if (c > 0 && o > 0) {
                    preBars.add(new double[]{o, c});
                    totalPreVolume += v;
                    if (c > pmHigh) pmHigh = c;
                    if (c < pmLow)  pmLow  = c;
                }
            }

            if (preBars.isEmpty()) return null;

            double preMarketPrice  = preBars.get(preBars.size() - 1)[1];
            double pmChangePercent = ((preMarketPrice - priorClose) / priorClose) * 100.0;

            if (Math.abs(pmChangePercent) < 0.5 && totalPreVolume < 5_000) return null;
            if (preMarketPrice < 5.0) return null;

            String pattern = detectPreMarketPattern(preBars, pmChangePercent, priorClose);

            if (pmHigh <= 0)             pmHigh = preMarketPrice * 1.005;
            if (pmLow == Double.MAX_VALUE) pmLow = preMarketPrice * 0.995;

            String mtf      = processIntradayMtfAlignment(ticker, preMarketPrice, pmHigh, pmLow, totalPreVolume, priorClose, 0);
            String pctString = String.format("%s%.2f%%", pmChangePercent >= 0 ? "+" : "", pmChangePercent);
            String base      = String.format("{\"symbol\":\"%s\",\"current_price\":%.2f,\"percent_change\":\"%s\",\"pre_market_volume\":\"%s\",\"pattern\":\"%s\"}",
                    ticker, preMarketPrice, pctString, String.format("%,d", totalPreVolume), pattern);
            return base.substring(0, base.length() - 1) + mtf + "}";
        } catch (Exception e) {
            return null;
        }
    }

    /** Classifies the pre-market price action pattern from a list of [open, close] bar pairs. */
    String detectPreMarketPattern(List<double[]> bars, double pmChangePercent, double priorClose) {
        int size = bars.size();
        if (size < 2) return pmChangePercent > 0 ? "Gapping Up" : "Gapping Down";

        int lookback = Math.min(3, size);
        int up = 0, down = 0;
        double rangeHigh = 0, rangeLow = Double.MAX_VALUE;
        for (int i = size - lookback; i < size; i++) {
            double o = bars.get(i)[0], c = bars.get(i)[1];
            if (c > o) up++;
            else if (c < o) down++;
            rangeHigh = Math.max(rangeHigh, Math.max(o, c));
            rangeLow  = Math.min(rangeLow,  Math.min(o, c));
        }
        double rangeWidth = rangeHigh > 0 ? (rangeHigh - rangeLow) / rangeHigh : 1.0;

        if (rangeWidth < 0.003 && size >= 3) return "Consolidating at Gap";

        if (pmChangePercent > 0.5) {
            if (up == lookback) return "Gap & Go (Bullish)";
            if (down >= 2)      return "Gap & Fade (Selling Pressure)";
            return "Gap Up (Mixed)";
        } else if (pmChangePercent < -0.5) {
            if (down == lookback) return "Gap & Go (Bearish)";
            if (up >= 2)          return "Gap & Fade (Buying Interest)";
            return "Gap Down (Mixed)";
        }
        return "Flat Drift";
    }

    // ── Full analysis methods (backing methods for @Bean lambdas) ─────────────

    /**
     * Full stock analysis: fetches Yahoo snapshot, runs MTF alignment, returns JSON.
     * Backing method for the stockPriceFunction @Bean.
     */
    public String analyzeStock(String ticker, int customDays) {
        // Ensure this ticker is being tracked by the WebSocket stream
        alpacaClient.alpacaStreamService.subscribe(ticker);
        String cacheKey = ticker + ":" + customDays;
        CachedScan cached = analysisCache.get(cacheKey);
        if (cached != null && Instant.now().minusSeconds(ANALYSIS_CACHE_TTL_SECONDS).isBefore(cached.cachedAt())) {
            return cached.json();
        }
        try {
            HttpRequest liveRequest = HttpRequest.newBuilder()
                    .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/" + ticker + "?includePrePost=true&interval=1m&range=1d"))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .GET()
                    .build();

            HttpResponse<String> res = alpacaClient.httpClient.send(liveRequest, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return String.format("{\"error\":\"CRITICAL FAILURE: Public Gateway Offline for %s.\"}", ticker);

            JsonNode root = objectMapper.readTree(res.body());
            JsonNode resultNode = root.path("chart").path("result").get(0);
            JsonNode meta = resultNode.path("meta");

            double regularPrice = meta.path("regularMarketPrice").asDouble();
            double priorClose = meta.path("chartPreviousClose").asDouble();
            long vol = meta.path("regularMarketVolume").asLong(0);
            double highToday = meta.path("regularMarketDayHigh").asDouble(regularPrice);
            double lowToday = meta.path("regularMarketDayLow").asDouble(regularPrice);

            // Seed from Yahoo meta; then refine from the 1-minute bar array (includes pre/post hours)
            double currentPrice = regularPrice;

            JsonNode closes = resultNode.path("indicators").path("quote").get(0).path("close");
            if (closes != null && closes.isArray() && !closes.isEmpty()) {
                for (int i = closes.size() - 1; i >= 0; i--) {
                    if (!closes.get(i).isNull()) {
                        currentPrice = closes.get(i).asDouble();
                        break;
                    }
                }
            }

            // If the WebSocket stream has a fresher quote (< 30 s old), use it as the price
            currentPrice = alpacaClient.alpacaStreamService.getLatestQuote(ticker)
                    .map(AlpacaStreamService.LiveQuote::price)
                    .filter(p -> p > 0)
                    .orElse(currentPrice);

            double percentChange = (priorClose > 0) ? ((currentPrice - priorClose) / priorClose) * 100.0 : 0.0;
            String pctString = String.format("%s%.2f%%", (percentChange >= 0 ? "+" : ""), percentChange);

            String payload = String.format("{\"symbol\":\"%s\",\"company_name\":\"%s\",\"current_price\":%.2f,\"change\":%.2f,\"percent_change\":\"%s\",\"volume\":\"%s\",\"high_today\":%.2f,\"low_today\":%.2f}",
                    ticker, ticker, currentPrice, currentPrice - priorClose, pctString, String.format("%,d", vol), highToday, lowToday);

            String result = payload.substring(0, payload.length() - 1) + processIntradayMtfAlignment(ticker, currentPrice, highToday, lowToday, vol, priorClose, customDays) + "}";
            analysisCache.put(cacheKey, new CachedScan(result, Instant.now()));
            return result;
        } catch (Exception e) {
            return String.format("{\"error\":\"CRITICAL FAILURE: Exception parsing data streams for %s.\"}", ticker);
        }
    }

    /**
     * Historical trend: fetches 60 days of daily bars, computes RSI + EMA crossover.
     * Backing method for the historicalTrendFunction @Bean.
     */
    public String analyzeHistoricalTrend(String ticker) {
        try {
            String startLookback = ZonedDateTime.now(ZoneId.of("America/New_York")).minusDays(60).format(DateTimeFormatter.ISO_INSTANT);
            HttpResponse<String> res = alpacaClient.httpClient.send(alpacaClient.buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Day&start=" + startLookback + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
            JsonNode tickerNode = objectMapper.readTree(res.body()).path("bars").path(ticker);

            if (tickerNode != null && tickerNode.isArray() && tickerNode.size() >= 25) {
                int len = tickerNode.size();
                double[] closes = new double[len];
                for (int i = 0; i < len; i++) {
                    closes[i] = tickerNode.get(i).path("c").asDouble();
                }

                double lastPrice = closes[len - 1];

                double gains = 0, losses = 0;
                for (int i = len - 14; i < len; i++) {
                    double diff = closes[i] - closes[i - 1];
                    if (diff > 0) gains += diff;
                    else losses -= diff;
                }
                double avgGain = gains / 14.0;
                double avgLoss = losses / 14.0;
                double rsi = 50.0;
                if (avgLoss != 0) {
                    double rs = avgGain / avgLoss;
                    rsi = 100.0 - (100.0 / (1.0 + rs));
                } else {
                    rsi = (avgGain > 0) ? 100.0 : 0.0;
                }

                double sma9 = 0, sma21 = 0;
                for(int i = len - 9; i < len; i++) sma9 += closes[i];
                sma9 /= 9.0;
                for(int i = len - 21; i < len; i++) sma21 += closes[i];
                sma21 /= 21.0;

                double prevSma9 = 0, prevSma21 = 0;
                for(int i = len - 10; i < len - 1; i++) prevSma9 += closes[i];
                prevSma9 /= 9.0;
                for(int i = len - 22; i < len - 1; i++) prevSma21 += closes[i];
                prevSma21 /= 21.0;

                String emaStatus;
                if (sma9 > sma21 && prevSma9 <= prevSma21) emaStatus = "Bullish Cross";
                else if (sma9 < sma21 && prevSma9 >= prevSma21) emaStatus = "Bearish Cross";
                else if (sma9 > sma21) emaStatus = "Bullish";
                else emaStatus = "Bearish";

                double atrHist = 0.0;
                if (len >= 15) {
                    double atrSum = 0;
                    for (int i = len - 14; i < len; i++) {
                        double h = tickerNode.get(i).path("h").asDouble();
                        double l = tickerNode.get(i).path("l").asDouble();
                        double prevC = closes[i - 1];
                        atrSum += Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
                    }
                    atrHist = atrSum / 14.0;
                }
                double dynSupport = atrHist > 0 ? lastPrice - (1.5 * atrHist) : lastPrice * 0.96;
                double dynResistance = atrHist > 0 ? lastPrice + (1.5 * atrHist) : lastPrice * 1.04;
                return String.format("{\"symbol\":\"%s\",\"calculated_support\":%.2f,\"calculated_resistance\":%.2f,\"ema_crossover_status\":\"%s\",\"calculated_rsi_14d\":%.1f}",
                        ticker, dynSupport, dynResistance, emaStatus, rsi);
            }
            return String.format("{\"symbol\":\"%s\",\"calculated_support\":100.0,\"calculated_resistance\":110.0,\"ema_crossover_status\":\"Neutral\",\"calculated_rsi_14d\":50.0}", ticker);
        } catch (Exception e) {
            return String.format("{\"error\":\"CRITICAL FAILURE: Exception building trend metrics for %s.\"}", ticker);
        }
    }
}
