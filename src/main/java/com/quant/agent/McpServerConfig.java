package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;

import java.net.URI;
import java.net.http.HttpClient;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Configuration
public class McpServerConfig {

    private static final Logger log = LoggerFactory.getLogger(McpServerConfig.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private record CachedScan(String json, Instant cachedAt) {}
    private final ConcurrentHashMap<String, CachedScan> scanCache = new ConcurrentHashMap<>();
    private static final long SCAN_CACHE_TTL_SECONDS = 30;

    @Value("${alpaca.api.key}")
    private String apiKey;

    @Value("${alpaca.api.secret}")
    private String apiSecret;

    @Value("${market.provider.api-key}")
    private String finnhubKey;

    private final AlpacaStreamService alpacaStreamService;
    private final MarketClockService marketClockService;

    public McpServerConfig(AlpacaStreamService alpacaStreamService, MarketClockService marketClockService) {
        this.alpacaStreamService = alpacaStreamService;
        this.marketClockService = marketClockService;
    }

    // UPGRADE: Split schemas so the AI doesn't drop the custom parameters
    public record PriceRequest(String symbol, Integer customTradingDays) {}
    public record TrendRequest(String symbol) {}
    public record EmptyRequest() {}

    private HttpRequest buildAlpacaBaseRequest(String fullPath) {
        return HttpRequest.newBuilder()
                .uri(URI.create("https://data.alpaca.markets" + fullPath))
                .header("APCA-API-KEY-ID", apiKey != null ? apiKey : "")
                .header("APCA-API-SECRET-KEY", apiSecret != null ? apiSecret : "")
                .header("accept", "application/json")
                .GET()
                .build();
    }

    private HttpRequest buildAlpacaRequest(String endpoint) {
        return buildAlpacaBaseRequest("/v2/stocks" + endpoint);
    }

    private double getNearestOptionStrike(double price) {
        if (price <= 50.0) {
            return Math.round(price * 2.0) / 2.0; 
        } else if (price <= 200.0) {
            return Math.round(price); 
        } else if (price <= 500.0) {
            return Math.round(price / 2.5) * 2.5; 
        } else {
            return Math.round(price / 5.0) * 5.0; 
        }
    }

    private double normalizeScore(double value, double scaleFactor) {
        return Math.max(-100.0, Math.min(100.0, value * scaleFactor));
    }

    private double calculateSmaFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period) return 0.0;
        double sum = 0;
        for (int i = len - period; i < len; i++) sum += bars.get(i).path("c").asDouble();
        return sum / period;
    }

    private double calculateEmaFromBars(JsonNode bars, int period) {
        if (bars.size() < period) return bars.get(bars.size() - 1).path("c").asDouble();
        double k = 2.0 / (period + 1.0);
        double ema = bars.get(0).path("c").asDouble();
        for (int i = 1; i < bars.size(); i++) ema = bars.get(i).path("c").asDouble() * k + ema * (1.0 - k);
        return ema;
    }

    private double calculateRsiFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period + 1) return 50.0;
        double gains = 0, losses = 0;
        for (int i = len - period; i < len; i++) {
            double diff = bars.get(i).path("c").asDouble() - bars.get(i - 1).path("c").asDouble();
            if (diff > 0) gains += diff; else losses -= diff;
        }
        double avgGain = gains / period, avgLoss = losses / period;
        if (avgLoss < 0.0001) return avgGain > 0 ? 95.0 : 50.0;
        double rs = avgGain / avgLoss;
        return 100.0 - (100.0 / (1.0 + rs));
    }

    private double calculateAtrFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period + 1) return 0.0;
        double sum = 0;
        for (int i = len - period; i < len; i++) {
            double h = bars.get(i).path("h").asDouble(), l = bars.get(i).path("l").asDouble();
            double prevC = bars.get(i - 1).path("c").asDouble();
            sum += Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
        }
        return sum / period;
    }

    private double calculateAvgVolumeFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len == 0) return 0.0;
        int count = Math.min(len, period);
        double sum = 0;
        for (int i = len - count; i < len; i++) sum += bars.get(i).path("v").asLong();
        return sum / count;
    }

    private double calculateAdxFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period * 2 + 1) return 25.0;
        double[] tr  = new double[len];
        double[] pdm = new double[len];
        double[] mdm = new double[len];
        for (int i = 1; i < len; i++) {
            double h  = bars.get(i).path("h").asDouble();
            double l  = bars.get(i).path("l").asDouble();
            double prevH = bars.get(i - 1).path("h").asDouble();
            double prevL = bars.get(i - 1).path("l").asDouble();
            double prevC = bars.get(i - 1).path("c").asDouble();
            tr[i]  = Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
            double up   = h - prevH, down = prevL - l;
            pdm[i] = (up > down && up > 0)     ? up   : 0;
            mdm[i] = (down > up && down > 0) ? down : 0;
        }
        double tr14 = 0, pdm14 = 0, mdm14 = 0;
        for (int i = 1; i <= period; i++) { tr14 += tr[i]; pdm14 += pdm[i]; mdm14 += mdm[i]; }
        double pdi = tr14 > 0 ? 100.0 * pdm14 / tr14 : 0;
        double mdi = tr14 > 0 ? 100.0 * mdm14 / tr14 : 0;
        double adx = (pdi + mdi) > 0 ? 100.0 * Math.abs(pdi - mdi) / (pdi + mdi) : 0;
        for (int i = period + 1; i < len; i++) {
            tr14  = tr14  - (tr14  / period) + tr[i];
            pdm14 = pdm14 - (pdm14 / period) + pdm[i];
            mdm14 = mdm14 - (mdm14 / period) + mdm[i];
            pdi = tr14 > 0 ? 100.0 * pdm14 / tr14 : 0;
            mdi = tr14 > 0 ? 100.0 * mdm14 / tr14 : 0;
            double dx = (pdi + mdi) > 0 ? 100.0 * Math.abs(pdi - mdi) / (pdi + mdi) : 0;
            adx = ((adx * (period - 1)) + dx) / period;
        }
        return adx;
    }

    private double extractConfluenceScore(String tickerJson) {
        try {
            return objectMapper.readTree(tickerJson).path("total_confluence_score").asDouble(0.0);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private String processIntradayMtfAlignment(String ticker, double currentPrice, double highToday, double lowToday, long totalVolume, double priorClose, int customDays) throws Exception {
        ZonedDateTime nowET = ZonedDateTime.now(ZoneId.of("America/New_York"));
        
        String lookback45Days = nowET.minusDays(90).format(DateTimeFormatter.ISO_INSTANT); // 90d → ~63 bars, enough for ADX(14) convergence
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
        double priorDayHigh = 0.0;
        double priorDayLow  = 0.0;
        double vwapUpper = 0.0;
        double vwapLower = 0.0;
        double spyScore = 0.0;
        String spyTrend = "Neutral";
        double vixLevel = 20.0;
        String marketRegime = "NEUTRAL";
        boolean earningsFlag = false;
        String earningsDate = "";
        int earningsDaysAway = 999;

        String insiderFrom = nowET.minusMonths(3).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String insiderTo   = nowET.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        CompletableFuture<HttpResponse<String>> futureD1 = httpClient.sendAsync(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Day&start=" + lookback45Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureH1 = httpClient.sendAsync(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Hour&start=" + lookback10Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureM15 = httpClient.sendAsync(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=15Min&start=" + lookback5Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureM5 = httpClient.sendAsync(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=5Min&start=" + lookback5Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureOptions = httpClient.sendAsync(buildAlpacaBaseRequest("/v1beta1/options/snapshots/" + ticker + "?feed=indicative&strike_price_gte=" + (currentPrice * 0.98) + "&strike_price_lte=" + (currentPrice * 1.02) + "&limit=50"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureNews = httpClient.sendAsync(
                HttpRequest.newBuilder()
                        .uri(URI.create("https://data.alpaca.markets/v1beta1/news?symbols=" + ticker + "&limit=20&sort=desc"))
                        .header("APCA-API-KEY-ID", apiKey != null ? apiKey : "")
                        .header("APCA-API-SECRET-KEY", apiSecret != null ? apiSecret : "")
                        .header("accept", "application/json")
                        .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureEarnings = httpClient.sendAsync(
                HttpRequest.newBuilder().uri(URI.create("https://finnhub.io/api/v1/calendar/earnings?symbol=" + ticker + "&from=" + earningsFrom + "&to=" + earningsTo + "&token=" + finnhubKey))
                        .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureSpy = httpClient.sendAsync(
                buildAlpacaRequest("/bars?symbols=SPY&timeframe=1Day&start=" + lookback45Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureVix = httpClient.sendAsync(
                HttpRequest.newBuilder()
                        .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1d&range=5d"))
                        .header("User-Agent", "Mozilla/5.0")
                        .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureInsider = httpClient.sendAsync(
                HttpRequest.newBuilder().uri(URI.create("https://finnhub.io/api/v1/stock/insider-sentiment?symbol=" + ticker + "&from=" + insiderFrom + "&to=" + insiderTo + "&token=" + finnhubKey))
                        .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureRec = httpClient.sendAsync(
                HttpRequest.newBuilder().uri(URI.create("https://finnhub.io/api/v1/stock/recommendation?symbol=" + ticker + "&token=" + finnhubKey))
                        .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());

        CompletableFuture.allOf(futureD1, futureH1, futureM15, futureM5, futureOptions, futureNews, futureEarnings, futureSpy, futureVix, futureInsider, futureRec).join();

        if (futureD1.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureD1.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && tickerNode.size() >= 22) {
                sma20 = calculateSmaFromBars(tickerNode, 20);
                rsi14 = calculateRsiFromBars(tickerNode, 14);
                atr14 = calculateAtrFromBars(tickerNode, 14);
                avgVolume30d = calculateAvgVolumeFromBars(tickerNode, 30);

                // EMA crossover (SMA9 vs SMA21) — previously required a separate historicalTrendFunction call
                int d1sz = tickerNode.size();
                double sma9d = calculateSmaFromBars(tickerNode, 9);
                double sma21d = calculateSmaFromBars(tickerNode, 21);
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
                adxValue = d1sz >= 40 ? calculateAdxFromBars(tickerNode, 14) : 25.0;
                adxTrend = adxValue >= 25 ? "Trending" : "Choppy";
                // Prior day high/low — key institutional support/resistance levels
                if (d1sz >= 2) {
                    priorDayHigh = tickerNode.get(d1sz - 2).path("h").asDouble();
                    priorDayLow  = tickerNode.get(d1sz - 2).path("l").asDouble();
                }
                // 20-day Historical Realized Volatility from daily log returns
                int d1len = tickerNode.size();
                if (d1len >= 22) {
                    double sumLogRets = 0;
                    for (int i = d1len - 20; i < d1len; i++) {
                        double c = tickerNode.get(i).path("c").asDouble();
                        double pc = tickerNode.get(i - 1).path("c").asDouble();
                        if (c > 0 && pc > 0) { double lr = Math.log(c / pc); sumLogRets += lr * lr; }
                    }
                    hrv = Math.min(Math.sqrt(sumLogRets / 20.0 * 252.0), 0.60);
                }
                double priceVsSmaScore = sma20 > 0 ? normalizeScore((currentPrice - sma20) / sma20, 2500.0) : 0.0;
                double rsiSignalScore = (rsi14 - 50.0) * 2.0;
                dailyScore = (priceVsSmaScore * 0.60) + (rsiSignalScore * 0.40);
                if (atr14 > 0 && lowToday <= 0.0) {
                    microSupport = currentPrice - (1.5 * atr14);
                }
                if (atr14 > 0 && highToday <= 0.0) {
                    microResistance = currentPrice + (1.5 * atr14);
                }
            }
        }

        if (futureH1.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureH1.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && tickerNode.size() >= 26) {
                double ema12h = calculateEmaFromBars(tickerNode, 12);
                double ema26h = calculateEmaFromBars(tickerNode, 26);
                double macdH = ema12h - ema26h;
                macdH1 = macdH;
                double rsiH1 = calculateRsiFromBars(tickerNode, 9);
                double macdScore = currentPrice > 0 ? normalizeScore(macdH / currentPrice, 5000.0) : 0.0;
                double priceVsEmaScore = ema26h > 0 ? normalizeScore((currentPrice - ema26h) / ema26h, 2500.0) : 0.0;
                double rsiH1Score = (rsiH1 - 50.0) * 2.0;
                h1Score = (priceVsEmaScore * 0.40) + (macdScore * 0.35) + (rsiH1Score * 0.25);
            }
        }

        if (futureM15.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureM15.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && tickerNode.size() >= 21) {
                double sma9m15 = calculateSmaFromBars(tickerNode, 9);
                double sma21m15 = calculateSmaFromBars(tickerNode, 21);
                double rsiM15 = calculateRsiFromBars(tickerNode, 9);
                int sz15 = tickerNode.size();
                double c1m15 = tickerNode.get(sz15 - 1).path("c").asDouble();
                double c2m15 = tickerNode.get(sz15 - 2).path("c").asDouble();
                double c3m15 = sz15 >= 3 ? tickerNode.get(sz15 - 3).path("c").asDouble() : c2m15;
                double smaCrossScore = sma21m15 > 0 ? normalizeScore((sma9m15 - sma21m15) / sma21m15, 3000.0) : 0.0;
                double rsiM15Score = (rsiM15 - 50.0) * 2.0;
                double momentumScore = (c1m15 > c2m15 && c2m15 > c3m15) ? 40.0 : (c1m15 < c2m15 && c2m15 < c3m15) ? -40.0 : 0.0;
                m15Score = (smaCrossScore * 0.50) + (rsiM15Score * 0.30) + (momentumScore * 0.20);
            }
        }

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
                double vwapDistScore = vwap > 0 ? normalizeScore((currentPrice - vwap) / vwap, 3000.0) : 0.0;
                double m5Momentum = (m5c1 > m5c2 && m5c2 > m5c3) ? 50.0 : (m5c1 < m5c2 && m5c2 < m5c3) ? -50.0 : (m5c1 > m5c2) ? 20.0 : -20.0;
                m5Score = (vwapDistScore * 0.55) + (m5Momentum * 0.45);
            }
        }

        if (Math.abs(currentPrice - vwap) / currentPrice > 0.08) {
            vwap = currentPrice;
        }

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

        // ── Smart money: insider sentiment + analyst consensus ────────────────
        double smartMoneyScore = 0.0;
        double insiderMspr    = 0.0;
        int    insiderBuys    = 0, insiderSells = 0;
        int    analystBuy     = 0, analystHold = 0, analystSell = 0;

        try {
            if (futureInsider.get().statusCode() == 200) {
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
            if (futureRec.get().statusCode() == 200) {
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
            if (futureEarnings.get().statusCode() == 200) {
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
                    double spySma20 = calculateSmaFromBars(spyBars, 20);
                    double spyRsi   = calculateRsiFromBars(spyBars, 14);
                    double spyPrice = spyBars.get(spyBars.size() - 1).path("c").asDouble();
                    double smaScore = spySma20 > 0 ? ((spyPrice - spySma20) / spySma20) * 250.0 : 0;
                    double rsiScore = (spyRsi - 50.0) * 1.5;
                    spyScore = Math.max(-100, Math.min(100, (smaScore * 0.6) + (rsiScore * 0.4)));
                    spyTrend = spyScore > 20 ? "Bull" : spyScore < -20 ? "Bear" : "Neutral";
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

        double impliedVolatility = 0.0;
        if (futureOptions.get().statusCode() == 200) {
            JsonNode snapshots = objectMapper.readTree(futureOptions.get().body()).path("snapshots");
            double totalIv = 0, totalOi = 0;
            if (snapshots.isObject()) {
                var iterator = snapshots.fields();
                while (iterator.hasNext()) {
                    JsonNode contract = iterator.next().getValue();
                    double iv = contract.path("implied_volatility").asDouble(0);
                    double oi = Math.max(1.0, contract.path("open_interest").asDouble(1.0));
                    // Accept only realistic ATM IV range; OI-weighted average
                    if (iv > 0.05 && iv < 0.80) {
                        totalIv += iv * oi;
                        totalOi += oi;
                    }
                }
            }
            if (totalOi > 0) impliedVolatility = totalIv / totalOi;
        }

        // Blend options IV with 20-day HRV: 55% options, 45% realized — anchors to actual movement.
        // If options data unavailable, fall straight back to HRV.
        if (impliedVolatility > 0) {
            impliedVolatility = (impliedVolatility * 0.55) + (hrv * 0.45);
        } else {
            impliedVolatility = hrv;
        }
        impliedVolatility = Math.min(impliedVolatility, 0.55); // hard cap at 55%

        // Realized vol is the actual driver of expected move (IV overstates by ~15%)
        double realizedVolEstimate = impliedVolatility * 0.85;

        double oneDayExpectedMove = currentPrice * realizedVolEstimate * Math.sqrt(1.0 / 252.0);
        double fiveDayExpectedMove = currentPrice * realizedVolEstimate * Math.sqrt(5.0 / 252.0);

        double customExpectedMove = 0.0;
        if (customDays > 0) {
            customExpectedMove = currentPrice * realizedVolEstimate * Math.sqrt(customDays / 252.0);
        }

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
        
        double customUpper = (customDays > 0) ? rangeAnchor + customExpectedMove : 0.0;
        double customLower = (customDays > 0) ? rangeAnchor - customExpectedMove : 0.0;

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

        double strikeBuy = getNearestOptionStrike(dynamicEntry);
        double strikeSell = getNearestOptionStrike(dynamicTp);

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
            spreadShortStrike = getNearestOptionStrike(strikeBuy + spreadWidth);   // call spread: sell OTM call above
        } else if (totalConfluenceScore <= -15.0) {
            spreadShortStrike = getNearestOptionStrike(strikeBuy - spreadWidth);   // put spread:  sell OTM put below
        } else {
            spreadShortStrike = strikeSell; // iron condor uses IC legs below
        }

        // Iron Condor legs — used when verdict is STAND_DOWN_COLLECT_PREMIUM
        double icWing = atr14 > 0 ? atr14 * 0.5 : oneDayExpectedMove * 0.5;
        double icPutSell  = getNearestOptionStrike(microSupport);
        double icPutBuy   = getNearestOptionStrike(microSupport - icWing);
        double icCallSell = getNearestOptionStrike(microResistance);
        double icCallBuy  = getNearestOptionStrike(microResistance + icWing);

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

        // Pre-computed strategy name and options line — model outputs these verbatim, zero reasoning required
        String strategyName;
        String optionsLine;
        if (totalConfluenceScore >= 15.0) {
            strategyName = "Bull Call Debit Spread";
            optionsLine  = "Buy 1× $" + String.format("%.2f", strikeBuy)
                         + " Call · Sell 1× $" + String.format("%.2f", spreadShortStrike)
                         + " Call · Expires " + targetExpiration;
        } else if (totalConfluenceScore <= -15.0) {
            strategyName = "Bear Put Debit Spread";
            optionsLine  = "Buy 1× $" + String.format("%.2f", strikeBuy)
                         + " Put · Sell 1× $" + String.format("%.2f", spreadShortStrike)
                         + " Put · Expires " + targetExpiration;
        } else {
            strategyName = "Iron Condor";
            optionsLine  = "Sell $" + String.format("%.2f", icPutSell)
                         + " Put · Buy $" + String.format("%.2f", icPutBuy)
                         + " Put · Sell $" + String.format("%.2f", icCallSell)
                         + " Call · Buy $" + String.format("%.2f", icCallBuy)
                         + " Call · Expires " + targetExpiration;
        }

        return String.format(",\"session_status\":\"%s\",\"macro_daily_trend_score\":%.1f,\"h1_radar_score\":%.1f,\"m15_radar_score\":%.1f,\"m5_radar_score\":%.1f,\"total_confluence_score\":%.1f,\"intraday_vwap\":%.2f,\"micro_support\":%.2f,\"micro_resistance\":%.2f,\"implied_volatility\":\"%.2f%%\",\"tomorrow_upper\":%.2f,\"tomorrow_lower\":%.2f,\"next_week_upper\":%.2f,\"next_week_lower\":%.2f,\"custom_upper\":%.2f,\"custom_lower\":%.2f,\"custom_days\":%d,\"automated_trade_verdict\":\"%s\",\"final_entry\":%.2f,\"final_tp\":%.2f,\"final_sl\":%.2f,\"strike_buy\":%.2f,\"spread_short_strike\":%.2f,\"strike_sell\":%.2f,\"target_expiration\":\"%s\",\"strategy_name\":\"%s\",\"options_line\":\"%s\",\"ema_crossover_status\":\"%s\",\"calculated_rsi_14d\":%.1f,\"calculated_support\":%.2f,\"calculated_resistance\":%.2f"
                + ",\"buy_strength\":\"%s\",\"buy_score\":%d,\"sell_score\":%d,\"rsi_14d\":%.1f"
                + ",\"active_buy_signals\":\"%s\",\"active_sell_signals\":\"%s\""
                + ",\"smart_money_score\":%.1f,\"smart_money_verdict\":\"%s\",\"smart_money_conflict\":%b"
                + ",\"insider_mspr\":%.4f,\"insider_buys\":%d,\"insider_sells\":%d"
                + ",\"analyst_buy\":%d,\"analyst_hold\":%d,\"analyst_sell\":%d"
                + ",\"market_regime\":\"%s\",\"regime_note\":\"%s\",\"vix_level\":%.1f,\"spy_trend\":\"%s\""
                + ",\"adx_value\":%.1f,\"adx_trend\":\"%s\""
                + ",\"prior_day_high\":%.2f,\"prior_day_low\":%.2f"
                + ",\"vwap_upper_1sd\":%.2f,\"vwap_lower_1sd\":%.2f"
                + ",\"earnings_flag\":%b,\"earnings_date\":\"%s\",\"earnings_days_away\":%d"
                + ",\"suggested_shares\":%d,\"suggested_contracts\":%d,\"rr_ratio\":\"%s\"",
                sessionStatus, dailyScore, h1Score, m15Score, m5Score, totalConfluenceScore, vwap, microSupport, microResistance, impliedVolatility * 100, tomorrowUpper, tomorrowLower, nextWeekUpper, nextWeekLower, customUpper, customLower, customDays, dynamicVerdict, dynamicEntry, dynamicTp, dynamicSl, strikeBuy, spreadShortStrike, strikeSell, targetExpiration, strategyName, optionsLine, emaCrossoverStatus, rsi14, calculatedSupport, calculatedResistance,
                buyStrength, buyScore, sellScore, rsi14,
                activeBuySignals, activeSellSignals,
                smartMoneyScore, smartMoneyVerdict, smConflict,
                insiderMspr, insiderBuys, insiderSells,
                analystBuy, analystHold, analystSell,
                marketRegime, regimeNote, vixLevel, spyTrend,
                adxValue, adxTrend,
                priorDayHigh, priorDayLow,
                vwapUpper, vwapLower,
                earningsFlag, earningsDate, earningsDaysAway,
                suggestedShares, suggestedContracts, rrDisplay);
    }

    // Fetches Yahoo + full MTF analysis for one ticker — shared by both stockPriceFunction and the scanner.
    private String scanTicker(String ticker) {
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
            HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
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

            currentPrice = alpacaStreamService.getLatestQuote(ticker)
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

    @Bean
    @Description("USE THIS tool to fetch market data. CRITICAL: If the user asks for a specific timeframe (e.g. 'in 4 weeks', '3 months'), you MUST pass the equivalent trading days into 'customTradingDays' (1 week=5, 4 weeks=20, 3 months=63).")
    public Function<PriceRequest, String> stockPriceFunction() {
        return request -> {
            String ticker = request.symbol().replaceAll("[\"']", "").trim().toUpperCase();
            int customDays = request.customTradingDays() != null ? request.customTradingDays() : 0;
            log.info("[TOOL] stockPriceFunction called — ticker={}, customDays={}", ticker, customDays);

            // Ensure this ticker is being tracked by the WebSocket stream
            alpacaStreamService.subscribe(ticker);

            try {
                HttpRequest liveRequest = HttpRequest.newBuilder()
                        .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/" + ticker + "?includePrePost=true&interval=1m&range=1d"))
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .GET()
                        .build();

                HttpResponse<String> res = httpClient.send(liveRequest, HttpResponse.BodyHandlers.ofString());
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
                currentPrice = alpacaStreamService.getLatestQuote(ticker)
                        .map(AlpacaStreamService.LiveQuote::price)
                        .filter(p -> p > 0)
                        .orElse(currentPrice);

                double percentChange = (priorClose > 0) ? ((currentPrice - priorClose) / priorClose) * 100.0 : 0.0;
                String pctString = String.format("%s%.2f%%", (percentChange >= 0 ? "+" : ""), percentChange);

                String payload = String.format("{\"symbol\":\"%s\",\"company_name\":\"%s\",\"current_price\":%.2f,\"change\":%.2f,\"percent_change\":\"%s\",\"volume\":\"%s\",\"high_today\":%.2f,\"low_today\":%.2f}",
                        ticker, ticker, currentPrice, currentPrice - priorClose, pctString, String.format("%,d", vol), highToday, lowToday);

                String result = payload.substring(0, payload.length() - 1) + processIntradayMtfAlignment(ticker, currentPrice, highToday, lowToday, vol, priorClose, customDays) + "}";
                return result;
            } catch (Exception e) {
                return String.format("{\"error\":\"CRITICAL FAILURE: Exception parsing data streams for %s.\"}", ticker);
            }
        };
    }

    @Bean
    @Description("USE THIS tool to get historical trend data, RSI, and EMA crossover status. Requires only the ticker symbol.")
    public Function<TrendRequest, String> historicalTrendFunction() {
        return request -> {
            String ticker = request.symbol().replaceAll("[\"']", "").trim().toUpperCase();
            try {
                String startLookback = ZonedDateTime.now(ZoneId.of("America/New_York")).minusDays(60).format(DateTimeFormatter.ISO_INSTANT);
                HttpResponse<String> res = httpClient.send(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Day&start=" + startLookback + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
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
        };
    }

    // ── Pre-market scanner helpers ────────────────────────────────────────────

    private String scanPreMarketTicker(String ticker) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/" + ticker + "?includePrePost=true&interval=5m&range=1d"))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .timeout(Duration.ofSeconds(6))
                    .GET().build();
            HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
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

    private String detectPreMarketPattern(List<double[]> bars, double pmChangePercent, double priorClose) {
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

    private double extractPreMarketChangePct(String json) {
        try {
            String raw = objectMapper.readTree(json).path("percent_change").asText("0%");
            return Double.parseDouble(raw.replace("%", "").replace("+", "").trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    @Bean
    @Description("USE THIS tool when the user asks about pre-market movers, gap plays, what to watch before the open, or pre-market scanner. Scans a curated watchlist for pre-market price movement and pattern (Gap & Go, Gap & Fade, Consolidating). Returns top movers with full options analysis — render results as a pre-market table.")
    public Function<EmptyRequest, String> preMarketScannerFunction() {
        return request -> {
            log.info("[TOOL] preMarketScannerFunction called");
            try {
                List<String> watchlist = new ArrayList<>(List.of(
                        "SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL", "AMD",
                        "AVGO", "NFLX", "JPM", "BAC", "GS", "XOM", "PLTR", "ARM", "MSTR", "COIN",
                        "SOFI", "RIVN", "F", "GE", "INTC", "MU", "SMCI", "UBER", "HOOD", "DIS",
                        "TSLL", "NVDL", "AAPU", "METU", "AMZU", "MSFU", "CONL", "MSTU",
                        "TQQQ", "SPXL", "SOXL", "LABU", "FNGU"
                ));
                // Append Yahoo most-actives to catch any fresh names not in the static list
                try {
                    HttpRequest req = HttpRequest.newBuilder()
                            .uri(URI.create("https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=10&fields=symbol,regularMarketPrice,regularMarketVolume"))
                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)").GET().build();
                    HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                    if (resp.statusCode() == 200) {
                        JsonNode quotes = objectMapper.readTree(resp.body()).path("finance").path("result").get(0).path("quotes");
                        if (quotes.isArray()) {
                            for (JsonNode q : quotes) {
                                String sym = q.path("symbol").asText("").trim();
                                if (!sym.isBlank() && !sym.contains("-") && !sym.contains(".") && !watchlist.contains(sym))
                                    watchlist.add(sym);
                            }
                        }
                    }
                } catch (Exception ignored) {}

                List<CompletableFuture<String>> futures = new ArrayList<>();
                for (String ticker : watchlist) {
                    futures.add(CompletableFuture.supplyAsync(() -> scanPreMarketTicker(ticker)));
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                List<String> valid = new ArrayList<>();
                for (CompletableFuture<String> f : futures) {
                    String result = f.join();
                    if (result != null) valid.add(result);
                }

                if (valid.isEmpty()) return "{\"error\":\"No pre-market movers found above threshold.\"}";

                valid.sort((a, b) -> Double.compare(Math.abs(extractPreMarketChangePct(b)), Math.abs(extractPreMarketChangePct(a))));

                List<String> top6 = valid.subList(0, Math.min(6, valid.size()));
                StringBuilder array = new StringBuilder("[");
                for (int i = 0; i < top6.size(); i++) {
                    if (i > 0) array.append(",");
                    array.append(top6.get(i));
                }
                array.append("]");

                return String.format("{\"status\":\"success\",\"ticker_count\":%d,\"pre_market_scan_results\":%s}",
                        top6.size(), array);
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
            String expiryPref = "default";
            try {
                List<String> universe = new ArrayList<>(List.of(
                        // Stocks $3–$80 range
                        "TSLA", "NVDA", "AAPL", "AMD", "PLTR", "SOFI", "RIVN", "INTC", "F", "BAC",
                        "HOOD", "COIN", "MSTR", "MU", "SMCI", "ARM", "UBER", "NFLX", "AVGO", "META",
                        "AMZN", "MSFT", "GOOGL", "DIS", "GE", "XOM", "AAL", "SNAP", "RBLX", "NIO",
                        "LCID", "SOUN", "BBAI", "IONQ", "QBTS", "RGTI", "OPEN", "UPST", "AFRM",
                        // Leveraged ETFs
                        "TSLL", "NVDL", "AAPU", "METU", "AMZU", "MSFU", "CONL", "MSTU",
                        "TQQQ", "SPXL", "SOXL", "LABU", "FNGU", "TECL", "WEBL", "DPST"
                ));

                ZoneId et = ZoneId.of("America/New_York");
                ZonedDateTime nowET = ZonedDateTime.now(et);
                String startDate = nowET.minusDays(15).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                String endDate   = nowET.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                LocalDate today = nowET.toLocalDate();

                record WheelCandidate(String ticker, double price, double iv, double putStrike,
                                      double putPremium, double weeklyReturnPct, String expiryDate,
                                      long volume, String callStrike, double callPremium, boolean isEtf) {}

                List<CompletableFuture<WheelCandidate>> futures = new ArrayList<>();

                for (String ticker : universe) {
                    final String sym = ticker;
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        try {
                            // 1. Get price + volume from Alpaca bars
                            String barsUrl = "https://data.alpaca.markets/v2/stocks/bars?symbols=" + sym
                                    + "&timeframe=1Day&start=" + startDate + "&end=" + endDate + "&limit=15&feed=iex";
                            HttpRequest barsReq = HttpRequest.newBuilder().uri(URI.create(barsUrl))
                                    .header("APCA-API-KEY-ID", apiKey)
                                    .header("APCA-API-SECRET-KEY", apiSecret).GET().build();
                            HttpResponse<String> barsResp = httpClient.send(barsReq, HttpResponse.BodyHandlers.ofString());
                            if (barsResp.statusCode() != 200) return null;
                            JsonNode bars = objectMapper.readTree(barsResp.body()).path("bars").path(sym);
                            if (!bars.isArray() || bars.size() < 3) return null;
                            double price = bars.get(bars.size() - 1).path("c").asDouble(0);
                            long volume  = bars.get(bars.size() - 1).path("v").asLong(0);

                            // Filter: price $3–$80, volume > 1M
                            if (price < 3.0 || price > 80.0) return null;
                            if (volume < 1_000_000) return null;

                            // 2. Trend check — must be bullish or neutral (daily SMA20)
                            double sma10 = calculateSmaFromBars(bars, Math.min(10, bars.size()));
                            // Bearish: recent bars consistently below sma10
                            double last3avg = 0;
                            int last3cnt = Math.min(3, bars.size());
                            for (int i = bars.size() - last3cnt; i < bars.size(); i++)
                                last3avg += bars.get(i).path("c").asDouble();
                            last3avg /= last3cnt;
                            if (sma10 > 0 && last3avg < sma10 * 0.95) return null; // strongly bearish — skip

                            // 3. Get near-ATM IV from today's options snapshot
                            String optUrl = "https://data.alpaca.markets/v1beta1/options/snapshots/" + sym
                                    + "?feed=indicative&strike_price_gte=" + (price * 0.95)
                                    + "&strike_price_lte=" + (price * 1.05) + "&limit=20";
                            HttpRequest optReq = HttpRequest.newBuilder().uri(URI.create(optUrl))
                                    .header("APCA-API-KEY-ID", apiKey)
                                    .header("APCA-API-SECRET-KEY", apiSecret).GET().build();
                            HttpResponse<String> optResp = httpClient.send(optReq, HttpResponse.BodyHandlers.ofString());

                            double iv = 0;
                            if (optResp.statusCode() == 200) {
                                JsonNode snapshots = objectMapper.readTree(optResp.body()).path("snapshots");
                                Iterator<Map.Entry<String, JsonNode>> it = snapshots.fields();
                                double ivSum = 0; int ivCount = 0;
                                while (it.hasNext()) {
                                    Map.Entry<String, JsonNode> entry = it.next();
                                    // Parse IV from bid/ask spread as proxy: mid/price ≈ IV×sqrt(T/365)
                                    JsonNode snap = entry.getValue();
                                    double ap = snap.path("latestQuote").path("ap").asDouble(0);
                                    double bp = snap.path("latestQuote").path("bp").asDouble(0);
                                    if (ap <= 0 || bp <= 0) continue;
                                    double mid = (ap + bp) / 2.0;
                                    // Back-calculate IV: IV ≈ mid / (price * sqrt(1/52)) for ~1-week ATM
                                    double rawIv = (price > 0) ? (mid / price) * Math.sqrt(52.0) : 0;
                                    if (rawIv > 0.10 && rawIv < 5.0) { ivSum += rawIv; ivCount++; }
                                }
                                if (ivCount > 0) iv = ivSum / ivCount;
                            }

                            // Filter: IV must be > 30%
                            if (iv < 0.30) return null;

                            // Put strike: 12.5% OTM, rounded to nearest $0.50
                            double putStrikeRaw = price * 0.875;
                            double putStrike = Math.round(putStrikeRaw * 2.0) / 2.0;
                            double capital = putStrike * 100.0;

                            // Build candidate expiry windows based on user preference
                            // weekly = next Friday ≥7 days, biweekly = next Friday ≥14 days, monthly = next Friday ≥28 days
                            LocalDate wkExp = today.plusDays(7);
                            while (wkExp.getDayOfWeek().getValue() != 5) wkExp = wkExp.plusDays(1);
                            LocalDate bwExp = today.plusDays(14);
                            while (bwExp.getDayOfWeek().getValue() != 5) bwExp = bwExp.plusDays(1);
                            LocalDate moExp = today.plusDays(28);
                            while (moExp.getDayOfWeek().getValue() != 5) moExp = moExp.plusDays(1);

                            // Order of expiries to try based on user preference
                            List<LocalDate> expiriesToTry;
                            if ("monthly".equals(expiryPref)) {
                                expiriesToTry = List.of(moExp, bwExp, wkExp);
                            } else if ("biweekly".equals(expiryPref) || "2week".equals(expiryPref) || "2 week".equals(expiryPref)) {
                                expiriesToTry = List.of(bwExp, wkExp, moExp);
                            } else {
                                // Default: weekly first for all tickers; fallback biweekly → monthly
                                expiriesToTry = List.of(wkExp, bwExp, moExp);
                            }

                            // Try each expiry until one hits ≥1%/week
                            LocalDate chosenExp = null;
                            double putPremium = 0, weeklyReturn_final = 0;
                            String expiryLabel = "weekly";
                            for (LocalDate exp : expiriesToTry) {
                                long days = today.until(exp, java.time.temporal.ChronoUnit.DAYS);
                                double weeks = days / 7.0;
                                double premium = Math.round(putStrike * iv * Math.sqrt(days / 365.0) * 0.30 * 100.0) / 100.0;
                                double wkReturn = (premium * 100.0) / capital / weeks * 100.0;
                                if (wkReturn >= 1.0) {
                                    chosenExp = exp;
                                    putPremium = premium;
                                    weeklyReturn_final = wkReturn;
                                    long expDays = today.until(exp, java.time.temporal.ChronoUnit.DAYS);
                                    expiryLabel = expDays <= 10 ? "weekly" : expDays <= 21 ? "2-week" : "monthly";
                                    break;
                                }
                            }
                            if (chosenExp == null) return null; // no expiry hits 1%/week

                            long chosenDays = today.until(chosenExp, java.time.temporal.ChronoUnit.DAYS);
                            double chosenWeeks = chosenDays / 7.0;
                            double totalReturn = weeklyReturn_final * chosenWeeks;
                            String totalSuffix = "weekly".equals(expiryLabel) ? "weekly" : expiryLabel + " · total ~" + String.format("%.1f", totalReturn) + "%";
                            String bestExpiry = chosenExp.format(DateTimeFormatter.ofPattern("MMM dd, yyyy")) + " (" + totalSuffix + ")";

                            // Covered call strike: 5% above current price
                            double callStrikeVal = Math.round(price * 1.05 * 2.0) / 2.0;
                            double callPremium = Math.round(putPremium * 0.6 * 100.0) / 100.0;

                            boolean isEtf = sym.equals("TSLL") || sym.equals("NVDL") || sym.equals("AAPU")
                                    || sym.equals("METU") || sym.equals("AMZU") || sym.equals("MSFU")
                                    || sym.equals("CONL") || sym.equals("MSTU") || sym.equals("TQQQ")
                                    || sym.equals("SPXL") || sym.equals("SOXL") || sym.equals("LABU")
                                    || sym.equals("FNGU") || sym.equals("TECL") || sym.equals("WEBL") || sym.equals("DPST");

                            return new WheelCandidate(sym, price, iv * 100, putStrike, putPremium,
                                    weeklyReturn_final, bestExpiry, volume,
                                    String.format("%.2f", callStrikeVal), callPremium, isEtf);
                        } catch (Exception e) {
                            return null;
                        }
                    }));
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                List<WheelCandidate> candidates = new ArrayList<>();
                for (CompletableFuture<WheelCandidate> f : futures) {
                    WheelCandidate c = f.join();
                    if (c != null) candidates.add(c);
                }

                // Sort by weekly return % descending
                candidates.sort((a, b) -> Double.compare(b.weeklyReturnPct(), a.weeklyReturnPct()));
                List<WheelCandidate> top5 = candidates.subList(0, Math.min(5, candidates.size()));

                if (top5.isEmpty()) return "{\"error\":\"No wheel candidates found matching criteria.\"}";

                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < top5.size(); i++) {
                    WheelCandidate c = top5.get(i);
                    if (i > 0) sb.append(",");
                    sb.append(String.format(
                        "{\"ticker\":\"%s\",\"price\":%.2f,\"iv\":%.1f,\"put_strike\":%.2f," +
                        "\"put_premium\":%.2f,\"total_premium_per_contract\":%.0f," +
                        "\"weekly_return_pct\":%.2f,\"expiry\":\"%s\",\"volume\":%d," +
                        "\"call_strike\":\"%s\",\"call_premium\":%.2f,\"is_etf\":%b}",
                        c.ticker(), c.price(), c.iv(), c.putStrike(), c.putPremium(),
                        c.putPremium() * 100, c.weeklyReturnPct(), c.expiryDate(),
                        c.volume(), c.callStrike(), c.callPremium(), c.isEtf()));
                }
                sb.append("]");

                return String.format("{\"status\":\"success\",\"scan_date\":\"%s\",\"wheel_candidates\":%s}",
                        today, sb);

            } catch (Exception e) {
                log.error("[WHEEL] scan failed", e);
                return "{\"error\":\"Wheel scan failed.\"}";
            }
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks for top options plays, trending tickers, market movers, or a broad market scan. Returns full multi-timeframe analysis and options levels for the top 5 trending US stocks — render results as a table.")
    public Function<EmptyRequest, String> generalMarketScannerFunction() {
        return request -> {
            log.info("[TOOL] generalMarketScannerFunction called");
            try {
                // 1. Fetch most-active tickers by volume (more reliable than trending/social buzz)
                //    Fall back to trending if screener is unavailable
                List<String> tickers = new ArrayList<>();
                for (String scrId : new String[]{"most_actives", "trending"}) {
                    if (tickers.size() >= 5) break;
                    String url = scrId.equals("most_actives")
                            ? "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=15&fields=symbol,regularMarketPrice,regularMarketVolume"
                            : "https://query1.finance.yahoo.com/v1/finance/trending/US";
                    HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)").GET().build();
                    HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                    if (resp.statusCode() != 200) continue;
                    JsonNode root = objectMapper.readTree(resp.body());
                    JsonNode quotes = scrId.equals("most_actives")
                            ? root.path("finance").path("result").get(0).path("quotes")
                            : root.path("finance").path("result").get(0).path("quotes");
                    if (!quotes.isArray()) continue;
                    for (JsonNode q : quotes) {
                        if (tickers.size() >= 8) break;
                        String sym = q.path("symbol").asText("").trim();
                        double price = q.path("regularMarketPrice").asDouble(0);
                        long vol = q.path("regularMarketVolume").asLong(0);
                        // Quality filters: skip crypto/foreign, penny stocks, thin volume
                        if (sym.isBlank() || sym.contains("-") || sym.contains(".")) continue;
                        if (price > 0 && price < 5.0) continue;
                        if (vol > 0 && vol < 300_000) continue;
                        if (!tickers.contains(sym)) {
                            tickers.add(sym);
                            alpacaStreamService.subscribe(sym);
                        }
                    }
                }

                if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

                // 2. Run full MTF analysis for each ticker concurrently
                List<CompletableFuture<String>> futures = new ArrayList<>();
                for (String ticker : tickers) {
                    futures.add(CompletableFuture.supplyAsync(() -> scanTicker(ticker)));
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                // 3. Aggregate, sort by absolute confluence score, keep top 5
                List<String> valid = new ArrayList<>();
                for (CompletableFuture<String> f : futures) {
                    String result = f.join();
                    if (result != null) valid.add(result);
                }
                valid.sort((a, b) -> {
                    double scoreA = extractConfluenceScore(a);
                    double scoreB = extractConfluenceScore(b);
                    return Double.compare(Math.abs(scoreB), Math.abs(scoreA));
                });
                List<String> top5 = valid.subList(0, Math.min(5, valid.size()));
                StringBuilder array = new StringBuilder("[");
                for (int i = 0; i < top5.size(); i++) {
                    if (i > 0) array.append(",");
                    array.append(top5.get(i));
                }
                array.append("]");

                return String.format("{\"status\":\"success\",\"ticker_count\":%d,\"scan_results\":%s}",
                        top5.size(), array);

            } catch (Exception e) {
                return "{\"error\":\"CRITICAL FAILURE: Exception parsing scanner streams.\"}";
            }
        };
    }
}