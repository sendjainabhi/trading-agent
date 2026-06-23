package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Scanner orchestration service — assembles ticker universes and delegates per-ticker
 * analysis to {@link StockAnalysisEngine}.
 */
@Service
public class MarketScannerService {

    // ── Injected collaborators ────────────────────────────────────────────────

    final StockAnalysisEngine engine;
    final AlpacaClient alpacaClient;
    final AlpacaStreamService alpacaStreamService;

    public MarketScannerService(StockAnalysisEngine engine,
                                AlpacaClient alpacaClient,
                                AlpacaStreamService alpacaStreamService) {
        this.engine = engine;
        this.alpacaClient = alpacaClient;
        this.alpacaStreamService = alpacaStreamService;
    }

    // ── General market scan ───────────────────────────────────────────────────

    /**
     * General market scan: most-active tickers, full MTF, top 5 by absolute confluence score.
     * Backing method for the generalMarketScannerFunction @Bean.
     */
    public String scanMarket() throws Exception {
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
            HttpResponse<String> resp = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) continue;
            JsonNode root = alpacaClient.objectMapper.readTree(resp.body());
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
            futures.add(CompletableFuture.supplyAsync(() -> engine.scanTicker(ticker)));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // 3. Aggregate, sort by absolute confluence score, keep top 5
        List<String> valid = new ArrayList<>();
        for (CompletableFuture<String> f : futures) {
            String result = f.join();
            if (result != null) valid.add(result);
        }
        valid.sort((a, b) -> {
            double scoreA = engine.extractConfluenceScore(a);
            double scoreB = engine.extractConfluenceScore(b);
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
    }

    // ── Bearish scan ──────────────────────────────────────────────────────────

    /**
     * Bearish scan: day-losers + most-actives, returns top 5 with most negative confluence score.
     * Backing method for the bearishScannerFunction @Bean.
     */
    public String scanBearish() throws Exception {
        List<String> tickers = new ArrayList<>();
        for (String scrId : new String[]{"day_losers", "most_actives"}) {
            if (tickers.size() >= 8) break;
            String url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=" + scrId + "&count=15&fields=symbol,regularMarketPrice,regularMarketVolume";
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)").GET().build();
            HttpResponse<String> resp = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) continue;
            JsonNode root = alpacaClient.objectMapper.readTree(resp.body());
            JsonNode quotes = root.path("finance").path("result").get(0).path("quotes");
            if (!quotes.isArray()) continue;
            for (JsonNode q : quotes) {
                if (tickers.size() >= 8) break;
                String sym = q.path("symbol").asText("").trim();
                double price = q.path("regularMarketPrice").asDouble(0);
                long vol = q.path("regularMarketVolume").asLong(0);
                if (sym.isBlank() || sym.contains("-") || sym.contains(".")) continue;
                if (price > 0 && price < 5.0) continue;
                if (vol > 0 && vol < 300_000) continue;
                if (!tickers.contains(sym)) { tickers.add(sym); alpacaStreamService.subscribe(sym); }
            }
        }
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (String ticker : tickers) futures.add(CompletableFuture.supplyAsync(() -> engine.scanTicker(ticker)));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        List<String> valid = new ArrayList<>();
        for (CompletableFuture<String> f : futures) {
            String r = f.join();
            if (r != null && engine.extractConfluenceScore(r) < 0) valid.add(r);
        }
        valid.sort((a, b) -> Double.compare(engine.extractConfluenceScore(a), engine.extractConfluenceScore(b)));
        List<String> top5 = valid.subList(0, Math.min(5, valid.size()));
        StringBuilder array = new StringBuilder("[");
        for (int i = 0; i < top5.size(); i++) { if (i > 0) array.append(","); array.append(top5.get(i)); }
        array.append("]");
        return String.format("{\"status\":\"success\",\"ticker_count\":%d,\"scan_results\":%s}", top5.size(), array);
    }

    // ── Bullish scan ──────────────────────────────────────────────────────────

    /**
     * Bullish scan: day-gainers + most-actives, returns top 5 with most positive confluence score.
     * Backing method for the bullishScannerFunction @Bean.
     */
    public String scanBullish() throws Exception {
        List<String> tickers = new ArrayList<>();
        for (String scrId : new String[]{"day_gainers", "most_actives"}) {
            if (tickers.size() >= 8) break;
            String url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=" + scrId + "&count=15&fields=symbol,regularMarketPrice,regularMarketVolume";
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)").GET().build();
            HttpResponse<String> resp = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) continue;
            JsonNode root = alpacaClient.objectMapper.readTree(resp.body());
            JsonNode quotes = root.path("finance").path("result").get(0).path("quotes");
            if (!quotes.isArray()) continue;
            for (JsonNode q : quotes) {
                if (tickers.size() >= 8) break;
                String sym = q.path("symbol").asText("").trim();
                double price = q.path("regularMarketPrice").asDouble(0);
                long vol = q.path("regularMarketVolume").asLong(0);
                if (sym.isBlank() || sym.contains("-") || sym.contains(".")) continue;
                if (price > 0 && price < 5.0) continue;
                if (vol > 0 && vol < 300_000) continue;
                if (!tickers.contains(sym)) { tickers.add(sym); alpacaStreamService.subscribe(sym); }
            }
        }
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (String ticker : tickers) futures.add(CompletableFuture.supplyAsync(() -> engine.scanTicker(ticker)));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        List<String> valid = new ArrayList<>();
        for (CompletableFuture<String> f : futures) {
            String r = f.join();
            if (r != null && engine.extractConfluenceScore(r) > 0) valid.add(r);
        }
        valid.sort((a, b) -> Double.compare(engine.extractConfluenceScore(b), engine.extractConfluenceScore(a)));
        List<String> top5 = valid.subList(0, Math.min(5, valid.size()));
        StringBuilder array = new StringBuilder("[");
        for (int i = 0; i < top5.size(); i++) { if (i > 0) array.append(","); array.append(top5.get(i)); }
        array.append("]");
        return String.format("{\"status\":\"success\",\"ticker_count\":%d,\"scan_results\":%s}", top5.size(), array);
    }

    // ── Swing scan ────────────────────────────────────────────────────────────

    /**
     * Swing scan: most-actives + gainers + losers, keeps tickers with SWING_LONG/SHORT/RANGE_PLAY signals.
     * Backing method for the swingScannerFunction @Bean.
     */
    public String scanSwing() throws Exception {
        // Broader universe: most-active + gainers + losers all produce swing setups
        List<String> tickers = new ArrayList<>();
        for (String scrId : new String[]{"most_actives", "day_gainers", "day_losers"}) {
            if (tickers.size() >= 15) break;
            String url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=" + scrId + "&count=20&fields=symbol,regularMarketPrice,regularMarketVolume";
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)").GET().build();
            HttpResponse<String> resp = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) continue;
            JsonNode root = alpacaClient.objectMapper.readTree(resp.body());
            JsonNode quotes = root.path("finance").path("result").get(0).path("quotes");
            if (!quotes.isArray()) continue;
            for (JsonNode q : quotes) {
                if (tickers.size() >= 15) break;
                String sym = q.path("symbol").asText("").trim();
                double price = q.path("regularMarketPrice").asDouble(0);
                long vol = q.path("regularMarketVolume").asLong(0);
                if (sym.isBlank() || sym.contains("-") || sym.contains(".")) continue;
                if (price > 0 && price < 5.0) continue;
                if (vol > 0 && vol < 500_000) continue; // minimum volume for swing liquidity
                if (!tickers.contains(sym)) { tickers.add(sym); alpacaStreamService.subscribe(sym); }
            }
        }
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        // Full MTF analysis (includes swing high/low detection) for each ticker concurrently
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (String ticker : tickers) futures.add(CompletableFuture.supplyAsync(() -> engine.scanTicker(ticker)));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Keep only stocks with a confirmed swing signal
        List<String> valid = new ArrayList<>();
        for (CompletableFuture<String> f : futures) {
            String r = f.join();
            if (r != null && !"NONE".equals(engine.extractSwingSignal(r))) valid.add(r);
        }

        // Sort: near-level setups (SWING_LONG/SHORT) before range plays; within each type, higher volume first
        valid.sort((a, b) -> {
            String sigA = engine.extractSwingSignal(a);
            String sigB = engine.extractSwingSignal(b);
            int rankA = "RANGE_PLAY".equals(sigA) ? 2 : "SWING_LONG".equals(sigA) ? 0 : 1;
            int rankB = "RANGE_PLAY".equals(sigB) ? 2 : "SWING_LONG".equals(sigB) ? 0 : 1;
            return Integer.compare(rankA, rankB);
        });

        List<String> top = valid.subList(0, Math.min(6, valid.size()));
        StringBuilder array = new StringBuilder("[");
        for (int i = 0; i < top.size(); i++) { if (i > 0) array.append(","); array.append(top.get(i)); }
        array.append("]");
        return String.format("{\"status\":\"success\",\"ticker_count\":%d,\"swing_scan_results\":%s}", top.size(), array);
    }

    // ── Pre-market scan ───────────────────────────────────────────────────────

    /**
     * Pre-market scan: curated watchlist + Yahoo most-actives, returns top movers with full options analysis.
     * Backing method for the preMarketScannerFunction @Bean.
     */
    public String scanPreMarket() throws Exception {
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
            HttpResponse<String> resp = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                JsonNode quotes = alpacaClient.objectMapper.readTree(resp.body()).path("finance").path("result").get(0).path("quotes");
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
            futures.add(CompletableFuture.supplyAsync(() -> engine.scanPreMarketTicker(ticker)));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<String> valid = new ArrayList<>();
        for (CompletableFuture<String> f : futures) {
            String result = f.join();
            if (result != null) valid.add(result);
        }

        if (valid.isEmpty()) return "{\"error\":\"No pre-market movers found above threshold.\"}";

        valid.sort((a, b) -> Double.compare(Math.abs(engine.extractPreMarketChangePct(b)), Math.abs(engine.extractPreMarketChangePct(a))));

        List<String> top6 = valid.subList(0, Math.min(6, valid.size()));
        StringBuilder array = new StringBuilder("[");
        for (int i = 0; i < top6.size(); i++) {
            if (i > 0) array.append(",");
            array.append(top6.get(i));
        }
        array.append("]");

        return String.format("{\"status\":\"success\",\"ticker_count\":%d,\"pre_market_scan_results\":%s}",
                top6.size(), array);
    }

    // ── Wheel strategy scan ───────────────────────────────────────────────────

    /**
     * Wheel strategy scan: finds stocks/ETFs $3–$80 with IV > 30% and ≥1%/week put premium.
     * Backing method for the wheelStrategyScannerFunction @Bean.
     */
    public String scanWheelStrategy() {
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
                                .header("APCA-API-KEY-ID", alpacaClient.apiKey)
                                .header("APCA-API-SECRET-KEY", alpacaClient.apiSecret).GET().build();
                        HttpResponse<String> barsResp = alpacaClient.httpClient.send(barsReq, HttpResponse.BodyHandlers.ofString());
                        if (barsResp.statusCode() != 200) return null;
                        JsonNode bars = alpacaClient.objectMapper.readTree(barsResp.body()).path("bars").path(sym);
                        if (!bars.isArray() || bars.size() < 3) return null;
                        double price = bars.get(bars.size() - 1).path("c").asDouble(0);
                        long volume  = bars.get(bars.size() - 1).path("v").asLong(0);

                        // Filter: price $3–$80, volume > 1M
                        if (price < 3.0 || price > 80.0) return null;
                        if (volume < 1_000_000) return null;

                        // 2. Trend check — must be bullish or neutral (daily SMA10)
                        double sma10 = IndicatorUtils.calculateSmaFromBars(bars, Math.min(10, bars.size()));
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
                                .header("APCA-API-KEY-ID", alpacaClient.apiKey)
                                .header("APCA-API-SECRET-KEY", alpacaClient.apiSecret).GET().build();
                        HttpResponse<String> optResp = alpacaClient.httpClient.send(optReq, HttpResponse.BodyHandlers.ofString());

                        double iv = 0;
                        if (optResp.statusCode() == 200) {
                            JsonNode snapshots = alpacaClient.objectMapper.readTree(optResp.body()).path("snapshots");
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
                            long days = today.until(exp, ChronoUnit.DAYS);
                            double weeks = days / 7.0;
                            double premium = Math.round(putStrike * iv * Math.sqrt(days / 365.0) * 0.30 * 100.0) / 100.0;
                            double wkReturn = (premium * 100.0) / capital / weeks * 100.0;
                            if (wkReturn >= 1.0) {
                                chosenExp = exp;
                                putPremium = premium;
                                weeklyReturn_final = wkReturn;
                                long expDays = today.until(exp, ChronoUnit.DAYS);
                                expiryLabel = expDays <= 10 ? "weekly" : expDays <= 21 ? "2-week" : "monthly";
                                break;
                            }
                        }
                        if (chosenExp == null) return null; // no expiry hits 1%/week

                        long chosenDays = today.until(chosenExp, ChronoUnit.DAYS);
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

            LocalDate today2 = ZonedDateTime.now(ZoneId.of("America/New_York")).toLocalDate();
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
                    today2, sb);

        } catch (Exception e) {
            return "{\"error\":\"Wheel scan failed.\"}";
        }
    }
}
