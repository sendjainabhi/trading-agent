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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

    // ── Shared screener fetch helper ──────────────────────────────────────────

    /**
     * Fetches tickers from Yahoo Finance predefined screeners, applying common quality filters.
     * Each screener ID is tried in order until {@code targetSize} unique tickers are collected.
     * The special value {@code "trending"} hits the trending endpoint instead of the screener API.
     *
     * @param screenerIds  ordered list of Yahoo screener IDs (e.g. "most_actives", "day_gainers")
     * @param targetSize   stop collecting once this many tickers are gathered
     * @param perPage      how many results to request per screener call
     * @param minVolume    minimum daily volume; tickers below this are skipped
     */
    private List<String> fetchScreenerTickers(String[] screenerIds, int targetSize,
                                               int perPage, long minVolume) throws Exception {
        List<String> tickers = new ArrayList<>();
        for (String scrId : screenerIds) {
            if (tickers.size() >= targetSize) break;
            String url = scrId.equals("trending")
                    ? "https://query1.finance.yahoo.com/v1/finance/trending/US"
                    : "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds="
                      + scrId + "&count=" + perPage + "&fields=symbol,regularMarketPrice,regularMarketVolume";
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)").GET().build();
            HttpResponse<String> resp = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) continue;
            JsonNode quotes = alpacaClient.objectMapper.readTree(resp.body())
                    .path("finance").path("result").get(0).path("quotes");
            if (!quotes.isArray()) continue;
            for (JsonNode q : quotes) {
                if (tickers.size() >= targetSize) break;
                String sym   = q.path("symbol").asText("").trim();
                double price = q.path("regularMarketPrice").asDouble(0);
                long   vol   = q.path("regularMarketVolume").asLong(0);
                if (sym.isBlank() || sym.contains("-") || sym.contains(".")) continue;
                if (price > 0 && price < 5.0) continue;
                if (vol > 0 && vol < minVolume) continue;
                if (!tickers.contains(sym)) {
                    tickers.add(sym);
                    alpacaStreamService.subscribe(sym);
                }
            }
        }
        return tickers;
    }

    /**
     * Runs {@code engine.scanTicker()} concurrently for all tickers and returns the raw results.
     */
    private List<String> runConcurrentScan(List<String> tickers) {
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (String t : tickers) futures.add(CompletableFuture.supplyAsync(() -> engine.scanTicker(t)));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        List<String> results = new ArrayList<>();
        for (CompletableFuture<String> f : futures) {
            String r = f.join();
            if (r != null) results.add(r);
        }
        return results;
    }

    /** Formats a list of result JSON objects into a scan response envelope. */
    private static String buildScanResponse(List<String> results, String arrayKey) {
        StringBuilder array = new StringBuilder("[");
        for (int i = 0; i < results.size(); i++) { if (i > 0) array.append(","); array.append(results.get(i)); }
        array.append("]");
        return String.format("{\"status\":\"success\",\"ticker_count\":%d,\"%s\":%s}",
                results.size(), arrayKey, array);
    }

    // ── General market scan ───────────────────────────────────────────────────

    /**
     * General market scan: most-active tickers, full MTF, top 5 by absolute confluence score.
     */
    public String scanWatchlist(String tickersCsv) throws Exception {
        List<String> tickers = Arrays.stream(tickersCsv.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).distinct()
                .limit(8)   // cap at 8 — each stock uses ~4 Finnhub calls; 8×4=32 fits well within the Semaphore(55) window
                .collect(Collectors.toList());
        if (tickers.isEmpty()) return "{\"error\":\"No tickers provided.\"}";
        List<String> results = runConcurrentScan(tickers);
        results.sort((a, b) -> Double.compare(
                Math.abs(engine.extractConfluenceScore(b)),
                Math.abs(engine.extractConfluenceScore(a))));
        return buildScanResponse(results, "scan_results");
    }

    public String scanMarket() throws Exception {
        List<String> tickers = fetchScreenerTickers(
                new String[]{"most_actives", "trending"}, 8, 15, 300_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.sort((a, b) -> Double.compare(
                Math.abs(engine.extractConfluenceScore(b)),
                Math.abs(engine.extractConfluenceScore(a))));
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "scan_results");
    }

    // ── Bearish scan ──────────────────────────────────────────────────────────

    /**
     * Bearish scan: day-losers + most-actives, returns top 5 with most negative confluence score.
     */
    public String scanBearish() throws Exception {
        List<String> tickers = fetchScreenerTickers(
                new String[]{"day_losers", "most_actives"}, 8, 15, 300_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.removeIf(r -> engine.extractConfluenceScore(r) >= 0);
        results.sort((a, b) -> Double.compare(
                engine.extractConfluenceScore(a),
                engine.extractConfluenceScore(b)));
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "scan_results");
    }

    // ── Bullish scan ──────────────────────────────────────────────────────────

    /**
     * Bullish scan: day-gainers + most-actives, returns top 5 with most positive confluence score.
     */
    public String scanBullish() throws Exception {
        List<String> tickers = fetchScreenerTickers(
                new String[]{"day_gainers", "most_actives"}, 8, 15, 300_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.removeIf(r -> engine.extractConfluenceScore(r) <= 0);
        results.sort((a, b) -> Double.compare(
                engine.extractConfluenceScore(b),
                engine.extractConfluenceScore(a)));
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "scan_results");
    }

    // ── Swing scan ────────────────────────────────────────────────────────────

    /**
     * Swing scan: most-actives + gainers + losers, keeps tickers with SWING_LONG/SHORT/RANGE_PLAY signals.
     * Uses a higher min-volume threshold (500K) to ensure sufficient liquidity for swing entries.
     */
    public String scanSwing() throws Exception {
        List<String> tickers = fetchScreenerTickers(
                new String[]{"most_actives", "day_gainers", "day_losers"}, 15, 20, 500_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.removeIf(r -> "NONE".equals(engine.extractSwingSignal(r)));
        results.sort((a, b) -> {
            String sigA = engine.extractSwingSignal(a);
            String sigB = engine.extractSwingSignal(b);
            int rankA = "RANGE_PLAY".equals(sigA) ? 2 : "SWING_LONG".equals(sigA) ? 0 : 1;
            int rankB = "RANGE_PLAY".equals(sigB) ? 2 : "SWING_LONG".equals(sigB) ? 0 : 1;
            return Integer.compare(rankA, rankB);
        });
        return buildScanResponse(results.subList(0, Math.min(6, results.size())), "swing_scan_results");
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

    // ── Sector rotation scan ──────────────────────────────────────────────────

    /**
     * Sector rotation scanner: ranks all 11 SPDR sector ETFs by 1W/1M momentum,
     * relative strength vs SPY, and volume trend to identify where money is flowing.
     */
    public String scanSectorRotation() throws Exception {
        ZoneId et = ZoneId.of("America/New_York");
        String lookback = ZonedDateTime.now(et).minusDays(50)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        String[] etfs  = {"XLK","XLF","XLE","XLI","XLV","XLC","XLY","XLP","XLB","XLRE","XLU"};
        String[] names = {"Technology","Financials","Energy","Industrials","Healthcare",
                          "Communication","Consumer Discr.","Consumer Staples","Materials",
                          "Real Estate","Utilities"};
        java.util.Map<String,String> topStocks = new java.util.LinkedHashMap<>();
        topStocks.put("XLK",  "NVDA, AAPL, MSFT");
        topStocks.put("XLF",  "JPM, BAC, GS");
        topStocks.put("XLE",  "XOM, CVX, COP");
        topStocks.put("XLI",  "CAT, GE, HON");
        topStocks.put("XLV",  "LLY, UNH, JNJ");
        topStocks.put("XLC",  "GOOGL, META, NFLX");
        topStocks.put("XLY",  "AMZN, TSLA, HD");
        topStocks.put("XLP",  "WMT, PG, COST");
        topStocks.put("XLB",  "LIN, SHW, APD");
        topStocks.put("XLRE", "PLD, AMT, EQIX");
        topStocks.put("XLU",  "NEE, DUK, SO");

        // Fetch daily bars, Finnhub news sentiment, and Alpaca options — all concurrently
        java.util.Map<String, CompletableFuture<HttpResponse<String>>> futures     = new java.util.LinkedHashMap<>();
        java.util.Map<String, CompletableFuture<HttpResponse<String>>> newsFutures = new java.util.LinkedHashMap<>();
        java.util.Map<String, CompletableFuture<HttpResponse<String>>> optFutures  = new java.util.LinkedHashMap<>();

        String fhKey = engine.finnhubKey;
        for (String sym : etfs) {
            futures.put(sym, alpacaClient.httpClient.sendAsync(
                alpacaClient.buildAlpacaRequest("/bars?symbols=" + sym + "&timeframe=1Day&start=" + lookback + "&feed=iex"),
                HttpResponse.BodyHandlers.ofString()));

            // Finnhub news sentiment — free tier, one call per ETF
            if (fhKey != null && !fhKey.isBlank()) {
                newsFutures.put(sym, alpacaClient.httpClient.sendAsync(
                    HttpRequest.newBuilder()
                        .uri(URI.create("https://finnhub.io/api/v1/news-sentiment?symbol=" + sym + "&token=" + fhKey))
                        .timeout(java.time.Duration.ofSeconds(4)).GET().build(),
                    HttpResponse.BodyHandlers.ofString()));
            }

            // Alpaca options snapshots — broader strike range for put/call ratio
            optFutures.put(sym, alpacaClient.httpClient.sendAsync(
                alpacaClient.buildAlpacaBaseRequest("/v1beta1/options/snapshots/" + sym + "?feed=indicative&limit=100"),
                HttpResponse.BodyHandlers.ofString()));
        }
        CompletableFuture<HttpResponse<String>> spyFuture = alpacaClient.httpClient.sendAsync(
            alpacaClient.buildAlpacaRequest("/bars?symbols=SPY&timeframe=1Day&start=" + lookback + "&feed=iex"),
            HttpResponse.BodyHandlers.ofString());

        // Await all requests together
        java.util.List<CompletableFuture<HttpResponse<String>>> all = new ArrayList<>();
        all.addAll(futures.values());
        all.addAll(newsFutures.values());
        all.addAll(optFutures.values());
        all.add(spyFuture);
        CompletableFuture.allOf(all.toArray(new CompletableFuture[0])).join();

        // Parse SPY for relative strength baseline
        double[] spyCloses = parseSectorCloses(spyFuture.join(), "SPY");
        double spy1M = calcSectorReturn(spyCloses, 20);

        // Score each sector
        record SectorResult(String etf, String name, double price, String ret1W, String ret1M,
                            String rsVsSpy, String volTrend, String newsSentiment,
                            String pcRatioStr, double score, String signal, String stocks) {}
        List<SectorResult> results = new ArrayList<>();

        for (int i = 0; i < etfs.length; i++) {
            String etf = etfs[i];
            try {
                HttpResponse<String> resp = futures.get(etf).join();
                double[] closes  = parseSectorCloses(resp, etf);
                double[] volumes = parseSectorVolumes(resp, etf);
                if (closes.length < 6) continue;

                double price  = closes[closes.length - 1];
                double r1W    = calcSectorReturn(closes, 5);
                double r1M    = calcSectorReturn(closes, 20);
                double rs     = r1M - spy1M;
                String vTrend = calcSectorVolumeTrend(volumes);

                // News sentiment from Finnhub (bullishPercent 0–1, default neutral 0.5)
                double newsBullish = 50.0;
                String newsSentLabel = "Neutral";
                CompletableFuture<HttpResponse<String>> newsF = newsFutures.get(etf);
                if (newsF != null) {
                    double nb = parseFinnhubNewsSentiment(newsF.join());
                    if (nb >= 0) {
                        newsBullish   = nb;
                        newsSentLabel = nb >= 65 ? "Bullish" : nb <= 35 ? "Bearish" : "Neutral";
                    }
                }

                // Put/call ratio from Alpaca options (lower = bullish, higher = bearish)
                double pcRatio = parsePutCallRatio(optFutures.get(etf).join(), etf);
                String pcLabel = pcRatio <= 0 ? "N/A"
                               : pcRatio < 0.7 ? String.format("%.2f ↑", pcRatio)
                               : pcRatio > 1.3 ? String.format("%.2f ↓", pcRatio)
                               : String.format("%.2f", pcRatio);

                // Composite score: momentum + RS + volume + news sentiment + put/call signal
                double volBonus      = vTrend.equals("RISING") ? 0.5 : vTrend.equals("FALLING") ? -0.5 : 0.0;
                double newsScore     = (newsBullish - 50.0) / 50.0 * 0.4; // -0.4 to +0.4
                double pcScore       = pcRatio > 0 ? Math.min(Math.max((1.0 - pcRatio) * 0.5, -0.5), 0.5) : 0.0;
                double score         = (r1W * 0.5) + (r1M * 0.3) + (rs * 0.15) + volBonus + newsScore + pcScore;

                String signal;
                if      (score > 3.0)  signal = "STRONG_INFLOW";
                else if (score > 0.8)  signal = "INFLOW";
                else if (score > -0.8) signal = "NEUTRAL";
                else if (score > -3.0) signal = "OUTFLOW";
                else                   signal = "STRONG_OUTFLOW";

                results.add(new SectorResult(
                    etf, names[i],
                    Math.round(price * 100.0) / 100.0,
                    String.format("%+.1f%%", r1W),
                    String.format("%+.1f%%", r1M),
                    String.format("%+.1f%%", rs),
                    vTrend, newsSentLabel, pcLabel,
                    score, signal,
                    topStocks.getOrDefault(etf, "")
                ));
            } catch (Exception ignored) {}
        }

        if (results.isEmpty()) return "{\"error\":\"Sector data unavailable.\"}";

        results.sort((a, b) -> Double.compare(b.score(), a.score()));

        // Market regime from SPY momentum
        String regime = spy1M > 4 ? "Risk-On (Bull)" : spy1M < -4 ? "Risk-Off (Bear)" : "Neutral / Rotating";
        String scanTime = ZonedDateTime.now(et).format(DateTimeFormatter.ofPattern("hh:mm:ss a z"));

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < results.size(); i++) {
            SectorResult s = results.get(i);
            if (i > 0) sb.append(",");
            sb.append(String.format(
                "{\"etf\":\"%s\",\"sector_name\":\"%s\",\"current_price\":%.2f," +
                "\"week_return\":\"%s\",\"month_return\":\"%s\",\"rs_vs_spy\":\"%s\"," +
                "\"volume_trend\":\"%s\",\"news_sentiment\":\"%s\",\"put_call_ratio\":\"%s\"," +
                "\"rotation_score\":%.2f,\"signal\":\"%s\",\"top_stocks\":\"%s\"}",
                s.etf(), s.name(), s.price(), s.ret1W(), s.ret1M(),
                s.rsVsSpy(), s.volTrend(), s.newsSentiment(), s.pcRatioStr(),
                s.score(), s.signal(), s.stocks()));
        }
        sb.append("]");

        return String.format(
            "{\"status\":\"success\",\"market_regime\":\"%s\",\"spy_1m_return\":\"%+.1f%%\"," +
            "\"scan_time\":\"%s\",\"sector_rotation_results\":%s}",
            regime, spy1M, scanTime, sb);
    }

    // ── Sector rotation helpers ───────────────────────────────────────────────

    private double[] parseSectorCloses(HttpResponse<String> resp, String sym) {
        try {
            if (resp.statusCode() != 200) return new double[0];
            JsonNode bars = alpacaClient.objectMapper.readTree(resp.body()).path("bars").path(sym);
            if (!bars.isArray() || bars.isEmpty()) return new double[0];
            double[] closes = new double[bars.size()];
            for (int i = 0; i < bars.size(); i++) closes[i] = bars.get(i).path("c").asDouble(0);
            return closes;
        } catch (Exception e) { return new double[0]; }
    }

    private double[] parseSectorVolumes(HttpResponse<String> resp, String sym) {
        try {
            if (resp.statusCode() != 200) return new double[0];
            JsonNode bars = alpacaClient.objectMapper.readTree(resp.body()).path("bars").path(sym);
            if (!bars.isArray() || bars.isEmpty()) return new double[0];
            double[] vols = new double[bars.size()];
            for (int i = 0; i < bars.size(); i++) vols[i] = bars.get(i).path("v").asDouble(0);
            return vols;
        } catch (Exception e) { return new double[0]; }
    }

    private double calcSectorReturn(double[] closes, int periods) {
        if (closes.length < periods + 1) return 0.0;
        double now  = closes[closes.length - 1];
        double prev = closes[closes.length - 1 - periods];
        return prev > 0 ? ((now - prev) / prev) * 100.0 : 0.0;
    }

    private String calcSectorVolumeTrend(double[] volumes) {
        if (volumes.length < 10) return "FLAT";
        double recent = 0, prior = 0;
        int recentN = Math.min(5, volumes.length);
        int priorN  = Math.min(15, volumes.length - recentN);
        for (int i = volumes.length - recentN; i < volumes.length; i++) recent += volumes[i];
        for (int i = volumes.length - recentN - priorN; i < volumes.length - recentN; i++) prior += volumes[i];
        recent /= recentN;
        prior  /= priorN;
        if (prior <= 0) return "FLAT";
        double ratio = recent / prior;
        return ratio > 1.10 ? "RISING" : ratio < 0.90 ? "FALLING" : "FLAT";
    }

    /**
     * Parses Finnhub /news-sentiment response.
     * Returns bullish percent as 0–100, or -1 if unavailable.
     */
    private double parseFinnhubNewsSentiment(HttpResponse<String> resp) {
        try {
            if (resp.statusCode() != 200) return -1;
            JsonNode root = alpacaClient.objectMapper.readTree(resp.body());
            JsonNode sent = root.path("sentiment");
            if (sent.isMissingNode()) return -1;
            double bull = sent.path("bullishPercent").asDouble(-1);
            return bull < 0 ? -1 : bull * 100.0;
        } catch (Exception e) { return -1; }
    }

    /**
     * Parses Alpaca options snapshots to compute a put/call volume ratio.
     * Returns the ratio (puts/calls), or 0 if options data is unavailable.
     * Ratio < 0.7 = bullish (call-heavy); ratio > 1.3 = bearish (put-heavy).
     */
    private double parsePutCallRatio(HttpResponse<String> resp, String underlying) {
        try {
            if (resp.statusCode() != 200) return 0;
            JsonNode snapshots = alpacaClient.objectMapper.readTree(resp.body()).path("snapshots");
            if (snapshots.isMissingNode() || snapshots.isEmpty()) return 0;
            double callVol = 0, putVol = 0;
            // Option symbol format: UNDERLYING + YYMMDD + C/P + PRICE_PADDED
            // Find C or P by matching the first occurrence after the underlying prefix + 6-digit date
            java.util.regex.Pattern cpPattern = java.util.regex.Pattern.compile(
                "^" + java.util.regex.Pattern.quote(underlying) + "\\d{6}([CP])");
            Iterator<java.util.Map.Entry<String, JsonNode>> it = snapshots.fields();
            while (it.hasNext()) {
                java.util.Map.Entry<String, JsonNode> entry = it.next();
                java.util.regex.Matcher m = cpPattern.matcher(entry.getKey());
                if (!m.find()) continue;
                double vol = entry.getValue().path("day").path("volume").asDouble(0);
                if ("C".equals(m.group(1))) callVol += vol;
                else                         putVol  += vol;
            }
            return callVol > 0 ? putVol / callVol : 0;
        } catch (Exception e) { return 0; }
    }

    // ── Squeeze setup scanner ─────────────────────────────────────────────────

    /**
     * Squeeze scanner: stocks with ADX < 22 — low trend strength signals price is coiling.
     * Scans a broader universe (actives + a curated mid-cap list) so results appear even
     * during trending markets. Lower ADX = tighter coil = higher priority.
     */
    public String scanSqueeze() throws Exception {
        // Curated liquid names that frequently form squeeze setups
        List<String> universe = new ArrayList<>(List.of(
                "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","NFLX","INTC",
                "AVGO","MU","QCOM","COIN","PLTR","UBER","SOFI","ARM","MSTR","HOOD",
                "F","GE","DIS","SMCI","RIVN","BAC","JPM","XOM","CVX","SBUX"
        ));
        List<String> screenerTickers = fetchScreenerTickers(
                new String[]{"most_actives", "day_gainers", "day_losers"}, 12, 20, 300_000L);
        for (String t : screenerTickers) if (!universe.contains(t)) universe.add(t);

        List<String> results = runConcurrentScan(universe);
        // Primary filter: ADX < 22 (flat/choppy = coiling)
        results.removeIf(r -> engine.extractAdxValue(r) >= 22.0);
        // Sort: lowest ADX first (tightest squeeze), then by iv_rank ascending (cheapest options)
        results.sort((a, b) -> {
            int adxCmp = Double.compare(engine.extractAdxValue(a), engine.extractAdxValue(b));
            if (adxCmp != 0) return adxCmp;
            return Double.compare(engine.extractIvRank(a), engine.extractIvRank(b));
        });
        if (results.isEmpty()) return "{\"status\":\"success\",\"ticker_count\":0,\"squeeze_scan_results\":[]}";
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "squeeze_scan_results");
    }

    // ── Earnings plays scanner ────────────────────────────────────────────────

    /**
     * Earnings plays scanner: stocks 2–5 days before earnings with elevated IV rank (>50)
     * and unusual options activity — prime candidates for pre-earnings volatility plays.
     */
    public String scanEarningsPlays() throws Exception {
        List<String> candidates = new ArrayList<>(List.of(
                "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","NFLX","INTC",
                "JPM","BAC","GS","MS","WFC","XOM","CVX","UNH","LLY","PFE",
                "AVGO","MU","QCOM","CRM","NOW","PLTR","UBER","COIN","MSTR","ARM"
        ));
        List<String> screenerTickers = fetchScreenerTickers(
                new String[]{"most_actives", "day_gainers"}, 10, 20, 300_000L);
        for (String t : screenerTickers) if (!candidates.contains(t)) candidates.add(t);

        List<String> results = runConcurrentScan(candidates);
        results.removeIf(r -> {
            if (!engine.extractEarningsFlag(r)) return true;
            int days = engine.extractEarningsDaysAway(r);
            if (days < 1 || days > 7) return true;
            return engine.extractIvRank(r) < 40.0;
        });
        results.sort((a, b) -> {
            int daysA = engine.extractEarningsDaysAway(a);
            int daysB = engine.extractEarningsDaysAway(b);
            if (daysA != daysB) return Integer.compare(daysA, daysB);
            return Double.compare(engine.extractIvRank(b), engine.extractIvRank(a));
        });
        if (results.isEmpty()) return "{\"status\":\"success\",\"ticker_count\":0,\"earnings_scan_results\":[]}";
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "earnings_scan_results");
    }

    // ── Failed breakdown scanner ──────────────────────────────────────────────

    /**
     * Failed breakdown scanner: stocks with SWING_LONG signal (near swing support)
     * AND bullish RSI divergence — price tested a low but momentum didn't follow through,
     * signaling a potential reversal snap-back.
     */
    public String scanFailedBreakdown() throws Exception {
        List<String> tickers = fetchScreenerTickers(
                new String[]{"day_losers", "most_actives", "day_gainers"}, 18, 20, 300_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.removeIf(r -> {
            boolean isSwingLong = "SWING_LONG".equals(engine.extractSwingSignal(r));
            boolean hasBullDiv  = "BULLISH_DIV".equals(engine.extractRsiDivergence(r));
            return !(isSwingLong && hasBullDiv);
        });
        results.sort((a, b) -> Double.compare(
                engine.extractConfluenceScore(b), engine.extractConfluenceScore(a)));
        if (results.isEmpty()) return "{\"status\":\"success\",\"ticker_count\":0,\"failed_breakdown_results\":[]}";
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "failed_breakdown_results");
    }
}
