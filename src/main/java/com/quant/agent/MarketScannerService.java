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

    // ── Sector ETF drill-down — when a sector ETF moves >1.5%, add its top stocks ─

    private static final Map<String, List<String>> SECTOR_ETF_HOLDINGS = Map.ofEntries(
        Map.entry("XLK",  List.of("NVDA","AAPL","MSFT","META","GOOGL","AVGO","AMD","QCOM","ORCL","CRM")),
        Map.entry("XLF",  List.of("JPM","BAC","GS","WFC","MS","C","V","MA","BLK","SCHW")),
        Map.entry("XLE",  List.of("XOM","CVX","COP","SLB","OXY","PSX","VLO","MPC","EOG","HAL")),
        Map.entry("XLI",  List.of("CAT","GE","HON","UNP","RTX","LMT","DE","NOC","EMR","FDX")),
        Map.entry("XLV",  List.of("LLY","UNH","JNJ","MRK","ABBV","TMO","ABT","DHR","AMGN","BMY")),
        Map.entry("XLY",  List.of("AMZN","TSLA","HD","NKE","MCD","SBUX","TJX","GM","F","BKNG")),
        Map.entry("XLP",  List.of("WMT","PG","COST","KO","PEP","MDLZ","CL","GIS","K","HRL")),
        Map.entry("XLRE", List.of("PLD","AMT","EQIX","SPG","CCI","PSA","O","WELL","DLR","IRM")),
        Map.entry("XLU",  List.of("NEE","DUK","SO","D","AEP","EXC","SRE","PCG","ED","XEL")),
        Map.entry("CIBR", List.of("CRWD","PANW","FTNT","ZS","S","OKTA","CYBR","TENB","CRDO","NET")),
        Map.entry("XBI",  List.of("MRNA","BNTX","REGN","VRTX","GILD","BIIB","EXAS","IONS","PCVX","INCY"))
    );

    private List<String> fetchSectorExpansion() {
        java.util.concurrent.CopyOnWriteArrayList<String> found = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : SECTOR_ETF_HOLDINGS.entrySet()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(engine.yahooBaseUrl + "/v8/finance/chart/"
                            + entry.getKey() + "?interval=1d&range=2d"))
                        .header("User-Agent", "Mozilla/5.0")
                        .timeout(java.time.Duration.ofSeconds(5)).GET().build();
                    HttpResponse<String> resp = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                    if (resp.statusCode() == 200) {
                        JsonNode meta = alpacaClient.objectMapper.readTree(resp.body())
                            .path("chart").path("result").get(0).path("meta");
                        double price = meta.path("regularMarketPrice").asDouble(0);
                        double prev  = meta.path("chartPreviousClose").asDouble(0);
                        if (price > 0 && prev > 0 && Math.abs((price - prev) / prev * 100.0) >= 1.5) {
                            found.addAll(entry.getValue());
                        }
                    }
                } catch (Exception ignored) {}
            }));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        List<String> expansion = new ArrayList<>();
        for (String sym : found) {
            if (!expansion.contains(sym)) {
                expansion.add(sym);
                alpacaStreamService.subscribe(sym);
            }
        }
        return expansion;
    }

    /** Composite rank: |confluence| + volume bonus + breakout bonus — always positive, works for both bull/bear scans */
    private double compositeRankScore(String result) {
        double confluence  = engine.extractConfluenceScore(result);
        double volRatio    = engine.extractVolumeRatio(result);
        String breakout    = engine.extractBreakoutType(result);
        double volBonus    = volRatio >= 2.0 ? 10.0 : volRatio >= 1.5 ? 5.0 : 0.0;
        double brkBonus    = "FRESH_CROSS".equals(breakout) ? 20.0
                           : "ABOVE_EMA50".equals(breakout) ? 15.0
                           : "RANGE_BREAK".equals(breakout) ? 10.0 : 0.0;
        return Math.abs(confluence) + volBonus + brkBonus;
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

        // ── Yahoo Finance screener ─────────────────────────────────────────────
        for (String scrId : screenerIds) {
            if (tickers.size() >= targetSize) break;
            try {
                String url = scrId.equals("trending")
                        ? engine.yahooBaseUrl + "/v1/finance/trending/US"
                        : engine.yahooBaseUrl + "/v1/finance/screener/predefined/saved?scrIds="
                          + scrId + "&count=" + perPage + "&fields=symbol,regularMarketPrice,regularMarketVolume";
                HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)").GET().build();
                HttpResponse<String> resp = alpacaClient.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    JsonNode quotes = alpacaClient.objectMapper.readTree(resp.body())
                            .path("finance").path("result").get(0).path("quotes");
                    if (quotes.isArray()) {
                        for (JsonNode q : quotes) {
                            if (tickers.size() >= targetSize) break;
                            String sym   = q.path("symbol").asText("").trim();
                            double price = q.path("regularMarketPrice").asDouble(0);
                            long   vol   = q.path("regularMarketVolume").asLong(0);
                            if (sym.isBlank() || sym.contains("-") || sym.contains(".")) continue;
                            if (price > 0 && price < 5.0) continue;
                            if (vol > 0 && vol < minVolume) continue;
                            if (!tickers.contains(sym)) { tickers.add(sym); alpacaStreamService.subscribe(sym); }
                        }
                    }
                }
            } catch (Exception ignored) {}
        }
        // Sector ETF drill-down: when a sector moves >1.5% add its top holdings
        try {
            List<String> sectorStocks = fetchSectorExpansion();
            for (String sym : sectorStocks) {
                if (!tickers.contains(sym)) { tickers.add(sym); }
            }
        } catch (Exception ignored) {}
        return tickers;
    }

    /**
     * Runs {@code engine.scanTicker()} concurrently for all tickers and returns the raw results.
     * Each ticker is isolated — one failure does not cancel the rest.
     */
    private List<String> runConcurrentScan(List<String> tickers) {
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (String t : tickers) futures.add(CompletableFuture.supplyAsync(() -> engine.scanTicker(t)));
        List<String> results = new ArrayList<>();
        for (CompletableFuture<String> f : futures) {
            try {
                String r = f.join();
                if (r != null) results.add(r);
            } catch (Exception ignored) {}
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
                new String[]{"most_actives", "trending"}, 8, 15, 1_000_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.sort((a, b) -> Double.compare(compositeRankScore(b), compositeRankScore(a)));
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "scan_results");
    }

    // ── Bearish scan ──────────────────────────────────────────────────────────

    /**
     * Bearish scan: day-losers + most-actives, returns top 5 with most negative confluence score.
     */
    public String scanBearish() throws Exception {
        List<String> tickers = fetchScreenerTickers(
                new String[]{"day_losers", "most_actives"}, 8, 15, 1_000_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.removeIf(r -> engine.extractConfluenceScore(r) >= 0);
        results.sort((a, b) -> Double.compare(compositeRankScore(a), compositeRankScore(b)));
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "scan_results");
    }

    // ── Bullish scan ──────────────────────────────────────────────────────────

    /**
     * Bullish scan: day-gainers + most-actives, returns top 5 with most positive confluence score.
     */
    public String scanBullish() throws Exception {
        List<String> tickers = fetchScreenerTickers(
                new String[]{"day_gainers", "most_actives"}, 8, 15, 1_000_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.removeIf(r -> engine.extractConfluenceScore(r) <= 0);
        results.sort((a, b) -> Double.compare(compositeRankScore(b), compositeRankScore(a)));
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "scan_results");
    }

    // ── Swing scan ────────────────────────────────────────────────────────────

    /**
     * Swing scan: most-actives + gainers + losers, keeps tickers with SWING_LONG/SHORT/RANGE_PLAY signals.
     * Uses a min-volume threshold of 1M to ensure sufficient liquidity for swing entries.
     */
    public String scanSwing() throws Exception {
        List<String> tickers = fetchScreenerTickers(
                new String[]{"most_actives", "day_gainers", "day_losers"}, 15, 20, 1_000_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.removeIf(r -> "NONE".equals(engine.extractSwingSignal(r)));
        results.sort((a, b) -> {
            String sigA = engine.extractSwingSignal(a);
            String sigB = engine.extractSwingSignal(b);
            int rankA = "RANGE_PLAY".equals(sigA) ? 2 : "SWING_LONG".equals(sigA) ? 0 : 1;
            int rankB = "RANGE_PLAY".equals(sigB) ? 2 : "SWING_LONG".equals(sigB) ? 0 : 1;
            if (rankA != rankB) return Integer.compare(rankA, rankB);
            return Double.compare(compositeRankScore(b), compositeRankScore(a));
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
                // Cybersecurity
                "CRWD", "PANW", "FTNT", "ZS", "S", "OKTA", "NET",
                // Semis
                "AMAT", "KLAC", "LRCX", "ON", "MRVL", "QCOM",
                "TSLL", "NVDL", "AAPU", "METU", "AMZU", "MSFU", "CONL", "MSTU",
                "TQQQ", "SPXL", "SOXL", "LABU", "FNGU"
        ));
        // Append Yahoo most-actives to catch any fresh names not in the static list
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(engine.yahooBaseUrl + "/v1/finance/screener/predefined/saved?scrIds=most_actives&count=10&fields=symbol,regularMarketPrice,regularMarketVolume"))
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

    // ── Stock → 2x/3x ETF companion map (for stocks typically >$80) ──────────
    private static final Map<String, String> STOCK_TO_2X_ETF = Map.ofEntries(
        Map.entry("AAPL",  "AAPU"),   // 2× AAPL (GraniteShares)
        Map.entry("MSFT",  "MSFU"),   // 2× MSFT
        Map.entry("NVDA",  "NVDL"),   // 2× NVDA
        Map.entry("AMZN",  "AMZU"),   // 2× AMZN
        Map.entry("META",  "METU"),   // 2× META
        Map.entry("TSLA",  "TSLL"),   // 2× TSLA
        Map.entry("GOOGL", "GGLL"),   // 2× Alphabet
        Map.entry("AVGO",  "SOXL"),   // SOX semis proxy
        Map.entry("QCOM",  "SOXL"),
        Map.entry("JPM",   "FAS"),    // 3× financials
        Map.entry("GS",    "FAS"),
        Map.entry("V",     "FAS"),
        Map.entry("MA",    "FAS"),
        Map.entry("HD",    "QLD"),    // 2× QQQ proxy for mega-caps
        Map.entry("COST",  "QLD"),
        Map.entry("CRM",   "QLD"),
        Map.entry("ORCL",  "QLD"),
        Map.entry("UNH",   "SSO")     // 2× SPY for healthcare/defensive
    );

    private static final java.util.Set<String> ETF_TICKERS = java.util.Set.of(
        "TSLL","NVDL","AAPU","METU","AMZU","MSFU","CONL","MSTU","GGLL",
        "TQQQ","SPXL","SOXL","QLD","SSO","TECL","FAS","UYG","ERX","LABU","FNGU","DPST"
    );

    public String scanWheelStrategy() {
        try {
            // ── Curated high-conviction universe (~80 tickers) ───────────────
            List<String> universe = List.of(
                // Quality stocks — typically $3–$80 (primary candidates)
                "PLTR","SOFI","SNAP","RBLX","NIO","RIVN","LCID","HOOD","COIN","AFRM",
                "UPST","OPEN","IONQ","SOUN","INTC","MU","SMCI","F","GM","UBER",
                "LYFT","SQ","PYPL","BAC","C","WFC","KEY","T","VZ","AAL",
                "DAL","UAL","CCL","RCL","GE","X","CLF","AA","MARA","RIOT",
                "DIS","AMD","NFLX","ARM","MSTR","BBAI","QBTS","RGTI","XOM","PARA",
                // Large-caps often >$80 — shown de-prioritised, ETF companion generated
                "AAPL","MSFT","NVDA","AMZN","META","TSLA","GOOGL",
                "JPM","GS","V","MA","HD","COST","AVGO","QCOM","CRM","ORCL","UNH",
                // 2×/3× leveraged ETFs — always in $3–$80 range
                "TSLL","NVDL","AAPU","METU","AMZU","MSFU","CONL","GGLL",
                "TQQQ","SPXL","SOXL","QLD","SSO","TECL","FAS","LABU","FNGU"
            );

            ZoneId   et    = ZoneId.of("America/New_York");
            ZonedDateTime nowET = ZonedDateTime.now(et);
            LocalDate today = nowET.toLocalDate();
            boolean   isWeekday = today.getDayOfWeek().getValue() <= 5;
            String startDate = nowET.minusDays(90)
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String endDate   = nowET.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            // ── Expanded record carrying all new fields ───────────────────────
            record WheelCandidate(
                String  ticker,
                double  price,
                double  iv,               // annualised, 0–1 scale
                double  putStrike,
                double  putPremium,       // per share
                double  weeklyReturnPct,
                String  expiryDate,
                long    volume,
                double  callStrikeVal,
                double  callPremium,
                boolean isEtf,
                double  percentChange,
                boolean isRedDay,         // true when percentChange < -0.5 on a weekday
                boolean bearishWeekly,    // 5-bar slope negative
                double  delta,
                double  capitalIfAssigned,
                double  takeProfitAt,     // premium × 0.10 (90% capture target)
                int     earningsDaysAway,
                boolean earningsThisWeek, // earningsDaysAway ≤ 7
                double  low20d,           // 20-day low used as swing support proxy
                boolean nearSupport,
                int     analystBuyPct,
                double  insiderMspr,
                String  etfAlt,           // companion 2x ETF ticker, "" if N/A
                boolean isCompanion,      // true = this row is an ETF companion
                String  parentTicker      // set for companion rows
            ) {}

            // ── Phase 1: Alpaca-only fast filter (parallel) ──────────────────
            List<CompletableFuture<WheelCandidate>> futures = new ArrayList<>();
            for (String ticker : universe) {
                final String sym = ticker;
                futures.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        // Bars — 65 days to support EMA50 + 60d high calc
                        HttpRequest barsReq = alpacaClient.buildAlpacaRequest(
                            "/bars?symbols=" + sym + "&timeframe=1Day&start=" + startDate
                            + "&end=" + endDate + "&limit=65&feed=iex");
                        HttpResponse<String> barsResp = alpacaClient.httpClient.send(
                            barsReq, HttpResponse.BodyHandlers.ofString());
                        if (barsResp.statusCode() != 200) return null;
                        JsonNode bars = alpacaClient.objectMapper.readTree(barsResp.body())
                            .path("bars").path(sym);
                        if (!bars.isArray() || bars.size() < 10) return null;

                        double price  = bars.get(bars.size() - 1).path("c").asDouble(0);
                        long   volume = bars.get(bars.size() - 1).path("v").asLong(0);
                        if (price <= 0 || volume < 1_000_000) return null;

                        // Percent change (day over day)
                        double prevClose     = bars.get(bars.size() - 2).path("c").asDouble(price);
                        double percentChange = prevClose > 0 ? ((price - prevClose) / prevClose) * 100.0 : 0.0;
                        boolean isRedDay     = isWeekday && percentChange < -0.5;

                        // EMA50 — long-term trend check
                        double ema50 = IndicatorUtils.calculateEmaFromBars(bars, Math.min(50, bars.size()));
                        // Skip fallen knives: price >20% below EMA50 AND recent slope negative
                        double weekSlope = bars.size() >= 6
                            ? bars.get(bars.size() - 1).path("c").asDouble()
                              - bars.get(bars.size() - 6).path("c").asDouble()
                            : 0;
                        boolean bearishWeekly = weekSlope < 0;
                        if (ema50 > 0 && price < ema50 * 0.80 && bearishWeekly) return null;

                        // 60-day high — fallen knife hard block (>20% below AND weekly down)
                        double high60d = 0;
                        for (JsonNode bar : bars) {
                            double h = bar.path("h").asDouble(0);
                            if (h > high60d) high60d = h;
                        }
                        if (high60d > 0 && price < high60d * 0.80 && bearishWeekly) return null;

                        // 20-day low as swing support proxy
                        double low20d = Double.MAX_VALUE;
                        int lb20 = Math.min(20, bars.size());
                        for (int i = bars.size() - lb20; i < bars.size(); i++) {
                            double l = bars.get(i).path("l").asDouble(Double.MAX_VALUE);
                            if (l > 0) low20d = Math.min(low20d, l);
                        }
                        if (low20d == Double.MAX_VALUE) low20d = 0;
                        boolean nearSupport = low20d > 0 && price >= low20d && price <= low20d * 1.06;

                        // Options snapshot — derive IV
                        HttpRequest optReq = alpacaClient.buildAlpacaBaseRequest(
                            "/v1beta1/options/snapshots/" + sym
                            + "?feed=indicative&strike_price_gte=" + String.format("%.2f", price * 0.90)
                            + "&strike_price_lte=" + String.format("%.2f", price * 1.10) + "&limit=30");
                        HttpResponse<String> optResp = alpacaClient.httpClient.send(
                            optReq, HttpResponse.BodyHandlers.ofString());
                        double iv = 0;
                        if (optResp.statusCode() == 200) {
                            JsonNode snaps = alpacaClient.objectMapper.readTree(optResp.body()).path("snapshots");
                            Iterator<Map.Entry<String, JsonNode>> it = snaps.fields();
                            double ivSum = 0; int ivCnt = 0;
                            while (it.hasNext()) {
                                JsonNode snap = it.next().getValue();
                                double ap = snap.path("latestQuote").path("ap").asDouble(0);
                                double bp = snap.path("latestQuote").path("bp").asDouble(0);
                                if (ap <= 0 || bp <= 0) continue;
                                double raw = (price > 0) ? ((ap + bp) / 2.0 / price) * Math.sqrt(52.0) : 0;
                                if (raw > 0.10 && raw < 5.0) { ivSum += raw; ivCnt++; }
                            }
                            if (ivCnt > 0) iv = ivSum / ivCnt;
                        }
                        // Fallback: estimate IV from 20-day historical volatility when options data is thin
                        if (iv < 0.10 && bars.size() >= 22) {
                            double[] logReturns = new double[20];
                            for (int i = bars.size() - 20; i < bars.size(); i++) {
                                double c0 = bars.get(i - 1).path("c").asDouble(0);
                                double c1 = bars.get(i).path("c").asDouble(0);
                                logReturns[i - (bars.size() - 20)] = (c0 > 0 && c1 > 0) ? Math.log(c1 / c0) : 0;
                            }
                            double mean = 0;
                            for (double r : logReturns) mean += r;
                            mean /= logReturns.length;
                            double variance = 0;
                            for (double r : logReturns) variance += (r - mean) * (r - mean);
                            iv = Math.sqrt((variance / logReturns.length) * 252.0);
                        }
                        // IV gate: 0.18–1.50 (18%–150% annualised). HV-derived IV allowed.
                        if (iv < 0.18 || iv > 1.50) return null;

                        // Expected-move lower bound for the week: price - price×IV×√(7/365)
                        double expectedMoveLower = price - price * iv * Math.sqrt(7.0 / 365.0);

                        // Put strike: nearest $0.50 step just below expected move (with 0.5% buffer)
                        double rawStrike = expectedMoveLower * 0.995;
                        double putStrike = Math.round(rawStrike * 2.0) / 2.0;
                        double capital   = putStrike * 100.0;

                        // Expiry ladder: weekly → biweekly → monthly, pick first that yields ≥1%/wk
                        LocalDate wkExp = today.plusDays(7);
                        while (wkExp.getDayOfWeek().getValue() != 5) wkExp = wkExp.plusDays(1);
                        LocalDate bwExp = today.plusDays(14);
                        while (bwExp.getDayOfWeek().getValue() != 5) bwExp = bwExp.plusDays(1);
                        LocalDate moExp = today.plusDays(28);
                        while (moExp.getDayOfWeek().getValue() != 5) moExp = moExp.plusDays(1);

                        // Always default to weekly expiry. Only fall back to biweekly/monthly
                        // if weekly premium is literally zero (no liquidity), not just under 1%/wk.
                        long   wkDays   = today.until(wkExp, ChronoUnit.DAYS);
                        double wkPrem   = Math.round(putStrike * iv * Math.sqrt(wkDays / 365.0) * 0.30 * 100.0) / 100.0;
                        double wkReturn = capital > 0 ? (wkPrem * 100.0) / capital / (wkDays / 7.0) * 100.0 : 0;

                        LocalDate chosenExp;
                        double putPremium;
                        double weeklyReturn;
                        String expiryLabel;

                        if (wkPrem > 0) {
                            // Use weekly regardless of whether it clears 1%/wk
                            chosenExp    = wkExp;
                            putPremium   = wkPrem;
                            weeklyReturn = wkReturn;
                            expiryLabel  = "weekly";
                        } else {
                            // Weekly has no premium — try biweekly then monthly as fallback
                            chosenExp    = null;
                            putPremium   = 0;
                            weeklyReturn = 0;
                            expiryLabel  = "weekly";
                            for (LocalDate exp : List.of(bwExp, moExp)) {
                                long   daysOut = today.until(exp, ChronoUnit.DAYS);
                                double wks     = daysOut / 7.0;
                                double prem    = Math.round(putStrike * iv * Math.sqrt(daysOut / 365.0) * 0.30 * 100.0) / 100.0;
                                double wkRet   = capital > 0 ? (prem * 100.0) / capital / wks * 100.0 : 0;
                                if (prem > 0) {
                                    chosenExp    = exp;
                                    putPremium   = prem;
                                    weeklyReturn = wkRet;
                                    expiryLabel  = daysOut <= 21 ? "2-week" : "monthly";
                                    break;
                                }
                            }
                        }
                        // Require at least 0.75%/wk (flexible floor — good stocks near support may yield slightly less)
                        if (chosenExp == null || weeklyReturn < 0.75) return null;

                        long   chosenDays  = today.until(chosenExp, ChronoUnit.DAYS);
                        double totalReturn = weeklyReturn * (chosenDays / 7.0);
                        String expDisplay  = chosenExp.format(DateTimeFormatter.ofPattern("MMM dd, yyyy"))
                            + " (" + expiryLabel
                            + ("weekly".equals(expiryLabel) ? "" : " · total ~" + String.format("%.1f", totalReturn) + "%")
                            + ")";

                        // Delta (Black-Scholes N(-d1) approximation)
                        double delta = StockAnalysisEngine.putDelta(price, putStrike, iv, (int) chosenDays);

                        // Covered call: strike 5% above price (always show regardless of earnings)
                        double callStrikeVal = Math.round(price * 1.05 * 2.0) / 2.0;
                        double callPremium   = Math.round(putPremium * 0.6 * 100.0) / 100.0;

                        double capitalIfAssigned = putStrike * 100.0;
                        double takeProfitAt      = Math.round(putPremium * 0.10 * 100.0) / 100.0;

                        boolean isEtf = ETF_TICKERS.contains(sym);
                        String  etfAlt = (!isEtf && price > 80.0) ? STOCK_TO_2X_ETF.getOrDefault(sym, "") : "";

                        // Phase 1 survivor — Finnhub quality check happens in Phase 2 below
                        return new WheelCandidate(
                            sym, price, iv, putStrike, putPremium, weeklyReturn, expDisplay, volume,
                            callStrikeVal, callPremium, isEtf,
                            percentChange, isRedDay, bearishWeekly,
                            delta, capitalIfAssigned, takeProfitAt,
                            999, false,   // earningsDaysAway + earningsThisWeek — filled in Phase 2
                            low20d, nearSupport,
                            50, 0.0,      // analystBuyPct + insiderMspr — filled in Phase 2
                            etfAlt, false, ""
                        );
                    } catch (Exception e) {
                        return null;
                    }
                }));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            List<WheelCandidate> phase1 = new ArrayList<>();
            for (var f : futures) {
                WheelCandidate c = f.join();
                if (c != null) phase1.add(c);
            }

            // ── Phase 2: Finnhub quality gate (sequential, rate-limited) ─────
            List<WheelCandidate> qualified = new ArrayList<>();
            for (WheelCandidate c : phase1) {
                StockAnalysisEngine.WheelQuality q = engine.fetchWheelQuality(c.ticker());
                boolean qualityPass = q.analystBuyPct() >= 45 || q.insiderMspr() > 5;
                if (!qualityPass && !c.isEtf()) continue; // ETFs bypass quality check
                boolean earningsThisWeek = q.earningsDaysAway() <= 7;
                // Earnings this week → skip NEW put sell but still include for CC display
                // We keep the candidate and flag it; template shows CC row, not put row
                qualified.add(new WheelCandidate(
                    c.ticker(), c.price(), c.iv(), c.putStrike(), c.putPremium(),
                    c.weeklyReturnPct(), c.expiryDate(), c.volume(),
                    c.callStrikeVal(), c.callPremium(), c.isEtf(),
                    c.percentChange(), c.isRedDay(), c.bearishWeekly(),
                    c.delta(), c.capitalIfAssigned(), c.takeProfitAt(),
                    q.earningsDaysAway(), earningsThisWeek,
                    c.low20d(), c.nearSupport(),
                    q.analystBuyPct(), q.insiderMspr(),
                    c.etfAlt(), false, ""
                ));
            }

            if (qualified.isEmpty()) return "{\"error\":\"No wheel candidates found matching criteria.\"}";

            // ── Phase 3: Sort ─────────────────────────────────────────────────
            // Primary: sub-$80 stocks (and ETFs) always before >$80 stocks
            // Secondary within group: RED_DAY > BEARISH_BIAS > STANDARD
            // Tertiary within same priority: most negative percent_change first (biggest red day)
            qualified.sort((a, b) -> {
                boolean aUnder = a.price() <= 80 || a.isEtf();
                boolean bUnder = b.price() <= 80 || b.isEtf();
                if (aUnder != bUnder) return aUnder ? -1 : 1; // sub-$80 always first

                int priA = a.isRedDay() ? 2 : a.bearishWeekly() ? 1 : 0;
                int priB = b.isRedDay() ? 2 : b.bearishWeekly() ? 1 : 0;
                if (priA != priB) return Integer.compare(priB, priA); // higher priority first

                // Same priority: most negative day change first (best entry point)
                return Double.compare(a.percentChange(), b.percentChange());
            });

            // ── Phase 4: Build output rows — insert ETF companion right after parent ─
            // Build a lookup of ETF companions already in qualified
            java.util.Map<String, WheelCandidate> etfByTicker = new java.util.LinkedHashMap<>();
            for (WheelCandidate c : qualified) {
                if (c.isEtf()) etfByTicker.put(c.ticker(), c);
            }
            java.util.Set<String> insertedEtfs = new java.util.HashSet<>();
            List<WheelCandidate> outputRows = new ArrayList<>();
            for (WheelCandidate c : qualified) {
                if (c.isEtf() && insertedEtfs.contains(c.ticker())) continue; // already placed as companion
                outputRows.add(c);
                // If this is a >$80 stock with a known ETF alt, insert the companion immediately after
                if (!c.isEtf() && !c.etfAlt().isEmpty() && etfByTicker.containsKey(c.etfAlt())) {
                    WheelCandidate etfRow = etfByTicker.get(c.etfAlt());
                    // Re-create as companion row
                    outputRows.add(new WheelCandidate(
                        etfRow.ticker(), etfRow.price(), etfRow.iv(), etfRow.putStrike(), etfRow.putPremium(),
                        etfRow.weeklyReturnPct(), etfRow.expiryDate(), etfRow.volume(),
                        etfRow.callStrikeVal(), etfRow.callPremium(), etfRow.isEtf(),
                        etfRow.percentChange(), etfRow.isRedDay(), etfRow.bearishWeekly(),
                        etfRow.delta(), etfRow.capitalIfAssigned(), etfRow.takeProfitAt(),
                        etfRow.earningsDaysAway(), etfRow.earningsThisWeek(),
                        etfRow.low20d(), etfRow.nearSupport(),
                        etfRow.analystBuyPct(), etfRow.insiderMspr(),
                        "", true, c.ticker()
                    ));
                    insertedEtfs.add(c.etfAlt());
                }
            }

            // Cap output at 8 rows
            List<WheelCandidate> top = outputRows.subList(0, Math.min(8, outputRows.size()));

            // ── Build JSON ────────────────────────────────────────────────────
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < top.size(); i++) {
                WheelCandidate c = top.get(i);
                if (i > 0) sb.append(",");
                String priority = c.isRedDay() ? "RED_DAY"
                    : c.bearishWeekly() ? "BEARISH_BIAS" : "STANDARD";
                sb.append(String.format(
                    "{\"ticker\":\"%s\",\"price\":%.2f,\"iv\":%.1f," +
                    "\"put_strike\":%.2f,\"put_premium\":%.2f," +
                    "\"total_premium_per_contract\":%.0f,\"weekly_return_pct\":%.2f," +
                    "\"expiry\":\"%s\",\"volume\":%d," +
                    "\"call_strike\":\"%.2f\",\"call_premium\":%.2f," +
                    "\"is_etf\":%b,\"percent_change\":%.2f,\"priority\":\"%s\"," +
                    "\"delta\":%.2f,\"capital_if_assigned\":%.0f,\"take_profit_at\":%.2f," +
                    "\"earnings_days_away\":%d,\"earnings_this_week\":%b," +
                    "\"near_support\":%b,\"analyst_buy_pct\":%d,\"insider_mspr\":%.1f," +
                    "\"etf_alt\":\"%s\",\"is_companion\":%b,\"parent_ticker\":\"%s\"}",
                    c.ticker(), c.price(), c.iv() * 100,
                    c.putStrike(), c.putPremium(), c.putPremium() * 100, c.weeklyReturnPct(),
                    c.expiryDate(), c.volume(),
                    c.callStrikeVal(), c.callPremium(),
                    c.isEtf(), c.percentChange(), priority,
                    c.delta(), c.capitalIfAssigned(), c.takeProfitAt(),
                    c.earningsDaysAway(), c.earningsThisWeek(),
                    c.nearSupport(), c.analystBuyPct(), c.insiderMspr(),
                    c.etfAlt(), c.isCompanion(), c.parentTicker()
                ));
            }
            sb.append("]");

            return String.format("{\"status\":\"success\",\"scan_date\":\"%s\",\"wheel_candidates\":%s}",
                today, sb);

        } catch (Exception e) {
            return "{\"error\":\"Wheel scan failed: " + e.getMessage() + "\"}";
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
                        .uri(URI.create(engine.finnhubBaseUrl + "/news-sentiment?symbol=" + sym + "&token=" + fhKey))
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
                "F","GE","DIS","SMCI","RIVN","BAC","JPM","XOM","CVX","SBUX",
                // Cybersecurity
                "CRWD","PANW","FTNT","ZS","S","OKTA","CYBR","NET","TENB",
                // Semis
                "AMAT","KLAC","LRCX","ON","MRVL","ASML",
                // Biotech
                "MRNA","BNTX","REGN","VRTX","GILD","IONS"
        ));
        List<String> screenerTickers = fetchScreenerTickers(
                new String[]{"most_actives", "day_gainers", "day_losers"}, 12, 20, 1_000_000L);
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
                "AVGO","MU","QCOM","CRM","NOW","PLTR","UBER","COIN","MSTR","ARM",
                // Cybersecurity
                "CRWD","PANW","FTNT","ZS","S","OKTA","CYBR","NET",
                // Semis
                "AMAT","KLAC","LRCX","ON","MRVL",
                // Biotech
                "MRNA","BNTX","REGN","VRTX","GILD","BIIB"
        ));
        List<String> screenerTickers = fetchScreenerTickers(
                new String[]{"most_actives", "day_gainers"}, 10, 20, 1_000_000L);
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
                new String[]{"day_losers", "most_actives", "day_gainers"}, 18, 20, 1_000_000L);
        if (tickers.isEmpty()) return "{\"error\":\"No eligible tickers found.\"}";

        List<String> results = runConcurrentScan(tickers);
        results.removeIf(r -> {
            boolean isSwingLong = "SWING_LONG".equals(engine.extractSwingSignal(r));
            boolean hasBullDiv  = "BULLISH_DIV".equals(engine.extractRsiDivergence(r));
            boolean isOversold  = engine.extractRsi14d(r) < 40.0;
            return !(isSwingLong && (hasBullDiv || isOversold));
        });
        results.sort((a, b) -> Double.compare(compositeRankScore(b), compositeRankScore(a)));
        if (results.isEmpty()) return "{\"status\":\"success\",\"ticker_count\":0,\"failed_breakdown_results\":[]}";
        return buildScanResponse(results.subList(0, Math.min(5, results.size())), "failed_breakdown_results");
    }
}
