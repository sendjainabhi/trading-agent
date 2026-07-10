package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.StringReader;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.springframework.web.multipart.MultipartFile;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class TradingAgentController {

    private static final Logger log = LoggerFactory.getLogger(TradingAgentController.class);
    private static final ZoneId ET = ZoneId.of("America/New_York");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Value("${yahoo.finance.base-url:https://query1.finance.yahoo.com}")
    private String yahooBaseUrl;

    @Value("${tradingview.scanner.futures-url:https://scanner.tradingview.com/futures/scan}")
    private String tradingviewFuturesUrl;

    @Value("${tradingview.scanner.stocks-url:https://scanner.tradingview.com/america/scan}")
    private String tradingviewStocksUrl;

    @Value("${tradingview.scanner.origin:https://www.tradingview.com}")
    private String tradingviewOrigin;

    private final TradingAgentService tradingAgentService;
    private final AlpacaStreamService alpacaStreamService;
    private final UserPrefsService    userPrefsService;
    private final MarketClockService  marketClockService;
    private final MarketPulseService  marketPulseService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public TradingAgentController(TradingAgentService tradingAgentService,
                                  AlpacaStreamService alpacaStreamService,
                                  UserPrefsService userPrefsService,
                                  MarketClockService marketClockService,
                                  MarketPulseService marketPulseService) {
        this.tradingAgentService = tradingAgentService;
        this.alpacaStreamService = alpacaStreamService;
        this.userPrefsService    = userPrefsService;
        this.marketClockService  = marketClockService;
        this.marketPulseService  = marketPulseService;
    }

    @GetMapping("/api/market/pulse")
    public MarketPulseService.MarketPulseResult getMarketPulse() {
        return marketPulseService.getMarketPulse();
    }

    // 15-second TTL cache — prevents hammering Yahoo Finance on every UI refresh tick
    private record CachedPrice(PriceResponse response, long fetchedAt) {
        boolean fresh() { return System.currentTimeMillis() - fetchedAt < 15_000; }
    }
    private final ConcurrentHashMap<String, CachedPrice> priceCache = new ConcurrentHashMap<>();

    public record ChatRequest(String input) {}

    public record PriceResponse(
            String symbol,
            double price,
            double change,
            double changePercent,
            String updatedAt,
            String source
    ) {}

    @PostMapping(value = "/api/chat/stream", produces = MediaType.TEXT_PLAIN_VALUE)
    public Flux<String> streamChat(@RequestBody ChatRequest request) {
        return this.tradingAgentService.streamAgentResponse(request.input());
    }

    /**
     * Lightweight price refresh — used by the UI's auto-refresh ticker strip.
     *
     * Always calls Yahoo Finance to obtain prevClose (needed to compute change%).
     * Then overlays the Alpaca WebSocket price if a fresh quote (< 30 s) is cached —
     * the WS price is more current but carries no prevClose of its own.
     */
    @GetMapping("/api/price/{symbol}")
    public PriceResponse getPrice(@PathVariable String symbol) {
        String sym = symbol.toUpperCase().trim();
        // Skip Alpaca WS subscription for futures/index symbols (e.g. ES=F, ^VIX)
        if (!sym.contains("=") && !sym.startsWith("^")) alpacaStreamService.subscribe(sym);

        CachedPrice hit = priceCache.get(sym);
        if (hit != null && hit.fresh()) return hit.response();

        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(yahooBaseUrl + "/v8/finance/chart/"
                            + sym + "?interval=1m&range=1d&includePrePost=true"))
                    .header("User-Agent", "Mozilla/5.0").timeout(Duration.ofSeconds(8)).GET().build();
            HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() == 200) {
                JsonNode root = objectMapper.readTree(res.body());
                JsonNode resultNode = root.path("chart").path("result").path(0);
                JsonNode meta = resultNode.path("meta");
                double yahooPrice = meta.path("regularMarketPrice").asDouble();
                double prevClose  = meta.path("chartPreviousClose").asDouble();
                if (prevClose <= 0) prevClose = meta.path("previousClose").asDouble();
                JsonNode closes = resultNode.path("indicators").path("quote").path(0).path("close");
                if (closes.isArray()) {
                    for (int i = closes.size() - 1; i >= 0; i--) {
                        if (!closes.get(i).isNull()) { yahooPrice = closes.get(i).asDouble(); break; }
                    }
                }
                Optional<AlpacaStreamService.LiveQuote> wsQuote = alpacaStreamService.getLatestQuote(sym);
                double price  = wsQuote.map(AlpacaStreamService.LiveQuote::price).filter(p -> p > 0).orElse(yahooPrice);
                String source = wsQuote.isPresent() ? "live" : "yahoo";
                double change    = prevClose > 0 ? price - prevClose : 0.0;
                double changePct = prevClose > 0 ? (change / prevClose) * 100.0 : 0.0;
                String updatedAt = TIME_FMT.format(LocalTime.now(ET));
                PriceResponse resp = new PriceResponse(sym, price, change, changePct, updatedAt, source);
                priceCache.put(sym, new CachedPrice(resp, System.currentTimeMillis()));
                return resp;
            }
        } catch (Exception e) {
            log.warn("Price refresh failed for {}: {}", sym, e.getMessage());
        }
        return new PriceResponse(sym, 0.0, 0.0, 0.0, "--:--:--", "error");
    }

    // ── Market bar — TradingView scanner (stocks + futures) ──────────────────

    // 5-second TTL matches the UI refresh interval
    private record CachedBar(List<Map<String,Object>> data, long fetchedAt) {
        boolean fresh() { return System.currentTimeMillis() - fetchedAt < 5_000; }
    }
    private volatile CachedBar cachedBar = null;

    @GetMapping("/api/market/bar")
    public List<Map<String,Object>> getMarketBar() {
        CachedBar hit = cachedBar;
        if (hit != null && hit.fresh()) return hit.data();

        // TradingView scanner symbols
        // Stocks/ETFs endpoint + Futures endpoint
        record TvSymbol(String tv, String label, boolean futures) {}
        List<TvSymbol> symbols = List.of(
            new TvSymbol("AMEX:SPY",       "SPY",  false),
            new TvSymbol("NASDAQ:QQQ",     "QQQ",  false),
            new TvSymbol("CME_MINI:ES1!",  "ES1!", true)
        );

        List<Map<String,Object>> result = new ArrayList<>();

        // Build two ticker lists: equities and futures
        List<String> equityTickers  = new ArrayList<>();
        List<String> futuresTickers = new ArrayList<>();
        for (TvSymbol s : symbols) {
            if (s.futures()) futuresTickers.add(s.tv());
            else              equityTickers.add(s.tv());
        }

        // Fetch from TradingView scanner (one call per endpoint)
        Map<String, double[]> prices = new java.util.LinkedHashMap<>(); // tv_symbol -> [close, changePct]

        for (boolean futures : new boolean[]{false, true}) {
            List<String> tickers = futures ? futuresTickers : equityTickers;
            if (tickers.isEmpty()) continue;
            String endpoint = futures
                ? tradingviewFuturesUrl
                : tradingviewStocksUrl;
            try {
                String tickerJson = tickers.stream()
                    .map(t -> "\"" + t + "\"")
                    .collect(java.util.stream.Collectors.joining(","));
                String body = "{\"symbols\":{\"tickers\":[" + tickerJson + "]," +
                    "\"query\":{\"types\":[]}}," +
                    "\"columns\":[\"close\",\"change\"]}";
                HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "Mozilla/5.0")
                    .header("Origin", tradingviewOrigin)
                    .header("Referer", tradingviewOrigin + "/")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(6))
                    .build();
                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    JsonNode data = objectMapper.readTree(resp.body()).path("data");
                    for (JsonNode row : data) {
                        String tvSym = row.path("s").asText();
                        JsonNode vals = row.path("d");
                        double close  = vals.path(0).asDouble(0);
                        double change = vals.path(1).asDouble(0);
                        if (close > 0) prices.put(tvSym, new double[]{close, change});
                    }
                }
            } catch (Exception e) {
                log.warn("TradingView market bar fetch failed: {}", e.getMessage());
            }
        }

        // Assemble result in original order
        String updatedAt = TIME_FMT.format(LocalTime.now(ET));
        for (TvSymbol s : symbols) {
            double[] p = prices.getOrDefault(s.tv(), new double[]{0, 0});
            Map<String,Object> row = new java.util.LinkedHashMap<>();
            row.put("symbol",     s.label());
            row.put("price",      p[0]);
            row.put("changePct",  p[1]);
            row.put("updatedAt",  updatedAt);
            row.put("source",     p[0] > 0 ? "tradingview" : "error");
            result.add(row);
        }

        cachedBar = new CachedBar(result, System.currentTimeMillis());
        return result;
    }

    // ── Model configuration endpoints ─────────────────────────────────────────

    @GetMapping("/api/model/status")
    public Map<String, Object> getModelStatus() {
        return tradingAgentService.getModelStatus();
    }

    @GetMapping("/api/model/config")
    public Map<String, Object> getModelConfig() {
        return tradingAgentService.getModelConfig();
    }

    @PostMapping("/api/model/config")
    public Map<String, Object> setModelConfig(@RequestBody Map<String, String> config) {
        Map<String, Object> result = tradingAgentService.updateModelConfig(config);
        if (Boolean.TRUE.equals(result.get("success"))) {
            userPrefsService.setModelConfig(config);
        }
        return result;
    }

    @PostMapping("/api/model/test")
    public Map<String, Object> testConnection(
            @RequestBody(required = false) Map<String, String> config) {
        if (config != null && !config.isEmpty()) {
            return tradingAgentService.testProviderConfig(config);
        }
        return tradingAgentService.testProviderConnection();
    }

    @PostMapping("/api/model/upload-config")
    public Map<String, Object> uploadConfig(@RequestParam("file") MultipartFile file) {
        try {
            String content  = new String(file.getBytes()).trim();
            String filename = file.getOriginalFilename() != null ? file.getOriginalFilename() : "";
            Map<String, String> config = new HashMap<>();

            if (filename.endsWith(".json")) {
                // ── JSON ──────────────────────────────────────────────────────
                JsonNode root = objectMapper.readTree(content);
                if (root.has("provider"))    config.put("provider",    root.path("provider").asText());
                if (root.has("model"))       config.put("model",       root.path("model").asText());
                if (root.has("apiKey"))      config.put("apiKey",      root.path("apiKey").asText());
                if (root.has("baseUrl"))     config.put("baseUrl",     root.path("baseUrl").asText());
                if (root.has("temperature")) config.put("temperature", root.path("temperature").asText());
            } else {
                // ── .properties / .txt  (key=value, one per line) ─────────────
                Properties props = new Properties();
                props.load(new StringReader(content));
                for (String key : new String[]{"provider", "model", "apiKey", "baseUrl", "temperature"}) {
                    String val = props.getProperty(key);
                    if (val != null && !val.isBlank()) config.put(key, val.trim());
                }
            }

            return tradingAgentService.updateModelConfig(config);
        } catch (Exception e) {
            return Map.of("success", false, "error", "Invalid config file: " + e.getMessage());
        }
    }

    // ── Market clock ─────────────────────────────────────────────────────────

    @GetMapping("/api/market/clock")
    public Map<String, Object> getMarketClock() {
        MarketClockService.Session session = marketClockService.getCurrentSession();
        int etHour = java.time.ZonedDateTime.now(ET).getHour();
        return Map.of(
                "session",  session.name(),
                "is_open",  session == MarketClockService.Session.REGULAR,
                "et_hour",  etHour,
                "label",    marketClockService.toPlainEnglish()
        );
    }

    // ── Recommendation journal ────────────────────────────────────────────────

    @GetMapping("/api/journal")
    public List<Map<String, String>> getJournal() {
        return userPrefsService.getJournal();
    }

    @PostMapping("/api/journal")
    public Map<String, Object> addJournalEntry(@RequestBody Map<String, String> entry) {
        if (entry == null || entry.getOrDefault("ticker", "").isBlank()) {
            return Map.of("success", false, "error", "ticker required");
        }
        Map<String, String> safeEntry = new HashMap<>(entry);
        if (!safeEntry.containsKey("date")) {
            safeEntry.put("date", java.time.LocalDate.now().toString());
        }
        userPrefsService.addJournalEntry(safeEntry);
        return Map.of("success", true);
    }

    @DeleteMapping("/api/journal")
    public Map<String, Object> clearJournal() {
        userPrefsService.clearJournal();
        return Map.of("success", true);
    }

    // ── Symbol search ─────────────────────────────────────────────────────────

    @GetMapping("/api/search")
    public List<Map<String, String>> searchSymbols(@RequestParam String q) {
        try {
            String encoded = URLEncoder.encode(q.trim(), StandardCharsets.UTF_8);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(yahooBaseUrl + "/v1/finance/search?q=" + encoded
                            + "&lang=en-US&region=US&quotesCount=8&newsCount=0"))
                    .header("User-Agent", "Mozilla/5.0")
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return List.of();
            JsonNode quotes = objectMapper.readTree(res.body()).path("quotes");
            List<Map<String, String>> result = new ArrayList<>();
            for (JsonNode item : quotes) {
                String type = item.path("quoteType").asText("");
                String sym  = item.path("symbol").asText("").trim();
                if ((!type.equals("EQUITY") && !type.equals("ETF")) || sym.isBlank() || sym.contains(".")) continue;
                String name = item.path("shortname").asText(item.path("longname").asText(sym));
                result.add(Map.of("symbol", sym, "name", name));
                if (result.size() >= 7) break;
            }
            return result;
        } catch (Exception e) {
            log.warn("Symbol search failed for '{}': {}", q, e.getMessage());
            return List.of();
        }
    }
}
