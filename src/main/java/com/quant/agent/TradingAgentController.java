package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.StringReader;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class TradingAgentController {

    private static final Logger log = LoggerFactory.getLogger(TradingAgentController.class);
    private static final ZoneId ET = ZoneId.of("America/New_York");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final TradingAgentService tradingAgentService;
    private final AlpacaStreamService alpacaStreamService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public TradingAgentController(TradingAgentService tradingAgentService,
                                  AlpacaStreamService alpacaStreamService) {
        this.tradingAgentService = tradingAgentService;
        this.alpacaStreamService = alpacaStreamService;
    }

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
        alpacaStreamService.subscribe(sym);

        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/"
                            + sym + "?interval=1m&range=1d&includePrePost=true"))
                    .header("User-Agent", "Mozilla/5.0")
                    .timeout(Duration.ofSeconds(8))
                    .GET()
                    .build();
            HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() == 200) {
                JsonNode root       = objectMapper.readTree(res.body());
                JsonNode resultNode = root.path("chart").path("result").path(0);
                JsonNode meta       = resultNode.path("meta");

                double yahooPrice = meta.path("regularMarketPrice").asDouble();
                double prevClose  = meta.path("chartPreviousClose").asDouble();
                if (prevClose <= 0) prevClose = meta.path("previousClose").asDouble();

                // Prefer the most recent 1-min bar close over the meta snapshot
                // (captures pre/post-market moves that regularMarketPrice misses)
                JsonNode closes = resultNode.path("indicators").path("quote").path(0).path("close");
                if (closes.isArray()) {
                    for (int i = closes.size() - 1; i >= 0; i--) {
                        if (!closes.get(i).isNull()) { yahooPrice = closes.get(i).asDouble(); break; }
                    }
                }

                // Use Alpaca WS price if it has a fresh quote — otherwise keep Yahoo's
                Optional<AlpacaStreamService.LiveQuote> wsQuote = alpacaStreamService.getLatestQuote(sym);
                double price  = wsQuote.map(AlpacaStreamService.LiveQuote::price).filter(p -> p > 0).orElse(yahooPrice);
                String source = wsQuote.isPresent() ? "live" : "yahoo";

                double change    = prevClose > 0 ? price - prevClose : 0.0;
                double changePct = prevClose > 0 ? (change / prevClose) * 100.0 : 0.0;
                String updatedAt = TIME_FMT.format(LocalTime.now(ET));
                return new PriceResponse(sym, price, change, changePct, updatedAt, source);
            }
        } catch (Exception e) {
            log.warn("Price refresh failed for {}: {}", sym, e.getMessage());
        }
        return new PriceResponse(sym, 0.0, 0.0, 0.0, "--:--:--", "error");
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
        return tradingAgentService.updateModelConfig(config);
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

    // ── Symbol search ─────────────────────────────────────────────────────────

    @GetMapping("/api/search")
    public List<Map<String, String>> searchSymbols(@RequestParam String q) {
        try {
            String encoded = URLEncoder.encode(q.trim(), StandardCharsets.UTF_8);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://query1.finance.yahoo.com/v1/finance/search?q=" + encoded
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
