package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
     * Priority:
     *   1. Alpaca WebSocket cache (zero new API calls — WS already running)
     *   2. Yahoo Finance REST (single free HTTP call, no API key required)
     * This endpoint should never trigger a full multi-timeframe analysis.
     */
    @GetMapping("/api/price/{symbol}")
    public PriceResponse getPrice(@PathVariable String symbol) {
        String sym = symbol.toUpperCase().trim();

        // Subscribe to WS stream (no-op if already subscribed)
        alpacaStreamService.subscribe(sym);

        // Try WS cache first — no API call consumed
        Optional<AlpacaStreamService.LiveQuote> cached = alpacaStreamService.getLatestQuote(sym);
        if (cached.isPresent()) {
            AlpacaStreamService.LiveQuote q = cached.get();
            String updatedAt = TIME_FMT.format(q.timestamp().atZone(ET));
            return new PriceResponse(sym, q.price(), 0.0, 0.0, updatedAt, "live");
        }

        // Fallback: Yahoo Finance (free, no API key)
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
                JsonNode meta = objectMapper.readTree(res.body())
                        .path("chart").path("result").path(0).path("meta");
                double price    = meta.path("regularMarketPrice").asDouble();
                double prevClose = meta.path("chartPreviousClose").asDouble();
                if (prevClose <= 0) prevClose = meta.path("previousClose").asDouble();
                double change    = price - prevClose;
                double changePct = prevClose > 0 ? (change / prevClose) * 100.0 : 0.0;
                String updatedAt = TIME_FMT.format(LocalTime.now(ET));
                return new PriceResponse(sym, price, change, changePct, updatedAt, "yahoo");
            }
        } catch (Exception e) {
            log.warn("Price refresh failed for {}: {}", sym, e.getMessage());
        }
        return new PriceResponse(sym, 0.0, 0.0, 0.0, "--:--:--", "error");
    }

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
