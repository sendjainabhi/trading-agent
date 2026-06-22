package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

@Service
public class AlpacaStreamService {

    private static final Logger log = LoggerFactory.getLogger(AlpacaStreamService.class);
    private static final String WS_URL = "wss://stream.data.alpaca.markets/v2/iex";
    // Quote cache TTL: 30 seconds — quotes older than this fall back to REST
    private static final long CACHE_TTL_SECONDS = 30;

    @Value("${alpaca.api.key}")
    private String apiKey;

    @Value("${alpaca.api.secret}")
    private String apiSecret;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .executor(Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "alpaca-ws-io");
                t.setDaemon(true);
                return t;
            }))
            .build();

    private final ConcurrentHashMap<String, LiveQuote> priceCache = new ConcurrentHashMap<>();
    private final Set<String> subscribedSymbols = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "alpaca-ws-watchdog");
        t.setDaemon(true);
        return t;
    });

    private volatile WebSocket webSocket;
    private volatile boolean authenticated = false;
    // Serialise sends — WebSocket forbids concurrent sends
    private volatile CompletableFuture<?> lastSend = CompletableFuture.completedFuture(null);

    public record LiveQuote(String symbol, double price, double bid, double ask, Instant timestamp) {}

    @PostConstruct
    public void start() {
        connectWebSocket();
        watchdog.scheduleAtFixedRate(this::ensureConnected, 30, 30, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        watchdog.shutdown();
        WebSocket ws = webSocket;
        if (ws != null) ws.abort();
    }

    // ── Public API ──────────────────────────────────────────────────────────

    public void subscribe(String symbol) {
        if (subscribedSymbols.add(symbol)) {
            if (authenticated) {
                sendMessage(subscribePayload(symbol));
            }
        }
    }

    public Optional<LiveQuote> getLatestQuote(String symbol) {
        LiveQuote q = priceCache.get(symbol);
        if (q != null && Instant.now().minusSeconds(CACHE_TTL_SECONDS).isBefore(q.timestamp())) {
            return Optional.of(q);
        }
        return Optional.empty();
    }

    // ── Connection lifecycle ─────────────────────────────────────────────────

    private void connectWebSocket() {
        try {
            webSocket = httpClient.newWebSocketBuilder()
                    .buildAsync(URI.create(WS_URL), new AlpacaListener())
                    .orTimeout(10, TimeUnit.SECONDS)
                    .join();
        } catch (Exception e) {
            log.error("Alpaca WebSocket connect failed: {}", e.getMessage());
            webSocket = null;
        }
    }

    private void ensureConnected() {
        WebSocket ws = webSocket;
        if (ws == null || ws.isInputClosed()) {
            log.info("Alpaca WebSocket reconnecting...");
            authenticated = false;
            lastSend = CompletableFuture.completedFuture(null);
            connectWebSocket();
        }
    }

    // ── Send helpers ─────────────────────────────────────────────────────────

    private synchronized void sendMessage(String msg) {
        WebSocket ws = webSocket;
        if (ws == null || ws.isInputClosed()) return;
        lastSend = lastSend
                .handle((v, ex) -> null)
                .thenCompose(ignored -> ws.sendText(msg, true))
                .exceptionally(e -> { log.warn("WebSocket send failed: {}", e.getMessage()); return null; });
    }

    private String subscribePayload(String symbol) {
        return String.format("{\"action\":\"subscribe\",\"trades\":[\"%s\"],\"quotes\":[\"%s\"]}", symbol, symbol);
    }

    private void resubscribeAll() {
        if (subscribedSymbols.isEmpty()) return;
        String list = subscribedSymbols.stream()
                .map(s -> "\"" + s + "\"")
                .reduce((a, b) -> a + "," + b)
                .orElse("");
        sendMessage(String.format("{\"action\":\"subscribe\",\"trades\":[%s],\"quotes\":[%s]}", list, list));
    }

    // ── WebSocket listener ────────────────────────────────────────────────────

    private class AlpacaListener implements WebSocket.Listener {

        private final StringBuilder buf = new StringBuilder();

        @Override
        public void onOpen(WebSocket ws) {
            log.info("Alpaca WebSocket opened");
            ws.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            buf.append(data);
            if (last) {
                handleMessages(buf.toString());
                buf.setLength(0);
            }
            ws.request(1);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            log.error("Alpaca WebSocket error: {}", error.getMessage());
            authenticated = false;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            log.info("Alpaca WebSocket closed [{} {}]", statusCode, reason);
            authenticated = false;
            return null;
        }
    }

    // ── Message dispatch ──────────────────────────────────────────────────────

    private void handleMessages(String raw) {
        try {
            JsonNode arr = objectMapper.readTree(raw);
            if (!arr.isArray()) return;
            for (JsonNode msg : arr) {
                switch (msg.path("T").asText()) {
                    case "success" -> handleSuccess(msg);
                    case "t"       -> handleTrade(msg);
                    case "q"       -> handleQuote(msg);
                    case "error"   -> log.warn("Alpaca WS error msg: {}", msg.path("msg").asText());
                }
            }
        } catch (Exception e) {
            log.debug("Failed to parse WebSocket message: {}", e.getMessage());
        }
    }

    private void handleSuccess(JsonNode msg) {
        String text = msg.path("msg").asText();
        if ("connected".equals(text)) {
            sendMessage(String.format("{\"action\":\"auth\",\"key\":\"%s\",\"secret\":\"%s\"}", apiKey, apiSecret));
        } else if ("authenticated".equals(text)) {
            log.info("Alpaca WebSocket authenticated — subscribing {} symbols", subscribedSymbols.size());
            authenticated = true;
            resubscribeAll();
        }
    }

    private void handleTrade(JsonNode msg) {
        String symbol = msg.path("S").asText();
        double price  = msg.path("p").asDouble();
        if (symbol.isBlank() || price <= 0) return;
        priceCache.merge(symbol, new LiveQuote(symbol, price, 0, 0, Instant.now()),
                (old, n) -> new LiveQuote(symbol, price, old.bid(), old.ask(), Instant.now()));
    }

    private void handleQuote(JsonNode msg) {
        String symbol = msg.path("S").asText();
        double bid    = msg.path("bp").asDouble();
        double ask    = msg.path("ap").asDouble();
        if (symbol.isBlank()) return;
        priceCache.merge(symbol, new LiveQuote(symbol, (bid + ask) / 2, bid, ask, Instant.now()),
                (old, n) -> new LiveQuote(symbol, old.price(), bid, ask, Instant.now()));
    }
}
