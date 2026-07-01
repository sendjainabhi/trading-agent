package com.quant.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;

/**
 * HTTP infrastructure for all Alpaca API calls.
 * Owns the shared HttpClient, credentials, and request-builder helpers.
 */
@Component
public class AlpacaClient {

    // ── Shared HTTP client ────────────────────────────────────────────────────

    final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** Shared ObjectMapper — safe to reuse across threads (Jackson is thread-safe). */
    final ObjectMapper objectMapper = new ObjectMapper();

    // ── Alpaca credentials (injected from application properties) ────────────

    @Value("${alpaca.api.key}")
    String apiKey;

    @Value("${alpaca.api.secret}")
    String apiSecret;

    @Value("${alpaca.data.base-url:https://data.alpaca.markets}")
    String alpacaDataBaseUrl;

    // ── Injected services ─────────────────────────────────────────────────────

    final AlpacaStreamService alpacaStreamService;

    public AlpacaClient(AlpacaStreamService alpacaStreamService) {
        this.alpacaStreamService = alpacaStreamService;
    }

    // ── HTTP request builders ─────────────────────────────────────────────────

    /** Builds a GET request against the Alpaca data gateway at {@code fullPath}. */
    public HttpRequest buildAlpacaBaseRequest(String fullPath) {
        return HttpRequest.newBuilder()
                .uri(URI.create(alpacaDataBaseUrl + fullPath))
                .header("APCA-API-KEY-ID", apiKey != null ? apiKey : "")
                .header("APCA-API-SECRET-KEY", apiSecret != null ? apiSecret : "")
                .header("accept", "application/json")
                .timeout(Duration.ofSeconds(8))
                .GET()
                .build();
    }

    /** Convenience wrapper — prepends the {@code /v2/stocks} path segment. */
    public HttpRequest buildAlpacaRequest(String endpoint) {
        return buildAlpacaBaseRequest("/v2/stocks" + endpoint);
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /** Returns the shared HTTP client (used by services that make non-Alpaca calls). */
    public HttpClient getHttpClient() {
        return httpClient;
    }

    /** Returns the shared ObjectMapper. */
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
