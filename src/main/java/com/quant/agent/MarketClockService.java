package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Service
public class MarketClockService {

    private static final Logger log = LoggerFactory.getLogger(MarketClockService.class);
    private static final ZoneId ET = ZoneId.of("America/New_York");
    private static final long CACHE_SECONDS = 60;

    public enum Session { PRE_MARKET, REGULAR, POST_MARKET, CLOSED }

    @Value("${alpaca.api.key}")
    private String apiKey;

    @Value("${alpaca.api.secret}")
    private String apiSecret;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private volatile Session cachedSession = Session.CLOSED;
    private volatile Instant cacheExpiry = Instant.MIN;

    // ── Public API ────────────────────────────────────────────────────────────

    public Session getCurrentSession() {
        if (Instant.now().isAfter(cacheExpiry)) {
            refresh();
        }
        return cachedSession;
    }

    public String toPlainEnglish() {
        return switch (getCurrentSession()) {
            case PRE_MARKET  -> "Pre-Market";
            case REGULAR     -> "Open Market";
            case POST_MARKET -> "Post-Market";
            case CLOSED      -> "Market Closed";
        };
    }

    // ── Refresh from Alpaca clock API ─────────────────────────────────────────

    private void refresh() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.alpaca.markets/v2/clock"))
                    .header("APCA-API-KEY-ID", apiKey)
                    .header("APCA-API-SECRET-KEY", apiSecret)
                    .GET()
                    .build();

            HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            if (res.statusCode() == 200) {
                JsonNode clock = objectMapper.readTree(res.body());
                boolean isOpen = clock.path("is_open").asBoolean();

                if (isOpen) {
                    cachedSession = Session.REGULAR;
                } else {
                    cachedSession = extendedHoursSession();
                }
                cacheExpiry = Instant.now().plusSeconds(CACHE_SECONDS);
                log.debug("Market session: {}", cachedSession);
            } else {
                // API unavailable — fall back to time-based detection, cache briefly
                log.warn("Clock API returned {}, falling back to time-based detection", res.statusCode());
                cachedSession = extendedHoursSession();
                cacheExpiry = Instant.now().plusSeconds(15);
            }
        } catch (Exception e) {
            log.warn("Clock API error: {} — using time-based fallback", e.getMessage());
            cachedSession = extendedHoursSession();
            cacheExpiry = Instant.now().plusSeconds(15);
        }
    }

    // Determines pre/post/closed from ET wall-clock time.
    // Used when is_open=false or when the Alpaca API is unreachable.
    //   Pre-market:  04:00 – 09:29 ET
    //   Post-market: 16:00 – 19:59 ET
    //   Closed:      everything else (overnight / weekend)
    private Session extendedHoursSession() {
        ZonedDateTime now = ZonedDateTime.now(ET);
        int hour = now.getHour();
        int min  = now.getMinute();
        boolean isPreMarket  = (hour >= 4  && (hour < 9  || (hour == 9  && min < 30)));
        boolean isPostMarket = (hour >= 16 && hour < 20);
        if (isPreMarket)  return Session.PRE_MARKET;
        if (isPostMarket) return Session.POST_MARKET;
        return Session.CLOSED;
    }
}
