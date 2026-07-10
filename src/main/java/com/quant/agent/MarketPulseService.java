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
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class MarketPulseService {

    private static final Logger log = LoggerFactory.getLogger(MarketPulseService.class);
    private static final ZoneId ET = ZoneId.of("America/New_York");
    private static final long CACHE_TTL_MS = 15 * 60 * 1000L;

    @Value("${market.provider.api-key}")
    private String finnhubKey;

    @Value("${market.provider.base-url:https://finnhub.io/api/v1}")
    private String finnhubBaseUrl;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile CachedPulse cache;

    // ── Records ───────────────────────────────────────────────────────────────

    public record EconomicEvent(String date, String etTime, String event,
                                 String estimate, String prev, String actual, String unit) {}

    public record NewsItem(String headline, String source, String url, int score, String ageLabel) {}

    public record MarketPulseResult(List<EconomicEvent> events, List<NewsItem> news, String updatedAt) {}

    private record CachedPulse(MarketPulseResult result, long at) {
        boolean fresh() { return System.currentTimeMillis() - at < CACHE_TTL_MS; }
    }

    // ── Keyword scoring: term → impact score ─────────────────────────────────

    private static final String[] KW_TERMS = {
        // Fed / monetary policy
        "federal reserve", "fomc", "rate decision", "rate cut", "rate hike", "fed chair", "powell",
        // Inflation — include standalone "cpi" and "pce"
        "consumer price index", "cpi report", "cpi data", "inflation report", "inflation data",
        "pce report", "pce data", ", cpi,", ", cpi ", "cpi,",
        // Labor
        "non-farm payroll", "nonfarm payroll", "jobs report", "payroll report",
        "unemployment rate", "jobless claims", "initial claims",
        // GDP / growth
        "gdp report", "gdp growth", "recession", "debt ceiling", "interest rate",
        // FDA / regulatory
        "fda approval", "fda approved", "fda reject", "fda denied", "fda decision",
        // M&A
        "merger agreement", "acquisition agreement", "takeover bid", "buyout offer", "acquired by",
        // Distress
        "bankruptcy", "chapter 11", "debt default",
        // Earnings
        "earnings beat", "earnings miss", "profit warning", "guidance cut", "guidance raise",
        // Geopolitical — active conflicts, sanctions, trade (market-moving)
        "iran war", "iran strike", "us-iran", "iran attack", "iran deal", "iran ceasefire",
        "war resumes", "war escalat", "military strike", "ballistic missile",
        "strait of hormuz", "hormuz", "oil embargo",
        "china tariff", "tariff announcement", "export ban", "new sanctions", "trade war",
        "war", "ceasefire", "escalat", "iran", "sanctions",
        // Oil & energy
        "oil prices surge", "crude oil surges", "crude oil falls",
        // Layoffs / ratings
        "mass layoffs", "major layoffs",
        "upgrade", "downgrade", "price target raised", "price target cut",
        // IPO
        "ipo priced", "direct listing",
        // Generic (below threshold — keep for scoring but not shown)
        "earnings", "quarterly results"
    };

    private static final int[] KW_SCORES = {
        // Fed (7 terms)
        10, 10, 10, 10, 10, 9, 9,
        // Inflation (10 terms)
        10, 10, 10, 10, 10, 10, 10, 9, 9, 9,
        // Labor (7 terms)
        10, 10, 10, 10, 9, 8, 8,
        // GDP (5 terms)
        9, 9, 9, 9, 9,
        // FDA (5 terms)
        10, 10, 10, 10, 10,
        // M&A (5 terms)
        9, 9, 9, 9, 9,
        // Distress (3 terms)
        9, 9, 9,
        // Earnings (5 terms)
        8, 8, 8, 8, 8,
        // Geopolitical war (6 terms)
        9, 9, 9, 9, 8, 8,
        // Military (4 terms)
        8, 8, 8, 8,
        // Hormuz/oil (3 terms)
        8, 8, 8,
        // Trade (5 terms)
        8, 7, 7, 7, 7,
        // General conflict (5 terms)
        7, 7, 7, 7, 7,
        // Oil energy (3 terms)
        8, 7, 7,
        // Layoffs (2 terms)
        8, 8,
        // Ratings (4 terms)
        6, 6, 6, 6,
        // IPO (2 terms)
        6, 6,
        // Generic (2 terms)
        5, 5
    };

    // ── Public ────────────────────────────────────────────────────────────────

    public MarketPulseResult getMarketPulse() {
        CachedPulse c = cache;
        if (c != null && c.fresh()) return c.result;
        try {
            List<EconomicEvent> events = fetchCalendar();
            List<NewsItem> news = fetchNews();
            String ts = ZonedDateTime.now(ET).format(DateTimeFormatter.ofPattern("h:mm a z"));
            MarketPulseResult r = new MarketPulseResult(events, news, ts);
            cache = new CachedPulse(r, System.currentTimeMillis());
            return r;
        } catch (Exception e) {
            log.warn("MarketPulse fetch error: {}", e.getMessage());
            return new MarketPulseResult(List.of(), List.of(), "unavailable");
        }
    }

    // ── Economic calendar ─────────────────────────────────────────────────────

    private List<EconomicEvent> fetchCalendar() {
        try {
            LocalDate today = LocalDate.now(ET);
            String url = finnhubBaseUrl + "/calendar/economic?from=" + today
                    + "&to=" + today.plusDays(7) + "&token=" + finnhubKey;
            HttpResponse<String> res = http.send(
                    HttpRequest.newBuilder().uri(URI.create(url))
                            .timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                log.debug("Economic calendar HTTP {}: {}", res.statusCode(), res.body().substring(0, Math.min(200, res.body().length())));
                return List.of();
            }
            JsonNode cal = mapper.readTree(res.body()).path("economicCalendar");
            if (!cal.isArray()) return List.of();
            List<EconomicEvent> out = new ArrayList<>();
            for (JsonNode n : cal) {
                if (!"US".equalsIgnoreCase(n.path("country").asText())) continue;
                if (!"high".equalsIgnoreCase(n.path("impact").asText())) continue;
                String rawTime = n.path("time").asText("");
                String[] parts = rawTime.split(" ", 2);
                String date    = parts.length > 0 ? parts[0] : "";
                String etTime  = parts.length > 1 ? utcToEt(parts[0], parts[1]) : "";
                out.add(new EconomicEvent(date, etTime,
                        n.path("event").asText(""),
                        n.path("estimate").asText(""),
                        n.path("prev").asText(""),
                        n.path("actual").asText(""),
                        n.path("unit").asText("")));
            }
            out.sort(Comparator.comparing(e -> e.date() + e.etTime()));
            List<EconomicEvent> finnhubResult = out.subList(0, Math.min(out.size(), 8));
            // Finnhub free tier often returns no calendar data — fall back to static schedule
            return finnhubResult.isEmpty() ? staticCalendar() : finnhubResult;
        } catch (Exception e) {
            log.warn("Economic calendar error: {}", e.getMessage());
            return staticCalendar();
        }
    }

    /**
     * Computes the next 3-5 high-impact US economic events from a known release schedule.
     * Used when Finnhub returns empty calendar data (common on free tier).
     */
    private List<EconomicEvent> staticCalendar() {
        LocalDate today = LocalDate.now(ET);
        List<EconomicEvent> out = new ArrayList<>();

        // NFP: first Friday of each month, 8:30am ET
        LocalDate nfp = firstFridayOfMonth(today.withDayOfMonth(1));
        if (nfp.isBefore(today)) {
            nfp = firstFridayOfMonth(today.plusMonths(1).withDayOfMonth(1));
        }
        out.add(new EconomicEvent(nfp.toString(),
                nfp.format(DateTimeFormatter.ofPattern("MMM d")) + ", 8:30 AM",
                "Non-Farm Payrolls (NFP)", "", "", "", "K"));

        // CPI: typically released 10th-15th of each month, 8:30am ET
        LocalDate cpiDay = today.withDayOfMonth(12);
        if (cpiDay.isBefore(today)) cpiDay = today.plusMonths(1).withDayOfMonth(12);
        // Shift to business day if weekend
        while (cpiDay.getDayOfWeek().getValue() > 5) cpiDay = cpiDay.plusDays(1);
        out.add(new EconomicEvent(cpiDay.toString(),
                cpiDay.format(DateTimeFormatter.ofPattern("MMM d")) + ", 8:30 AM",
                "CPI (Consumer Price Index)", "", "", "", "%"));

        // FOMC: 8 meetings per year — hardcoded 2026 dates
        String[] fomc2026 = {"2026-01-28","2026-03-18","2026-04-29","2026-06-10",
                             "2026-07-29","2026-09-16","2026-10-28","2026-12-09"};
        for (String d : fomc2026) {
            LocalDate fd = LocalDate.parse(d);
            if (!fd.isBefore(today)) {
                out.add(new EconomicEvent(d,
                        fd.format(DateTimeFormatter.ofPattern("MMM d")) + ", 2:00 PM",
                        "FOMC Rate Decision", "", "", "", ""));
                break; // only next upcoming
            }
        }

        // Initial Jobless Claims: every Thursday 8:30am ET
        LocalDate nextThursday = today;
        while (nextThursday.getDayOfWeek().getValue() != 4) nextThursday = nextThursday.plusDays(1);
        if (nextThursday.equals(today)) nextThursday = nextThursday.plusDays(7);
        out.add(new EconomicEvent(nextThursday.toString(),
                nextThursday.format(DateTimeFormatter.ofPattern("MMM d")) + ", 8:30 AM",
                "Initial Jobless Claims", "", "", "", "K"));

        out.sort(Comparator.comparing(EconomicEvent::date));
        return out.subList(0, Math.min(out.size(), 5));
    }

    private LocalDate firstFridayOfMonth(LocalDate anchor) {
        LocalDate d = anchor.withDayOfMonth(1);
        while (d.getDayOfWeek().getValue() != 5) d = d.plusDays(1);
        return d;
    }

    private String utcToEt(String date, String time) {
        try {
            ZonedDateTime utc = LocalDateTime.parse(date + "T" + time).atZone(ZoneOffset.UTC);
            return utc.withZoneSameInstant(ET)
                    .format(DateTimeFormatter.ofPattern("MMM d, h:mm a"));
        } catch (Exception e) {
            return date + " " + time;
        }
    }

    // ── Breaking news ─────────────────────────────────────────────────────────

    private List<NewsItem> fetchNews() {
        try {
            String url = finnhubBaseUrl + "/news?category=general&token=" + finnhubKey;
            HttpResponse<String> res = http.send(
                    HttpRequest.newBuilder().uri(URI.create(url))
                            .timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return List.of();
            JsonNode arr = mapper.readTree(res.body());
            if (!arr.isArray()) return List.of();
            long now     = Instant.now().getEpochSecond();
            long cutoff  = now - 24 * 3600L; // 24h window — keeps important stories from earlier today
            List<NewsItem> out = new ArrayList<>();
            Set<String> seen = new HashSet<>(); // dedupe by headline prefix
            for (JsonNode n : arr) {
                long dt = n.path("datetime").asLong(0);
                if (dt < cutoff) continue;
                String headline = n.path("headline").asText("").trim();
                if (headline.isEmpty()) continue;
                String dedupeKey = headline.toLowerCase().replaceAll("[^a-z0-9]", "")
                        .substring(0, Math.min(40, headline.length()));
                if (!seen.add(dedupeKey)) continue;
                int sc = scoreText(headline + " " + n.path("summary").asText(""));
                if (sc < 6) continue; // threshold: 6+ = geopolitical/macro/regulatory
                long ageS = now - dt;
                String age = ageS < 3600 ? (ageS / 60) + "m ago" : (ageS / 3600) + "h ago";
                out.add(new NewsItem(headline, n.path("source").asText(""),
                        n.path("url").asText(""), sc, age));
            }
            // Best score first, then most recent
            out.sort((a, b) -> b.score() != a.score()
                    ? Integer.compare(b.score(), a.score()) : 0);
            return out.subList(0, Math.min(out.size(), 6));
        } catch (Exception e) {
            log.warn("Market news error: {}", e.getMessage());
            return List.of();
        }
    }

    private int scoreText(String text) {
        String lower = text.toLowerCase();
        int max = 0;
        for (int i = 0; i < KW_TERMS.length && i < KW_SCORES.length; i++) {
            if (lower.contains(KW_TERMS[i]) && KW_SCORES[i] > max) max = KW_SCORES[i];
        }
        return max;
    }
}
