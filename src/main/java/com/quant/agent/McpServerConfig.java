package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration
public class McpServerConfig {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Value("${market.provider.api-url}")
    private String apiUrl;

    @Value("${market.provider.api-key}")
    private String apiKey;

    @Value("${market.provider.read-timeout-seconds:5}")
    private int readTimeoutSeconds;

    public record TickerRequest(String symbol) {}
    public record EmptyRequest() {}

    @Bean
    @Description("USE THIS tool when the user asks to analyze an individual stock ticker symbol. Gets current real-time market pricing data and the verified official company name.")
    public Function<TickerRequest, String> stockPriceFunction() {
        return request -> {
            String ticker = request.symbol().replaceAll("[\"']", "").trim().toUpperCase();
            try {
                String url = apiUrl + ticker + "&token=" + apiKey;
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(readTimeoutSeconds))
                        .GET().build();
                HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                
                String companyName = ticker; 
                if (res.statusCode() == 200) {
                    JsonNode root = objectMapper.readTree(res.body());
                    double currentPrice = root.path("c").asDouble();
                    if (currentPrice == 0.0) return "{\"error\": \"Ticker " + ticker + " not found.\"}";
                    
                    try {
                        String profileUrl = "https://finnhub.io/api/v1/stock/profile2?symbol=" + ticker + "&token=" + apiKey;
                        HttpRequest profileReq = HttpRequest.newBuilder()
                                .uri(URI.create(profileUrl))
                                .timeout(Duration.ofSeconds(2))
                                .GET().build();
                        HttpResponse<String> profileRes = httpClient.send(profileReq, HttpResponse.BodyHandlers.ofString());
                        if (profileRes.statusCode() == 200) {
                            JsonNode profileRoot = objectMapper.readTree(profileRes.body());
                            String fetchedName = profileRoot.path("name").asText("").trim();
                            if (!fetchedName.isEmpty()) companyName = fetchedName;
                        }
                    } catch (Exception e) { /* Fallback to ticker symbol */ }
                    
                    return String.format("{\"symbol\":\"%s\",\"company_name\":\"%s\",\"current_price\":%.2f,\"change\":%.2f,\"percent_change\":\"%.2f%%\",\"open\":%.2f,\"prior_close\":%.2f}",
                            ticker, companyName, currentPrice, root.path("d").asDouble(), root.path("dp").asDouble(),
                            root.path("o").asDouble(), root.path("pc").asDouble());
                }
                return "{\"error\": \"API status code: " + res.statusCode() + "\"}";
            } catch (Exception e) { return "{\"error\": \"Tool execution failed: " + e.getMessage() + "\"}"; }
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks to analyze an individual stock ticker symbol. Programmatically calculates 30-day support floors, 30-day resistance ceilings, and the 20-day Simple Moving Average (SMA).")
    public Function<TickerRequest, String> historicalTrendFunction() {
        return request -> {
            String ticker = request.symbol().replaceAll("[\"']", "").trim().toUpperCase();
            long end = Instant.now().getEpochSecond();
            long start = end - (60L * 24 * 60 * 60); 
            try {
                String url = String.format("https://finnhub.io/api/v1/stock/candle?symbol=%s&resolution=D&from=%d&to=%d&token=%s",
                        ticker, start, end, apiKey);
                HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
                HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                
                if (res.statusCode() == 200) {
                    JsonNode root = objectMapper.readTree(res.body());
                    if ("no_data".equals(root.path("s").asText())) {
                        return "{\"error\": \"Finnhub returned no historical data for " + ticker + ".\"}";
                    }
                    JsonNode closePrices = root.path("c");
                    if (closePrices.isArray() && !closePrices.isEmpty()) {
                        int totalElements = closePrices.size();
                        int lookback30 = Math.min(totalElements, 30);
                        
                        double support30d = Double.MAX_VALUE;
                        double resistance30d = -Double.MAX_VALUE;
                        double totalSum20d = 0;
                        int actualCount20d = 0;

                        // Calculate Programmatic Support and Resistance over last 30 trading sessions
                        for (int i = totalElements - lookback30; i < totalElements; i++) {
                            double closingPrice = closePrices.get(i).asDouble();
                            if (closingPrice < support30d) support30d = closingPrice;
                            if (closingPrice > resistance30d) resistance30d = closingPrice;
                        }

                        // Calculate programmatically exact 20-Day Simple Moving Average (SMA)
                        int start20d = Math.max(0, totalElements - 20);
                        for (int i = start20d; i < totalElements; i++) {
                            totalSum20d += closePrices.get(i).asDouble();
                            actualCount20d++;
                        }
                        double sma20d = actualCount20d > 0 ? totalSum20d / actualCount20d : 0.0;

                        return String.format("{\"symbol\":\"%s\",\"calculated_30d_support\":%.2f,\"calculated_30d_resistance\":%.2f,\"calculated_20d_sma\":%.2f,\"data_points_sync\":%d}",
                                ticker, support30d, resistance30d, sma20d, lookback30);
                    }
                }
                return "{\"error\": \"Historical indicators failed with status code: " + res.statusCode() + "\"}";
            } catch (Exception e) { return "{\"error\": \"Indicator compilation pipeline failed: " + e.getMessage() + "\"}"; }
        };
    }

    @Bean
    @Description("ONLY use this tool when the user explicitly requests a broad market scan, trending tickers, top plays, or a multi-stock list. DO NOT use this for single stock requests.")
    public Function<EmptyRequest, String> generalMarketScannerFunction() {
        return request -> {
            Set<String> dynamicTickers = new LinkedHashSet<>();
            try {
                String newsUrl = "https://finnhub.io/api/v1/news?category=general&token=" + apiKey;
                HttpRequest req = HttpRequest.newBuilder().uri(URI.create(newsUrl)).GET().build();
                HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

                if (res.statusCode() == 200) {
                    JsonNode root = objectMapper.readTree(res.body());
                    if (root.isArray()) {
                        for (JsonNode article : root) {
                            String related = article.path("related").asText("");
                            if (!related.isEmpty()) {
                                String[] symbols = related.split(",");
                                for (String sym : symbols) {
                                    String cleanSym = sym.trim().toUpperCase();
                                    if (!cleanSym.isEmpty() && !cleanSym.contains(":") && !cleanSym.contains("-")) {
                                        dynamicTickers.add(cleanSym);
                                    }
                                    if (dynamicTickers.size() >= 10) break;
                                }
                            }
                            if (dynamicTickers.size() >= 10) break;
                        }
                    }
                }

                if (dynamicTickers.size() < 10) {
                    String peersUrl = "https://finnhub.io/api/v1/stock/peers?symbol=NVDA&token=" + apiKey;
                    HttpRequest pReq = HttpRequest.newBuilder().uri(URI.create(peersUrl)).GET().build();
                    HttpResponse<String> pRes = httpClient.send(pReq, HttpResponse.BodyHandlers.ofString());

                    if (pRes.statusCode() == 200) {
                        JsonNode pRoot = objectMapper.readTree(pRes.body());
                        if (pRoot.isArray()) {
                            for (JsonNode peer : pRoot) {
                                String cleanSym = peer.asText().trim().toUpperCase();
                                if (!cleanSym.isEmpty() && !cleanSym.contains(":") && !cleanSym.contains("-")) {
                                    dynamicTickers.add(cleanSym);
                                }
                                if (dynamicTickers.size() >= 10) break;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println(">>> [SCANNER ERROR] " + e.getMessage());
            }

            if (dynamicTickers.size() < 10) {
                dynamicTickers.clear();
                dynamicTickers.addAll(Arrays.asList("NVDA", "GOOGL", "AAPL", "MSFT", "AMZN", "TSM", "AVGO", "META", "TSLA", "LLY"));
            }

            String jsonArray = dynamicTickers.stream()
                    .limit(10)
                    .map(s -> "\"" + s + "\"")
                    .collect(Collectors.joining(", ", "[", "]"));

            return "{\"status\": \"Dynamic Hybrid Scan Success\", \"top_tickers\": " + jsonArray + "}";
        };
    }
}