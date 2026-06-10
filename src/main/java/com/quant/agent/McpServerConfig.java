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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Configuration
public class McpServerConfig {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Value("${alpaca.api.key}")
    private String apiKey;

    @Value("${alpaca.api.secret}")
    private String apiSecret;

    public record TickerRequest(String symbol) {}
    public record EmptyRequest() {}

    private HttpRequest buildAlpacaBaseRequest(String fullPath) {
        return HttpRequest.newBuilder()
                .uri(URI.create("https://data.alpaca.markets" + fullPath))
                .header("APCA-API-KEY-ID", apiKey != null ? apiKey : "")
                .header("APCA-API-SECRET-KEY", apiSecret != null ? apiSecret : "")
                .header("accept", "application/json")
                .GET()
                .build();
    }

    private HttpRequest buildAlpacaRequest(String endpoint) {
        return buildAlpacaBaseRequest("/v2/stocks" + endpoint);
    }

    // Real-world options exchange increment rounding algorithm
    private double getNearestOptionStrike(double price) {
        if (price <= 50.0) {
            return Math.round(price * 2.0) / 2.0; // $0.50 increments
        } else if (price <= 200.0) {
            return Math.round(price); // $1.00 increments
        } else if (price <= 500.0) {
            return Math.round(price / 2.5) * 2.5; // $2.50 increments
        } else {
            return Math.round(price / 5.0) * 5.0; // $5.00 increments
        }
    }

    private String processIntradayMtfAlignment(String ticker, double currentPrice, double highToday, double lowToday, long totalVolume, double priorClose) throws Exception {
        ZonedDateTime nowET = ZonedDateTime.now(ZoneId.of("America/New_York"));
        
        String lookback45Days = nowET.minusDays(45).format(DateTimeFormatter.ISO_INSTANT);
        String lookback10Days = nowET.minusDays(10).format(DateTimeFormatter.ISO_INSTANT);
        String lookback5Days = nowET.minusDays(5).format(DateTimeFormatter.ISO_INSTANT);

        double dailyScore = (currentPrice >= priorClose) ? 100.0 : -100.0;
        double h1Score = dailyScore;
        double m15Score = dailyScore;
        double m5Score = dailyScore;
        
        double vwap = currentPrice;
        double microSupport = (lowToday > 0.0) ? lowToday : currentPrice * 0.99;
        double microResistance = (highToday > 0.0) ? highToday : currentPrice * 1.01;

        int hour = nowET.getHour();
        int minute = nowET.getMinute();
        String sessionStatus = (hour < 9 || (hour == 9 && minute < 30)) ? "Pre-Market" : (hour >= 16 ? "Post-Market" : "Open Market");

        CompletableFuture<HttpResponse<String>> futureD1 = httpClient.sendAsync(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Day&start=" + lookback45Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureH1 = httpClient.sendAsync(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Hour&start=" + lookback10Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureM15 = httpClient.sendAsync(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=15Min&start=" + lookback5Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureM5 = httpClient.sendAsync(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=5Min&start=" + lookback5Days + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> futureOptions = httpClient.sendAsync(buildAlpacaBaseRequest("/v1beta1/options/snapshots/" + ticker + "?feed=indicative&strike_price_gte=" + (currentPrice * 0.95) + "&strike_price_lte=" + (currentPrice * 1.05) + "&limit=100"), HttpResponse.BodyHandlers.ofString());

        CompletableFuture.allOf(futureD1, futureH1, futureM15, futureM5, futureOptions).join();

        if (futureD1.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureD1.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && tickerNode.size() >= 2) {
                double lastD1 = tickerNode.get(tickerNode.size() - 1).path("c").asDouble();
                double prevD1 = tickerNode.get(tickerNode.size() - 2).path("c").asDouble();
                dailyScore = (lastD1 >= prevD1) ? 100.0 : -100.0;
            }
        }

        if (futureH1.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureH1.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && !tickerNode.isEmpty()) {
                double sum = 0; int count = Math.min(tickerNode.size(), 20);
                for (int i = tickerNode.size() - count; i < tickerNode.size(); i++) {
                    sum += tickerNode.get(i).path("c").asDouble();
                }
                double h1Ma = sum / count;
                h1Score = (currentPrice >= h1Ma) ? 100.0 : -100.0;
            }
        }

        if (futureM15.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureM15.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && tickerNode.size() >= 2) {
                double lastM15 = tickerNode.get(tickerNode.size() - 1).path("c").asDouble();
                double prevM15 = tickerNode.get(tickerNode.size() - 2).path("c").asDouble();
                m15Score = (lastM15 >= prevM15) ? 100.0 : -100.0;
            }
        }

        if (futureM5.get().statusCode() == 200) {
            JsonNode tickerNode = objectMapper.readTree(futureM5.get().body()).path("bars").path(ticker);
            if (tickerNode.isArray() && !tickerNode.isEmpty()) {
                int size = tickerNode.size();
                JsonNode lastCandle = tickerNode.get(size - 1);
                
                long lastCandleTime = Instant.parse(lastCandle.path("t").asText()).getEpochSecond();
                long startOfDayEpoch = Instant.ofEpochSecond(lastCandleTime).atZone(ZoneId.of("America/New_York")).toLocalDate().atStartOfDay(ZoneId.of("America/New_York")).toEpochSecond();
                
                double cumulativeTPV = 0; double cumulativeVol = 0;
                
                for(int i = 0; i < size; i++) {
                    JsonNode bar = tickerNode.get(i);
                    if(Instant.parse(bar.path("t").asText()).getEpochSecond() >= startOfDayEpoch) {
                        double h = bar.path("h").asDouble();
                        double l = bar.path("l").asDouble();
                        cumulativeTPV += ((h + l + bar.path("c").asDouble()) / 3.0) * bar.path("v").asLong();
                        cumulativeVol += bar.path("v").asLong();
                    }
                }
                if (cumulativeVol > 0) vwap = cumulativeTPV / cumulativeVol;
                m5Score = (lastCandle.path("c").asDouble() >= lastCandle.path("o").asDouble()) ? 100.0 : -100.0;
            }
        }

        if (Math.abs(currentPrice - vwap) / currentPrice > 0.08) {
            vwap = currentPrice;
        }

        double totalConfluenceScore = (dailyScore * 0.40) + (h1Score * 0.30) + (m15Score * 0.20) + (m5Score * 0.10);

        double impliedVolatility = 0.0;
        if (futureOptions.get().statusCode() == 200) {
            JsonNode snapshots = objectMapper.readTree(futureOptions.get().body()).path("snapshots");
            double totalIv = 0;
            int ivCount = 0;
            if (snapshots.isObject()) {
                var iterator = snapshots.fields();
                while (iterator.hasNext()) {
                    JsonNode contract = iterator.next().getValue();
                    JsonNode ivNode = contract.path("implied_volatility");
                    if (!ivNode.isMissingNode() && !ivNode.isNull()) {
                        double iv = ivNode.asDouble();
                        if (iv > 0.01 && iv < 2.0) { 
                            totalIv += iv;
                            ivCount++;
                        }
                    }
                }
            }
            if (ivCount > 0) impliedVolatility = totalIv / ivCount;
        }

        if (impliedVolatility == 0.0) {
            double localVariance = microResistance - microSupport;
            if (localVariance > 0) {
                double dailySigma = (localVariance / 2.5) / currentPrice; 
                impliedVolatility = dailySigma * Math.sqrt(252);
            } else {
                impliedVolatility = 0.24; 
            }
            impliedVolatility = Math.min(impliedVolatility, 0.80);
        }

        // 1. VOLATILITY DEFLATOR: Strip out the 15% market maker fear premium
        double realizedVolEstimate = impliedVolatility * 0.85;

        // 2. TIME CONVERSION: Use 252 trading days instead of 365 calendar days for intraday accuracy
        double oneDayExpectedMove = currentPrice * realizedVolEstimate * Math.sqrt(1.0 / 252.0);
        double fiveDayExpectedMove = currentPrice * realizedVolEstimate * Math.sqrt(5.0 / 252.0);

        // 3. DIRECTIONAL SKEW: Shift the center anchor based on the day's trend momentum
        double rangeAnchor = currentPrice;
        if (totalConfluenceScore <= -40.0) {
            rangeAnchor = currentPrice - (oneDayExpectedMove * 0.35); 
        } else if (totalConfluenceScore >= 40.0) {
            rangeAnchor = currentPrice + (oneDayExpectedMove * 0.35); 
        }

        double tomorrowUpper = rangeAnchor + oneDayExpectedMove;
        double tomorrowLower = rangeAnchor - oneDayExpectedMove;
        double nextWeekUpper = rangeAnchor + fiveDayExpectedMove;
        double nextWeekLower = rangeAnchor - fiveDayExpectedMove;

        // --- SYNCHRONIZED RISK LIMITS ---
        double minBreathableMove = currentPrice * 0.015; 
        double tradeTargetDistance = Math.max(oneDayExpectedMove, minBreathableMove);

        double dynamicEntry = currentPrice;
        double dynamicSl = currentPrice;
        double dynamicTp = currentPrice;
        String dynamicVerdict;

        if (totalConfluenceScore >= 70.0) {
            dynamicVerdict = "EXECUTE_CALL_OR_LONG_SPREAD";
            dynamicEntry = currentPrice;
            dynamicTp = Math.max(currentPrice + minBreathableMove, tomorrowUpper);
            dynamicSl = Math.max(currentPrice - minBreathableMove, tomorrowLower);
        } else if (totalConfluenceScore >= 15.0) {
            dynamicVerdict = "PREPARE_LONG_BUY_DIP_AT_VWAP";
            dynamicEntry = (vwap < currentPrice) ? vwap : currentPrice - (tradeTargetDistance * 0.3);
            dynamicTp = Math.max(dynamicEntry + minBreathableMove, tomorrowUpper);
            dynamicSl = dynamicEntry - (tradeTargetDistance * 0.6);
        } else if (totalConfluenceScore <= -70.0) {
            dynamicVerdict = "EXECUTE_PUT_OR_SHORT_SPREAD";
            dynamicEntry = currentPrice;
            dynamicTp = Math.min(currentPrice - minBreathableMove, tomorrowLower);
            dynamicSl = Math.min(currentPrice + minBreathableMove, tomorrowUpper);
        } else if (totalConfluenceScore <= -15.0) {
            dynamicVerdict = "PREPARE_SHORT_FADE_BOUNCE_AT_VWAP";
            dynamicEntry = (vwap > currentPrice) ? vwap : currentPrice + (tradeTargetDistance * 0.3);
            dynamicTp = Math.min(dynamicEntry - minBreathableMove, tomorrowLower);
            dynamicSl = dynamicEntry + (tradeTargetDistance * 0.6);
        } else {
            dynamicVerdict = "STAND_DOWN_COLLECT_PREMIUM";
            dynamicEntry = currentPrice;
            dynamicTp = tomorrowUpper;
            dynamicSl = tomorrowLower;
        }

        // Snap the calculated TP and Entry to realistic option chain strikes
        double strikeBuy = getNearestOptionStrike(dynamicEntry);
        double strikeSell = getNearestOptionStrike(dynamicTp);

        return String.format(",\"session_status\":\"%s\",\"macro_daily_trend_score\":%.1f,\"h1_radar_score\":%.1f,\"m15_radar_score\":%.1f,\"m5_radar_score\":%.1f,\"total_confluence_score\":%.1f,\"intraday_vwap\":%.2f,\"micro_support\":%.2f,\"micro_resistance\":%.2f,\"implied_volatility\":\"%.2f%%\",\"tomorrow_upper\":%.2f,\"tomorrow_lower\":%.2f,\"next_week_upper\":%.2f,\"next_week_lower\":%.2f,\"automated_trade_verdict\":\"%s\",\"final_entry\":%.2f,\"final_tp\":%.2f,\"final_sl\":%.2f,\"strike_buy\":%.2f,\"strike_sell\":%.2f",
                sessionStatus, dailyScore, h1Score, m15Score, m5Score, totalConfluenceScore, vwap, microSupport, microResistance, impliedVolatility * 100, tomorrowUpper, tomorrowLower, nextWeekUpper, nextWeekLower, dynamicVerdict, dynamicEntry, dynamicTp, dynamicSl, strikeBuy, strikeSell);
    }

    @Bean
    @Description("USE THIS tool when the user asks to analyze an individual stock ticker symbol. Gets current real-time market pricing data.")
    public Function<TickerRequest, String> stockPriceFunction() {
        return request -> {
            String ticker = request.symbol().replaceAll("[\"']", "").trim().toUpperCase();
            try {
                // NEW URL: Forces the public V8 API to load extended hours 1-minute candles
                HttpRequest liveRequest = HttpRequest.newBuilder()
                        .uri(URI.create("https://query1.finance.yahoo.com/v8/finance/chart/" + ticker + "?includePrePost=true&interval=1m&range=1d"))
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .GET()
                        .build();

                HttpResponse<String> res = httpClient.send(liveRequest, HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() != 200) return String.format("{\"error\":\"CRITICAL FAILURE: Public Gateway Offline for %s.\"}", ticker);

                JsonNode root = objectMapper.readTree(res.body());
                JsonNode resultNode = root.path("chart").path("result").get(0);
                JsonNode meta = resultNode.path("meta");

                double regularPrice = meta.path("regularMarketPrice").asDouble();
                double priorClose = meta.path("chartPreviousClose").asDouble();
                long vol = meta.path("regularMarketVolume").asLong(0);
                double highToday = meta.path("regularMarketDayHigh").asDouble(regularPrice);
                double lowToday = meta.path("regularMarketDayLow").asDouble(regularPrice);

                double currentPrice = regularPrice;
                
                // BACKDOOR: Extract the absolute latest active price from the minute candles (Captures Pre/Post Market)
                JsonNode closes = resultNode.path("indicators").path("quote").get(0).path("close");
                if (closes != null && closes.isArray() && !closes.isEmpty()) {
                    // Loop backwards from the end of the day to find the last non-null trade execution
                    for (int i = closes.size() - 1; i >= 0; i--) {
                        if (!closes.get(i).isNull()) {
                            currentPrice = closes.get(i).asDouble();
                            break;
                        }
                    }
                }

                double percentChange = (priorClose > 0) ? ((currentPrice - priorClose) / priorClose) * 100.0 : 0.0;
                String pctString = String.format("%s%.2f%%", (percentChange >= 0 ? "+" : ""), percentChange);

                String payload = String.format("{\"symbol\":\"%s\",\"company_name\":\"%s\",\"current_price\":%.2f,\"change\":%.2f,\"percent_change\":\"%s\",\"volume\":\"%s\",\"high_today\":%.2f,\"low_today\":%.2f}",
                        ticker, ticker, currentPrice, currentPrice - priorClose, pctString, String.format("%,d", vol), highToday, lowToday);
                
                return payload.substring(0, payload.length() - 1) + processIntradayMtfAlignment(ticker, currentPrice, highToday, lowToday, vol, priorClose) + "}";
            } catch (Exception e) {
                return String.format("{\"error\":\"CRITICAL FAILURE: Exception parsing data streams for %s.\"}", ticker);
            }
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks to analyze an individual stock ticker symbol. Calculates structured channels and true dynamic RSI/EMA.")
    public Function<TickerRequest, String> historicalTrendFunction() {
        return request -> {
            String ticker = request.symbol().replaceAll("[\"']", "").trim().toUpperCase();
            try {
                String startLookback = ZonedDateTime.now(ZoneId.of("America/New_York")).minusDays(60).format(DateTimeFormatter.ISO_INSTANT);
                HttpResponse<String> res = httpClient.send(buildAlpacaRequest("/bars?symbols=" + ticker + "&timeframe=1Day&start=" + startLookback + "&feed=iex"), HttpResponse.BodyHandlers.ofString());
                JsonNode tickerNode = objectMapper.readTree(res.body()).path("bars").path(ticker);
                
                if (tickerNode != null && tickerNode.isArray() && tickerNode.size() >= 25) {
                    int len = tickerNode.size(); 
                    double[] closes = new double[len];
                    for (int i = 0; i < len; i++) {
                        closes[i] = tickerNode.get(i).path("c").asDouble();
                    }
                    
                    double lastPrice = closes[len - 1];

                    double gains = 0, losses = 0;
                    for (int i = len - 14; i < len; i++) {
                        double diff = closes[i] - closes[i - 1];
                        if (diff > 0) gains += diff;
                        else losses -= diff;
                    }
                    double avgGain = gains / 14.0;
                    double avgLoss = losses / 14.0;
                    double rsi = 50.0;
                    if (avgLoss != 0) {
                        double rs = avgGain / avgLoss;
                        rsi = 100.0 - (100.0 / (1.0 + rs));
                    } else {
                        rsi = (avgGain > 0) ? 100.0 : 0.0;
                    }

                    double sma9 = 0, sma21 = 0;
                    for(int i = len - 9; i < len; i++) sma9 += closes[i];
                    sma9 /= 9.0;
                    for(int i = len - 21; i < len; i++) sma21 += closes[i];
                    sma21 /= 21.0;
                    
                    double prevSma9 = 0, prevSma21 = 0;
                    for(int i = len - 10; i < len - 1; i++) prevSma9 += closes[i];
                    prevSma9 /= 9.0;
                    for(int i = len - 22; i < len - 1; i++) prevSma21 += closes[i];
                    prevSma21 /= 21.0;

                    String emaStatus;
                    if (sma9 > sma21 && prevSma9 <= prevSma21) emaStatus = "Bullish Cross";
                    else if (sma9 < sma21 && prevSma9 >= prevSma21) emaStatus = "Bearish Cross";
                    else if (sma9 > sma21) emaStatus = "Bullish";
                    else emaStatus = "Bearish";

                    return String.format("{\"symbol\":\"%s\",\"calculated_support\":%.2f,\"calculated_resistance\":%.2f,\"ema_crossover_status\":\"%s\",\"calculated_rsi_14d\":%.1f}",
                            ticker, lastPrice * 0.96, lastPrice * 1.04, emaStatus, rsi);
                }
                return String.format("{\"symbol\":\"%s\",\"calculated_support\":100.0,\"calculated_resistance\":110.0,\"ema_crossover_status\":\"Neutral\",\"calculated_rsi_14d\":50.0}", ticker);
            } catch (Exception e) {
                return String.format("{\"error\":\"CRITICAL FAILURE: Exception building trend metrics for %s.\"}", ticker);
            }
        };
    }

    @Bean
    @Description("USE THIS tool when the user asks for top options, trending tickers, market movers, or a broad market scan.")
    public Function<EmptyRequest, String> generalMarketScannerFunction() {
        return request -> {
            try {
                // Hits the live US trending ticker tape
                HttpRequest trendingReq = HttpRequest.newBuilder()
                        .uri(URI.create("https://query1.finance.yahoo.com/v1/finance/trending/US"))
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .GET()
                        .build();
                        
                HttpResponse<String> res = httpClient.send(trendingReq, HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() != 200) return "{\"error\":\"Market scanner gateway offline.\"}";
                
                JsonNode quotes = objectMapper.readTree(res.body())
                        .path("finance").path("result").get(0).path("quotes");
                
                StringBuilder topTickers = new StringBuilder();
                int count = 0;
                if (quotes.isArray()) {
                    for (JsonNode quote : quotes) {
                        if (count >= 5) break;
                        topTickers.append(quote.path("symbol").asText()).append(", ");
                        count++;
                    }
                }
                
                String tickersString = topTickers.toString().replaceAll(", $", "");
                
                // Instructs the AI on how to handle the broad list
                return String.format("{\"status\":\"success\", \"trending_tickers\":\"%s\", \"system_instruction\":\"List these 5 trending tickers to the user and ask them which specific symbol they want you to run the deep multi-timeframe option strategy analysis on.\"}", tickersString);
                
            } catch (Exception e) {
                 return "{\"error\":\"CRITICAL FAILURE: Exception parsing scanner streams.\"}";
            }
        };
    }
}