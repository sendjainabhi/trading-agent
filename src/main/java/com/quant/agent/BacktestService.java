package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class BacktestService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @Value("${alpaca.api.key}")
    private String apiKey;

    @Value("${alpaca.api.secret}")
    private String apiSecret;

    public record TradeRecord(String entryDate, String exitDate, double entryPrice, double exitPrice,
                               double returnPct, boolean isLong) {}

    public record BacktestResult(String symbol, int lookbackDays, int holdDays, double threshold,
                                  int totalTrades, int wins, double winRate, double avgReturnPct,
                                  double totalReturnPct, double maxDrawdownPct, double sharpeRatio,
                                  List<TradeRecord> trades) {}

    public BacktestResult run(String symbol, int lookbackDays, int holdDays, double threshold) throws Exception {
        ZonedDateTime nowET = ZonedDateTime.now(ZoneId.of("America/New_York"));
        // Fetch extra bars for indicator warmup (need 30 bars before test period starts)
        String startDate = nowET.minusDays(lookbackDays + 60L).format(DateTimeFormatter.ISO_INSTANT);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("https://data.alpaca.markets/v2/stocks/bars?symbols=" + symbol
                        + "&timeframe=1Day&start=" + startDate + "&feed=iex"))
                .header("APCA-API-KEY-ID", apiKey)
                .header("APCA-API-SECRET-KEY", apiSecret)
                .header("accept", "application/json")
                .GET().build();

        HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) throw new RuntimeException("Alpaca returned " + res.statusCode());

        JsonNode barsNode = objectMapper.readTree(res.body()).path("bars").path(symbol);
        if (!barsNode.isArray() || barsNode.size() < 35)
            throw new RuntimeException("Insufficient history for " + symbol + " (need 35+ daily bars)");

        int len = barsNode.size();
        double[] closes = new double[len];
        double[] opens  = new double[len];
        double[] highs  = new double[len];
        double[] lows   = new double[len];
        String[] dates  = new String[len];

        for (int i = 0; i < len; i++) {
            JsonNode b = barsNode.get(i);
            closes[i] = b.path("c").asDouble();
            opens[i]  = b.path("o").asDouble();
            highs[i]  = b.path("h").asDouble();
            lows[i]   = b.path("l").asDouble();
            dates[i]  = b.path("t").asText("").substring(0, 10);
        }

        List<TradeRecord> trades = new ArrayList<>();
        boolean inTrade = false;
        boolean isLong = true;
        double entryPrice = 0;
        String entryDate = "";
        int daysHeld = 0;
        double equity = 1.0;
        double peak = 1.0;
        double maxDrawdown = 0.0;
        int warmup = 30;

        for (int i = warmup; i < len - 1; i++) {
            double score = computeDailyScore(closes, highs, lows, i);

            if (inTrade) {
                daysHeld++;
                boolean reversal = isLong ? score < -threshold : score > threshold;
                if (daysHeld >= holdDays || reversal) {
                    double exitPrice = opens[i + 1];
                    double ret = isLong
                            ? (exitPrice - entryPrice) / entryPrice * 100.0
                            : (entryPrice - exitPrice) / entryPrice * 100.0;
                    ret -= 0.10; // 10 bps slippage per side
                    trades.add(new TradeRecord(entryDate, dates[i + 1], entryPrice, exitPrice, ret, isLong));
                    equity *= (1.0 + ret / 100.0);
                    if (equity > peak) peak = equity;
                    double dd = (peak - equity) / peak * 100.0;
                    if (dd > maxDrawdown) maxDrawdown = dd;
                    inTrade = false;
                }
            } else {
                if (score >= threshold) {
                    inTrade = true; isLong = true;
                    entryPrice = opens[i + 1]; entryDate = dates[i + 1]; daysHeld = 0;
                } else if (score <= -threshold) {
                    inTrade = true; isLong = false;
                    entryPrice = opens[i + 1]; entryDate = dates[i + 1]; daysHeld = 0;
                }
            }
        }

        int wins = (int) trades.stream().filter(t -> t.returnPct() > 0).count();
        double winRate    = trades.isEmpty() ? 0 : (double) wins / trades.size() * 100.0;
        double avgReturn  = trades.stream().mapToDouble(TradeRecord::returnPct).average().orElse(0);
        double totalReturn = trades.stream()
                .mapToDouble(t -> Math.log1p(t.returnPct() / 100.0)).sum();
        totalReturn = (Math.exp(totalReturn) - 1.0) * 100.0;

        double[] rets = trades.stream().mapToDouble(TradeRecord::returnPct).toArray();
        double variance = 0;
        for (double r : rets) variance += (r - avgReturn) * (r - avgReturn);
        double stdDev  = rets.length > 1 ? Math.sqrt(variance / (rets.length - 1)) : 1.0;
        double sharpe  = stdDev > 0 ? (avgReturn / stdDev) * Math.sqrt(252.0 / holdDays) : 0;

        return new BacktestResult(symbol, lookbackDays, holdDays, threshold,
                trades.size(), wins, winRate, avgReturn, totalReturn, maxDrawdown, sharpe, trades);
    }

    // Walk-forward daily score: SMA20 + RSI14, only uses data available up to bar index i
    private double computeDailyScore(double[] closes, double[] highs, double[] lows, int i) {
        if (i < 20) return 0.0;

        double sma20 = 0;
        for (int j = i - 19; j <= i; j++) sma20 += closes[j];
        sma20 /= 20.0;

        double gains = 0, losses = 0;
        for (int j = i - 13; j <= i; j++) {
            double diff = closes[j] - closes[j - 1];
            if (diff > 0) gains += diff; else losses -= diff;
        }
        double avgGain = gains / 14.0, avgLoss = losses / 14.0;
        double rsi = avgLoss < 0.0001 ? (avgGain > 0 ? 95.0 : 50.0) : 100.0 - (100.0 / (1.0 + avgGain / avgLoss));

        double priceVsSma = sma20 > 0 ? Math.max(-100.0, Math.min(100.0, (closes[i] - sma20) / sma20 * 2500.0)) : 0.0;
        double rsiSignal  = (rsi - 50.0) * 2.0;
        return (priceVsSma * 0.60) + (rsiSignal * 0.40);
    }
}
