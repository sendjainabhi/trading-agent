package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Pure static math helpers — no Spring, no I/O.
 * All methods operate on a JsonNode bar array fetched from Alpaca.
 */
public final class IndicatorUtils {

    private IndicatorUtils() {}

    // ── Moving averages ───────────────────────────────────────────────────────

    /** Simple Moving Average of the last {@code period} closing prices. */
    public static double calculateSmaFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period) return 0.0;
        double sum = 0;
        for (int i = len - period; i < len; i++) sum += bars.get(i).path("c").asDouble();
        return sum / period;
    }

    /** Exponential Moving Average — seeded from first bar, iterated over all bars. */
    public static double calculateEmaFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period) return len > 0 ? bars.get(len - 1).path("c").asDouble() : 0.0;
        double k = 2.0 / (period + 1.0);
        double ema = bars.get(0).path("c").asDouble();
        for (int i = 1; i < len; i++) ema = bars.get(i).path("c").asDouble() * k + ema * (1.0 - k);
        return ema;
    }

    /**
     * EMA computed over bars[0..len-excludeLastN-1] — used for "previous bar" EMA comparisons
     * without needing a full copy of the array.
     */
    public static double calculateEmaFromBarsOffset(JsonNode bars, int period, int excludeLastN) {
        int effectiveLen = bars.size() - excludeLastN;
        if (effectiveLen < period) return 0.0;
        double k = 2.0 / (period + 1.0);
        double ema = bars.get(0).path("c").asDouble();
        for (int i = 1; i < effectiveLen; i++)
            ema = bars.get(i).path("c").asDouble() * k + ema * (1.0 - k);
        return ema;
    }

    // ── Momentum ──────────────────────────────────────────────────────────────

    /**
     * RSI-14 with Wilder's exponential smoothing — matches TradingView / thinkorswim output.
     * Seeds with the first {@code period} changes as a simple average, then applies the
     * standard Wilder formula: avgGain = (prevAvgGain × (period-1) + gain) / period.
     */
    public static double calculateRsiFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period + 1) return 50.0;
        // Seed: simple average of first period gains/losses
        double avgGain = 0, avgLoss = 0;
        int seedEnd = Math.min(period, len - 1);
        for (int i = 1; i <= seedEnd; i++) {
            double diff = bars.get(i).path("c").asDouble() - bars.get(i - 1).path("c").asDouble();
            if (diff > 0) avgGain += diff; else avgLoss -= diff;
        }
        avgGain /= seedEnd;
        avgLoss /= seedEnd;
        // Wilder's smoothing over remaining bars
        for (int i = period + 1; i < len; i++) {
            double diff = bars.get(i).path("c").asDouble() - bars.get(i - 1).path("c").asDouble();
            double gain = diff > 0 ? diff : 0;
            double loss = diff < 0 ? -diff : 0;
            avgGain = (avgGain * (period - 1) + gain) / period;
            avgLoss = (avgLoss * (period - 1) + loss) / period;
        }
        if (avgLoss < 0.0001) return avgGain > 0 ? 95.0 : 50.0;
        return 100.0 - (100.0 / (1.0 + avgGain / avgLoss));
    }

    /**
     * RSI computed over bars[0..size-1-offset] with Wilder's smoothing — for comparing
     * current RSI vs RSI N bars ago to detect momentum divergence.
     */
    public static double calculateRsiAtOffset(JsonNode bars, int period, int offset) {
        int len = bars.size() - offset;
        if (len < period + 1) return 50.0;
        double avgGain = 0, avgLoss = 0;
        int seedEnd = Math.min(period, len - 1);
        for (int i = 1; i <= seedEnd; i++) {
            double diff = bars.get(i).path("c").asDouble() - bars.get(i - 1).path("c").asDouble();
            if (diff > 0) avgGain += diff; else avgLoss -= diff;
        }
        avgGain /= seedEnd;
        avgLoss /= seedEnd;
        for (int i = period + 1; i < len; i++) {
            double diff = bars.get(i).path("c").asDouble() - bars.get(i - 1).path("c").asDouble();
            double gain = diff > 0 ? diff : 0;
            double loss = diff < 0 ? -diff : 0;
            avgGain = (avgGain * (period - 1) + gain) / period;
            avgLoss = (avgLoss * (period - 1) + loss) / period;
        }
        if (avgLoss < 0.0001) return avgGain > 0 ? 95.0 : 50.0;
        return 100.0 - (100.0 / (1.0 + avgGain / avgLoss));
    }

    /**
     * MACD computed on closing prices. Returns double[]{macdLine, signalLine, histogram}.
     * Uses EMA12, EMA26 for MACD line and EMA9 of MACD line for signal.
     * Requires at least 35 bars for a reliable signal line.
     */
    public static double[] calculateMacdFromBars(JsonNode bars) {
        int len = bars.size();
        if (len < 27) return new double[]{0, 0, 0};
        double k12 = 2.0 / 13.0, k26 = 2.0 / 27.0, k9 = 2.0 / 10.0;
        double ema12 = bars.get(0).path("c").asDouble();
        double ema26 = bars.get(0).path("c").asDouble();
        // Build MACD history from bar 1 onward
        double[] macdHistory = new double[len];
        for (int i = 1; i < len; i++) {
            double c = bars.get(i).path("c").asDouble();
            ema12 = c * k12 + ema12 * (1 - k12);
            ema26 = c * k26 + ema26 * (1 - k26);
            macdHistory[i] = ema12 - ema26;
        }
        // Signal line = 9-period EMA of MACD, seeded from bar 26
        double signal = macdHistory[Math.min(26, len - 1)];
        for (int i = 27; i < len; i++) signal = macdHistory[i] * k9 + signal * (1 - k9);
        double macdLine = macdHistory[len - 1];
        return new double[]{macdLine, signal, macdLine - signal};
    }

    // ── Volatility ────────────────────────────────────────────────────────────

    /**
     * ATR with Wilder's exponential smoothing — matches standard charting platform output.
     * Seeds with the first {@code period} true ranges as a simple average, then applies
     * Wilder's formula: ATR = (prevATR × (period-1) + currentTR) / period.
     */
    public static double calculateAtrFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period + 1) return 0.0;
        // Seed with simple average of first period TRs
        double atr = 0;
        for (int i = 1; i <= period && i < len; i++) {
            double h = bars.get(i).path("h").asDouble();
            double l = bars.get(i).path("l").asDouble();
            double prevC = bars.get(i - 1).path("c").asDouble();
            atr += Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
        }
        atr /= period;
        // Wilder's smoothing over remaining bars
        for (int i = period + 1; i < len; i++) {
            double h = bars.get(i).path("h").asDouble();
            double l = bars.get(i).path("l").asDouble();
            double prevC = bars.get(i - 1).path("c").asDouble();
            double tr = Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
            atr = (atr * (period - 1) + tr) / period;
        }
        return atr;
    }

    /**
     * Bollinger Bands: returns double[]{upper, middle, lower, widthPct, stdDev}.
     * widthPct = (upper - lower) / middle × 100 — used to detect volatility squeezes.
     */
    public static double[] calculateBollingerBands(JsonNode bars, int period, double multiplier) {
        int len = bars.size();
        if (len < period) return new double[]{0, 0, 0, 0, 0};
        double sma = calculateSmaFromBars(bars, period);
        double sumSq = 0;
        for (int i = len - period; i < len; i++) {
            double diff = bars.get(i).path("c").asDouble() - sma;
            sumSq += diff * diff;
        }
        double std = Math.sqrt(sumSq / period);
        double upper = sma + multiplier * std;
        double lower = sma - multiplier * std;
        double widthPct = sma > 0 ? (upper - lower) / sma * 100.0 : 0;
        return new double[]{upper, sma, lower, widthPct, std};
    }

    // ── Volume ────────────────────────────────────────────────────────────────

    /** Average daily volume over the last {@code period} bars (or all available if fewer). */
    public static double calculateAvgVolumeFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len == 0) return 0.0;
        int count = Math.min(len, period);
        double sum = 0;
        for (int i = len - count; i < len; i++) sum += bars.get(i).path("v").asLong();
        return sum / count;
    }

    // ── Trend strength ────────────────────────────────────────────────────────

    /** ADX — measures trend strength; returns 25.0 on insufficient data. */
    public static double calculateAdxFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period * 2 + 1) return 25.0;
        double[] tr = new double[len], pdm = new double[len], mdm = new double[len];
        for (int i = 1; i < len; i++) {
            double h = bars.get(i).path("h").asDouble(), l = bars.get(i).path("l").asDouble();
            double prevH = bars.get(i - 1).path("h").asDouble(), prevL = bars.get(i - 1).path("l").asDouble();
            double prevC = bars.get(i - 1).path("c").asDouble();
            tr[i]  = Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
            double up = h - prevH, down = prevL - l;
            pdm[i] = (up > down && up > 0) ? up : 0;
            mdm[i] = (down > up && down > 0) ? down : 0;
        }
        double tr14 = 0, pdm14 = 0, mdm14 = 0;
        for (int i = 1; i <= period; i++) { tr14 += tr[i]; pdm14 += pdm[i]; mdm14 += mdm[i]; }
        double pdi = tr14 > 0 ? 100.0 * pdm14 / tr14 : 0;
        double mdi = tr14 > 0 ? 100.0 * mdm14 / tr14 : 0;
        double adx = (pdi + mdi) > 0 ? 100.0 * Math.abs(pdi - mdi) / (pdi + mdi) : 0;
        for (int i = period + 1; i < len; i++) {
            tr14  = tr14  - (tr14  / period) + tr[i];
            pdm14 = pdm14 - (pdm14 / period) + pdm[i];
            mdm14 = mdm14 - (mdm14 / period) + mdm[i];
            pdi = tr14 > 0 ? 100.0 * pdm14 / tr14 : 0;
            mdi = tr14 > 0 ? 100.0 * mdm14 / tr14 : 0;
            double dx = (pdi + mdi) > 0 ? 100.0 * Math.abs(pdi - mdi) / (pdi + mdi) : 0;
            adx = ((adx * (period - 1)) + dx) / period;
        }
        return adx;
    }

    /** ADX computed over bars[0..size-1-offset] — for trend-exhaustion detection. */
    public static double calculateAdxAtOffset(JsonNode bars, int period, int offset) {
        int len = bars.size() - offset;
        if (len < period * 2 + 1) return 25.0;
        double[] tr = new double[len], pdm = new double[len], mdm = new double[len];
        for (int i = 1; i < len; i++) {
            double h = bars.get(i).path("h").asDouble(), l = bars.get(i).path("l").asDouble();
            double prevH = bars.get(i - 1).path("h").asDouble(), prevL = bars.get(i - 1).path("l").asDouble();
            double prevC = bars.get(i - 1).path("c").asDouble();
            tr[i]  = Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
            double up = h - prevH, down = prevL - l;
            pdm[i] = (up > down && up > 0) ? up : 0;
            mdm[i] = (down > up && down > 0) ? down : 0;
        }
        double tr14 = 0, pdm14 = 0, mdm14 = 0;
        for (int i = 1; i <= period; i++) { tr14 += tr[i]; pdm14 += pdm[i]; mdm14 += mdm[i]; }
        double pdi = tr14 > 0 ? 100.0 * pdm14 / tr14 : 0;
        double mdi = tr14 > 0 ? 100.0 * mdm14 / tr14 : 0;
        double adx = (pdi + mdi) > 0 ? 100.0 * Math.abs(pdi - mdi) / (pdi + mdi) : 0;
        for (int i = period + 1; i < len; i++) {
            tr14  = tr14  - (tr14  / period) + tr[i];
            pdm14 = pdm14 - (pdm14 / period) + pdm[i];
            mdm14 = mdm14 - (mdm14 / period) + mdm[i];
            pdi = tr14 > 0 ? 100.0 * pdm14 / tr14 : 0;
            mdi = tr14 > 0 ? 100.0 * mdm14 / tr14 : 0;
            double dx = (pdi + mdi) > 0 ? 100.0 * Math.abs(pdi - mdi) / (pdi + mdi) : 0;
            adx = ((adx * (period - 1)) + dx) / period;
        }
        return adx;
    }

    // ── Pattern detection ─────────────────────────────────────────────────────

    /**
     * Detects single-bar and two-bar daily candlestick reversal patterns.
     * Returns one of: HAMMER, SHOOTING_STAR, DOJI, BULLISH_ENGULFING, BEARISH_ENGULFING,
     * BULLISH_MARUBOZU, BEARISH_MARUBOZU, or NONE.
     */
    public static String detectDailyCandlePattern(JsonNode bars) {
        int len = bars.size();
        if (len < 2) return "NONE";
        JsonNode today     = bars.get(len - 1);
        JsonNode yesterday = bars.get(len - 2);
        double o = today.path("o").asDouble(),     c = today.path("c").asDouble();
        double h = today.path("h").asDouble(),     l = today.path("l").asDouble();
        double yo = yesterday.path("o").asDouble(), yc = yesterday.path("c").asDouble();
        double range = h - l;
        if (range < 0.0001) return "NONE";
        double body       = Math.abs(c - o);
        double upperWick  = h - Math.max(o, c);
        double lowerWick  = Math.min(o, c) - l;
        double bodyRatio  = body / range;
        // Doji: near-zero body
        if (bodyRatio < 0.08) return "DOJI";
        // Marubozu: almost pure body, minimal wicks
        if (bodyRatio > 0.85) return c > o ? "BULLISH_MARUBOZU" : "BEARISH_MARUBOZU";
        // Hammer: bullish reversal at lows — long lower wick, small body at top
        if (c >= o && lowerWick >= 2.0 * body && upperWick <= 0.4 * body && bodyRatio < 0.4)
            return "HAMMER";
        // Shooting Star: bearish reversal at highs — long upper wick, small body at bottom
        if (c <= o && upperWick >= 2.0 * body && lowerWick <= 0.4 * body && bodyRatio < 0.4)
            return "SHOOTING_STAR";
        // Bullish Engulfing: green today fully engulfs red yesterday
        if (c > o && yc < yo && o <= yc && c >= yo) return "BULLISH_ENGULFING";
        // Bearish Engulfing: red today fully engulfs green yesterday
        if (c < o && yc > yo && o >= yc && c <= yo) return "BEARISH_ENGULFING";
        return "NONE";
    }

    /**
     * Detects multi-bar chart patterns from daily bars.
     * Checks (in priority order): INSIDE_BAR, NR7, BULL_FLAG, BEAR_FLAG.
     * Returns the first matching pattern or NONE.
     */
    public static String detectChartPattern(JsonNode bars, double atr) {
        int len = bars.size();
        if (len < 7) return "NONE";
        double todayH = bars.get(len - 1).path("h").asDouble();
        double todayL = bars.get(len - 1).path("l").asDouble();
        double yestH  = bars.get(len - 2).path("h").asDouble();
        double yestL  = bars.get(len - 2).path("l").asDouble();
        double todayRange = todayH - todayL;
        // Inside Bar: today fully contained within yesterday
        if (todayH < yestH && todayL > yestL) return "INSIDE_BAR";
        // NR7: today's range is tightest of last 7 bars
        boolean nr7 = true;
        for (int i = len - 7; i < len - 1; i++) {
            double r = bars.get(i).path("h").asDouble() - bars.get(i).path("l").asDouble();
            if (todayRange >= r) { nr7 = false; break; }
        }
        if (nr7 && todayRange > 0) return "NR7";
        // Bull Flag / Bear Flag — require at least 10 bars
        if (len >= 10 && atr > 0) {
            int poleEnd = len - 4;
            // Bull Flag: 3-bar flagpole (each close up > 0.2×ATR) + 3-bar tight consolidation
            boolean bullPole = true;
            for (int i = poleEnd - 2; i <= poleEnd; i++) {
                double c = bars.get(i).path("c").asDouble();
                double pc = bars.get(i - 1).path("c").asDouble();
                if (c <= pc || (c - pc) < atr * 0.2) { bullPole = false; break; }
            }
            if (bullPole) {
                boolean tight = true;
                for (int i = poleEnd + 1; i < len; i++) {
                    double r = bars.get(i).path("h").asDouble() - bars.get(i).path("l").asDouble();
                    if (r > atr * 0.8) { tight = false; break; }
                }
                if (tight) return "BULL_FLAG";
            }
            // Bear Flag: 3-bar flagpole (each close down > 0.2×ATR) + 3-bar tight consolidation
            boolean bearPole = true;
            for (int i = poleEnd - 2; i <= poleEnd; i++) {
                double c = bars.get(i).path("c").asDouble();
                double pc = bars.get(i - 1).path("c").asDouble();
                if (c >= pc || (pc - c) < atr * 0.2) { bearPole = false; break; }
            }
            if (bearPole) {
                boolean tight = true;
                for (int i = poleEnd + 1; i < len; i++) {
                    double r = bars.get(i).path("h").asDouble() - bars.get(i).path("l").asDouble();
                    if (r > atr * 0.8) { tight = false; break; }
                }
                if (tight) return "BEAR_FLAG";
            }
        }
        return "NONE";
    }

    // ── Options helpers ───────────────────────────────────────────────────────

    /** Rounds a stock price to the nearest standard options strike increment. */
    public static double getNearestOptionStrike(double price) {
        if (price <= 50.0)       return Math.round(price * 2.0) / 2.0;
        else if (price <= 200.0) return Math.round(price);
        else if (price <= 500.0) return Math.round(price / 2.5) * 2.5;
        else                     return Math.round(price / 5.0) * 5.0;
    }

    // ── Score utilities ───────────────────────────────────────────────────────

    /** Clamps a raw value (scaled by {@code scaleFactor}) into the [-100, +100] range. */
    public static double normalizeScore(double value, double scaleFactor) {
        return Math.max(-100.0, Math.min(100.0, value * scaleFactor));
    }
}
