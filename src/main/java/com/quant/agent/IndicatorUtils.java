package com.quant.agent;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Pure static math helpers — no Spring, no I/O.
 * All methods operate on a JsonNode bar array fetched from Alpaca.
 */
public final class IndicatorUtils {

    // Prevent instantiation — utility class only
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
        if (bars.size() < period) return bars.get(bars.size() - 1).path("c").asDouble();
        double k = 2.0 / (period + 1.0);
        double ema = bars.get(0).path("c").asDouble();
        for (int i = 1; i < bars.size(); i++) ema = bars.get(i).path("c").asDouble() * k + ema * (1.0 - k);
        return ema;
    }

    // ── Momentum ──────────────────────────────────────────────────────────────

    /** Relative Strength Index over the last {@code period} bars; returns 50.0 on insufficient data. */
    public static double calculateRsiFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period + 1) return 50.0;
        double gains = 0, losses = 0;
        for (int i = len - period; i < len; i++) {
            double diff = bars.get(i).path("c").asDouble() - bars.get(i - 1).path("c").asDouble();
            if (diff > 0) gains += diff; else losses -= diff;
        }
        double avgGain = gains / period, avgLoss = losses / period;
        if (avgLoss < 0.0001) return avgGain > 0 ? 95.0 : 50.0;
        double rs = avgGain / avgLoss;
        return 100.0 - (100.0 / (1.0 + rs));
    }

    // ── Volatility ────────────────────────────────────────────────────────────

    /** Average True Range over the last {@code period} bars; returns 0.0 on insufficient data. */
    public static double calculateAtrFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period + 1) return 0.0;
        double sum = 0;
        for (int i = len - period; i < len; i++) {
            double h = bars.get(i).path("h").asDouble(), l = bars.get(i).path("l").asDouble();
            double prevC = bars.get(i - 1).path("c").asDouble();
            sum += Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
        }
        return sum / period;
    }

    // ── Volume ────────────────────────────────────────────────────────────────

    /** Average daily volume over the last {@code period} bars (or all available bars if fewer). */
    public static double calculateAvgVolumeFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len == 0) return 0.0;
        int count = Math.min(len, period);
        double sum = 0;
        for (int i = len - count; i < len; i++) sum += bars.get(i).path("v").asLong();
        return sum / count;
    }

    // ── Trend strength ────────────────────────────────────────────────────────

    /** Average Directional Index — measures trend strength; returns 25.0 on insufficient data. */
    public static double calculateAdxFromBars(JsonNode bars, int period) {
        int len = bars.size();
        if (len < period * 2 + 1) return 25.0;
        double[] tr  = new double[len];
        double[] pdm = new double[len];
        double[] mdm = new double[len];
        for (int i = 1; i < len; i++) {
            double h  = bars.get(i).path("h").asDouble();
            double l  = bars.get(i).path("l").asDouble();
            double prevH = bars.get(i - 1).path("h").asDouble();
            double prevL = bars.get(i - 1).path("l").asDouble();
            double prevC = bars.get(i - 1).path("c").asDouble();
            tr[i]  = Math.max(h - l, Math.max(Math.abs(h - prevC), Math.abs(l - prevC)));
            double up   = h - prevH, down = prevL - l;
            pdm[i] = (up > down && up > 0)     ? up   : 0;
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

    // ── Options helpers ───────────────────────────────────────────────────────

    /** Rounds a stock price to the nearest standard options strike increment. */
    public static double getNearestOptionStrike(double price) {
        if (price <= 50.0) {
            return Math.round(price * 2.0) / 2.0;
        } else if (price <= 200.0) {
            return Math.round(price);
        } else if (price <= 500.0) {
            return Math.round(price / 2.5) * 2.5;
        } else {
            return Math.round(price / 5.0) * 5.0;
        }
    }

    // ── Score utilities ───────────────────────────────────────────────────────

    /** Clamps a raw value (scaled by {@code scaleFactor}) into the [-100, +100] range. */
    public static double normalizeScore(double value, double scaleFactor) {
        return Math.max(-100.0, Math.min(100.0, value * scaleFactor));
    }
}
