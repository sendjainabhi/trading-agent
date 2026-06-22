package com.quant.agent;

import org.springframework.web.bind.annotation.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class BacktestController {

    private final BacktestService backtestService;

    public BacktestController(BacktestService backtestService) {
        this.backtestService = backtestService;
    }

    /**
     * GET /api/backtest?symbol=AAPL&days=252&holdDays=5&threshold=20
     *
     * Replays the scoring engine over historical daily bars using a walk-forward approach.
     * Entry when daily score >= threshold (long) or <= -threshold (short).
     * Exit after holdDays OR on a reversal signal. Returns an HTML report.
     */
    @GetMapping("/backtest")
    public String runBacktest(
            @RequestParam String symbol,
            @RequestParam(defaultValue = "252") int days,
            @RequestParam(defaultValue = "5")   int holdDays,
            @RequestParam(defaultValue = "20.0") double threshold) {
        try {
            String ticker = symbol.replaceAll("[\"'\\s]", "").toUpperCase();
            BacktestService.BacktestResult r = backtestService.run(ticker, days, holdDays, threshold);
            return formatReport(r);
        } catch (Exception e) {
            return "<b>Backtest failed for " + symbol.toUpperCase() + ":</b> " + e.getMessage();
        }
    }

    private String formatReport(BacktestService.BacktestResult r) {
        String ts = ZonedDateTime.now(ZoneId.of("America/New_York"))
                .format(DateTimeFormatter.ofPattern("hh:mm:ss a z, MMMM dd yyyy"));

        String totalColor  = r.totalReturnPct()  >= 0 ? "#28a745" : "#dc3545";
        String sharpeColor = r.sharpeRatio() >= 1.0 ? "#28a745" : r.sharpeRatio() >= 0.5 ? "#ffc107" : "#dc3545";
        String winColor    = r.winRate() >= 55 ? "#28a745" : r.winRate() >= 45 ? "#ffc107" : "#dc3545";

        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "<b>[BACKTEST REPORT — %s]</b>\n" +
                "Generated: %s | Lookback: %d days | Hold: up to %d days | Entry threshold: ±%.0f\n" +
                "---\n" +
                "<b>[PERFORMANCE SUMMARY]</b>\n" +
                "<b>Total Trades:</b> %d &nbsp;|&nbsp; " +
                "<b>Win Rate:</b> <span style=\"color:%s;\">%.1f%%</span> &nbsp;|&nbsp; " +
                "<b>Avg Return/Trade:</b> %.2f%%\n" +
                "<b>Total Return (compounded):</b> <span style=\"color:%s;\">%.2f%%</span> &nbsp;|&nbsp; " +
                "<b>Max Drawdown:</b> -%.2f%%\n" +
                "<b>Sharpe Ratio:</b> <span style=\"color:%s;\">%.2f</span>\n" +
                "---\n" +
                "<b>[TRADE LOG]</b> (most recent %d of %d)\n",
                r.symbol(), ts, r.lookbackDays(), r.holdDays(), r.threshold(),
                r.totalTrades(), winColor, r.winRate(), r.avgReturnPct(),
                totalColor, r.totalReturnPct(), r.maxDrawdownPct(),
                sharpeColor, r.sharpeRatio(),
                Math.min(r.trades().size(), 20), r.trades().size()
        ));

        List<BacktestService.TradeRecord> recent = r.trades()
                .subList(Math.max(0, r.trades().size() - 20), r.trades().size());

        sb.append("<table><tr><th>Entry Date</th><th>Exit Date</th><th>Dir</th>" +
                  "<th>Entry $</th><th>Exit $</th><th>Return</th></tr>\n");
        for (BacktestService.TradeRecord t : recent) {
            String retColor = t.returnPct() > 0 ? "#28a745" : "#dc3545";
            String dir = t.isLong()
                    ? "<span style=\"color:#28a745;\">LONG</span>"
                    : "<span style=\"color:#dc3545;\">SHORT</span>";
            sb.append(String.format(
                    "<tr><td>%s</td><td>%s</td><td>%s</td><td>$%.2f</td><td>$%.2f</td>" +
                    "<td><span style=\"color:%s;\">%s%.2f%%</span></td></tr>\n",
                    t.entryDate(), t.exitDate(), dir,
                    t.entryPrice(), t.exitPrice(),
                    retColor, t.returnPct() >= 0 ? "+" : "", t.returnPct()
            ));
        }
        sb.append("</table>\n---");
        return sb.toString();
    }
}
