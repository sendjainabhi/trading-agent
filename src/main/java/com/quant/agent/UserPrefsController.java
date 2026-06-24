package com.quant.agent;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class UserPrefsController {

    private static final Logger log = LoggerFactory.getLogger(UserPrefsController.class);

    private final UserPrefsService    prefsService;
    private final TradingAgentService tradingAgentService;

    public UserPrefsController(UserPrefsService prefsService,
                               TradingAgentService tradingAgentService) {
        this.prefsService        = prefsService;
        this.tradingAgentService = tradingAgentService;
    }

    /** Auto-apply persisted model config on every server start. */
    @PostConstruct
    public void applyPersistedModelConfig() {
        Map<String, String> cfg = prefsService.getModelConfig();
        if (cfg != null && cfg.containsKey("model") && !cfg.get("model").isBlank()) {
            try {
                tradingAgentService.updateModelConfig(cfg);
                log.info("Auto-applied persisted model: {}/{}", cfg.get("provider"), cfg.get("model"));
            } catch (Exception e) {
                log.warn("Could not auto-apply persisted model config: {}", e.getMessage());
            }
        }
    }

    /** Returns all persisted prefs. API key is never sent back to the browser. */
    @GetMapping("/api/prefs")
    public Map<String, Object> getAll() {
        UserPrefsService.Prefs prefs = prefsService.getAll();
        Map<String, Object> resp = new HashMap<>();
        resp.put("watchlist",      prefs.watchlist);
        resp.put("watchlistNames", prefs.watchlistNames);
        resp.put("theme",          prefs.theme);

        Map<String, String> safeModel = new HashMap<>();
        if (prefs.modelConfig != null) {
            prefs.modelConfig.forEach((k, v) -> {
                if (!"apiKey".equals(k)) safeModel.put(k, v);
            });
            safeModel.put("hasApiKey", String.valueOf(
                prefs.modelConfig.containsKey("apiKey") &&
                !prefs.modelConfig.getOrDefault("apiKey", "").isBlank()));
        }
        resp.put("modelConfig", safeModel);
        return resp;
    }

    @PostMapping("/api/prefs/watchlist")
    public Map<String, Object> saveWatchlist(@RequestBody Map<String, Object> body) {
        try {
            @SuppressWarnings("unchecked")
            List<String> watchlist = (List<String>) body.get("watchlist");
            @SuppressWarnings("unchecked")
            Map<String, String> names = (Map<String, String>) body.get("names");
            prefsService.setWatchlist(watchlist, names);
            return Map.of("success", true);
        } catch (Exception e) {
            return Map.of("success", false, "error", e.getMessage());
        }
    }

    @PostMapping("/api/prefs/theme")
    public Map<String, Object> saveTheme(@RequestBody Map<String, String> body) {
        try {
            prefsService.setTheme(body.get("theme"));
            return Map.of("success", true);
        } catch (Exception e) {
            return Map.of("success", false, "error", e.getMessage());
        }
    }

    @PostMapping("/api/prefs/model")
    public Map<String, Object> saveModelConfig(@RequestBody Map<String, String> config) {
        try {
            prefsService.setModelConfig(config);
            return Map.of("success", true);
        } catch (Exception e) {
            return Map.of("success", false, "error", e.getMessage());
        }
    }
}
