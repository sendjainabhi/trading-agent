package com.quant.agent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class UserPrefsService {

    private static final Logger log = LoggerFactory.getLogger(UserPrefsService.class);

    private final Path prefsFile = Paths.get(System.getProperty("user.home"), ".alphaquant", "prefs.json");
    private final ObjectMapper mapper = new ObjectMapper();
    private Prefs prefs = new Prefs();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Prefs {
        public List<String>              watchlist      = new ArrayList<>();
        public Map<String, String>       watchlistNames = new HashMap<>();
        public String                    theme          = "light";
        public Map<String, String>       modelConfig    = new HashMap<>();
        public List<Map<String, String>> journal        = new ArrayList<>();
    }

    @PostConstruct
    public void load() {
        try {
            if (Files.exists(prefsFile)) {
                prefs = mapper.readValue(prefsFile.toFile(), Prefs.class);
                log.info("Loaded user prefs from {}", prefsFile);
            }
        } catch (Exception e) {
            log.warn("Could not load prefs from {}: {}", prefsFile, e.getMessage());
            prefs = new Prefs();
        }
    }

    private synchronized void persist() {
        try {
            Files.createDirectories(prefsFile.getParent());
            mapper.writerWithDefaultPrettyPrinter().writeValue(prefsFile.toFile(), prefs);
        } catch (Exception e) {
            log.warn("Could not save prefs to {}: {}", prefsFile, e.getMessage());
        }
    }

    public Prefs getAll() { return prefs; }

    public void setWatchlist(List<String> list, Map<String, String> names) {
        prefs.watchlist = list != null ? list : new ArrayList<>();
        if (names != null) prefs.watchlistNames = new HashMap<>(names);
        persist();
    }

    public void setTheme(String theme) {
        prefs.theme = theme != null ? theme : "light";
        persist();
    }

    public void setModelConfig(Map<String, String> config) {
        if (config != null) prefs.modelConfig = new HashMap<>(config);
        persist();
    }

    public Map<String, String> getModelConfig() {
        return prefs.modelConfig;
    }

    public List<Map<String, String>> getJournal() {
        return new ArrayList<>(prefs.journal);
    }

    public synchronized void addJournalEntry(Map<String, String> entry) {
        prefs.journal.add(0, new HashMap<>(entry)); // newest first
        persist();
    }

    public synchronized void clearJournal() {
        prefs.journal.clear();
        persist();
    }
}
