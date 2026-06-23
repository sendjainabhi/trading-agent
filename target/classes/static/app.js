// ── State ─────────────────────────────────────────────────────────────────
let activeStrip           = null;
let lastAgentMsg          = null;
let refreshTimerId        = null;
let activeAbortController = null;
let stripGeneration       = 0;

// ── marked: don't wrap block-level LLM HTML in <p> tags ──────────────────
(function () {
    const renderer = new marked.Renderer();
    renderer.paragraph = function (token) {
        const text = (typeof token === 'object' && token !== null) ? (token.text || '') : String(token || '');
        return text.trimStart().startsWith('<') ? text + '\n' : '<p>' + text + '</p>\n';
    };
    marked.use({ renderer, breaks: true, gfm: true, pedantic: false });
})();

// ── Ticker helpers ────────────────────────────────────────────────────────
const TICKER_STOP = new Set([
    'A','I','AM','AN','AT','BE','BY','DO','ETF','FOR','GO','HI','IF',
    'IN','IS','IT','ME','MY','NO','OF','OK','ON','OR','THE','TO','UP',
    'VS','AND','ARE','BIG','BUY','CAN','CEO','CFO','GET','HOT','HOW',
    'IPO','NEW','NOW','OTC','ATH','ATL','PRE','ALL','TOP','SELL','SCAN',
    'SHOW','WHAT','WHEN','WITH','MOST','BEST','FROM','GIVE','FIND','LAST',
    'LIVE','JUST','HIGH','LOW'
]);

const COMPANY_MAP = {
    'tesla':'TSLA','nvidia':'NVDA','apple':'AAPL','google':'GOOGL',
    'alphabet':'GOOGL','microsoft':'MSFT','amazon':'AMZN','meta':'META',
    'facebook':'META','netflix':'NFLX','amd':'AMD','intel':'INTC',
    'broadcom':'AVGO','salesforce':'CRM','palantir':'PLTR',
    'coinbase':'COIN','shopify':'SHOP','snowflake':'SNOW',
    'uber':'UBER','lyft':'LYFT','airbnb':'ABNB','spotify':'SPOT'
};

// Extract every unique TICKER ($PRICE) pair from the AI's response text
// Common financial terms that look like tickers (uppercase + price in parens) but aren't
const TICKER_BLOCKLIST = new Set([
    'VWAP','EMA','SMA','WMA','RSI','MACD','ATH','ATL','ETF','IPO','NAV',
    'ADX','ADR','OTC','SEC','NYSE','NASDAQ','SPX','VIX','OI','IV','PE',
    'EPS','TTM','YTD','QOQ','YOY','AUM','CAGR','FCF','DCF','ROE','ROI',
    'EBIT','EBITDA','GAAP','ROIC','NII','NIM','BPS','PPS','ATM','ITM','OTM'
]);

function extractAllTickers(text) {
    const seen = new Set(), result = [];
    const re = /\b([A-Z]{1,5})\s*\(\$[\d,]+\.?\d{0,2}\)/g;
    let m;
    while ((m = re.exec(text)) !== null) {
        if (!seen.has(m[1]) && !TICKER_BLOCKLIST.has(m[1])) {
            seen.add(m[1]);
            result.push(m[1]);
        }
    }
    return result;
}

// Fallback when response has no TICKER ($PRICE) pattern (e.g. conversational reply)
function detectTickerFromInput(text) {
    const lower = text.toLowerCase();
    for (const [name, ticker] of Object.entries(COMPANY_MAP)) {
        if (lower.includes(name)) return ticker;
    }
    const upper = text.toUpperCase();
    for (const m of upper.matchAll(/\b([A-Z]{2,5})\b/g)) {
        if (!TICKER_STOP.has(m[1])) return m[1];
    }
    return null;
}

// ── LLM output helpers ────────────────────────────────────────────────────
const RAW_TAG_MAP = {
    'EXECUTE_CALL_OR_LONG_SPREAD':       '🔥 Buy Now',
    'PREPARE_LONG_BUY_DIP_AT_VWAP':      '⏳ Wait to Buy on a Dip',
    'EXECUTE_PUT_OR_SHORT_SPREAD':        '⚠️ Sell / Short Now',
    'PREPARE_SHORT_FADE_BOUNCE_AT_VWAP':  '⏳ Wait to Sell on a Bounce',
    'STAND_DOWN_COLLECT_PREMIUM':         '— No Clear Trade',
    'STRONG_BUY':  '🔥 Strong Buy',
    'STRONG_SELL': '🔴 Strong Sell',
};
function sanitizeLlmOutput(html) {
    let out = html;
    for (const [raw, label] of Object.entries(RAW_TAG_MAP))
        out = out.replaceAll(raw, label);
    out = out.replace(/\{OPTIONAL_CUSTOM_DURATION\}/g, '');
    out = applyColorCoding(out);
    return out;
}

// Colour-code arrows and direction phrases in text nodes only (never inside tag attributes).
const GREEN = '#28a745', RED = '#dc3545', ORANGE = '#ffc107';

// Phrases the model should colour but sometimes misses
const PHRASE_COLORS = [
    [/Trending UP ↑|Trend just flipped UP ↑|Going Up ↑|Rising ↑|Pushing Up ↑|Moving Up ↑/g, GREEN],
    [/Trending DOWN ↓|Trend just flipped DOWN ↓|Going Down ↓|Falling ↓|Pushing Down ↓|Moving Down ↓/g, RED],
    [/Buy \/ Long|Buy Now|Wait to Buy on a Dip/g, GREEN],
    [/Sell \/ Short|Sell Now|Wait to Sell on a Bounce/g, RED],
    [/Sideways →|Flat →|Sit Out for Now/g, ORANGE],
];

function applyColorCoding(html) {
    // Split into tag tokens and text tokens; only mutate text tokens.
    return html.replace(/(<[^>]*>)|([^<]+)/g, (match, tag, text) => {
        if (tag) return tag; // preserve HTML tags and their attributes untouched

        // 1. Colour full directional phrases first (they include the arrow)
        let out = PHRASE_COLORS.reduce((t, [re, color]) =>
            t.replace(re, m => `<span style="color:${color}">${m}</span>`), text);

        // 2. Colour any remaining bare arrows not already wrapped above
        out = out
            .replace(/↑/g, `<span style="color:${GREEN}">↑</span>`)
            .replace(/↓/g, `<span style="color:${RED}">↓</span>`)
            .replace(/→/g, `<span style="color:${ORANGE}">→</span>`);

        return out;
    });
}
function wrapTables(container) {
    container.querySelectorAll('table').forEach(t => {
        if (t.parentElement.classList.contains('table-scroll')) return;
        const wrap = document.createElement('div');
        wrap.className = 'table-scroll';
        t.parentNode.insertBefore(wrap, t);
        wrap.appendChild(t);
    });
}

// ── Bubble strip ──────────────────────────────────────────────────────────
function activateBubbleStrip(agentMsg, tickers) {
    if (!tickers.length) return;
    const myGen = ++stripGeneration;

    // 1. Wrap each inline TICKER ($PRICE) — symbol gets data-live-symbol, price gets
    //    data-live-price so the refresh can color both identically (same change direction).
    //    Do this BEFORE appending the strip so the innerHTML rebuild is safe.
    tickers.forEach(ticker => {
        const esc = ticker.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const re  = new RegExp('(' + esc + '\\s*\\()\\$[\\d,]+\\.?\\d{0,2}(\\))', 'g');
        const patched = agentMsg.innerHTML.replace(
            re,
            () =>
                `<span data-live-symbol data-ticker="${ticker}" style="font-weight:bold">${ticker}</span>` +
                ` (<span data-live-price data-ticker="${ticker}" style="font-weight:bold">$…</span>)`
        );
        if (patched !== agentMsg.innerHTML) agentMsg.innerHTML = patched;
    });

    // 2. Build the strip with one cell per ticker
    const strip = document.createElement('div');
    strip.className = 'bubble-strip';

    tickers.forEach(ticker => {
        const cell = document.createElement('div');
        cell.className   = 'bubble-strip-cell';
        cell.dataset.ticker = ticker;
        cell.innerHTML =
            `<span class="bsc-ticker">${ticker}</span>` +
            `<span class="bsc-price">…</span>` +
            `<span class="bsc-change flat"></span>`;
        strip.appendChild(cell);
    });

    // Controls: time + interval selector + refresh button
    const ctrl = document.createElement('div');
    ctrl.className = 'bsc-controls';
    ctrl.innerHTML =
        `<span class="bsc-time"></span>` +
        `<select class="bsc-select">` +
            `<option value="0">Off</option>` +
            `<option value="30">30s</option>` +
            `<option value="60" selected>1m</option>` +
            `<option value="120">2m</option>` +
            `<option value="300">5m</option>` +
        `</select>` +
        `<button class="bsc-btn" title="Refresh now">⟳</button>`;
    strip.appendChild(ctrl);
    agentMsg.insertBefore(strip, agentMsg.firstChild);

    // 3. Wire up controls with closures so they always refresh THIS strip
    const doRefresh = () => { if (myGen !== stripGeneration) return; refreshAllCells(strip, agentMsg); };
    ctrl.querySelector('.bsc-btn').onclick = doRefresh;
    ctrl.querySelector('.bsc-select').onchange = function () {
        if (refreshTimerId) clearInterval(refreshTimerId);
        refreshTimerId = null;
        const secs = parseInt(this.value, 10);
        if (secs > 0) refreshTimerId = setInterval(doRefresh, secs * 1000);
    };

    // 4. Replace global active strip reference and start refreshing
    if (refreshTimerId) clearInterval(refreshTimerId);
    activeStrip  = strip;
    lastAgentMsg = agentMsg;
    doRefresh();                                              // immediate
    refreshTimerId = setInterval(doRefresh, 60 * 1000);      // default 1m
}

async function refreshAllCells(strip, msgEl) {
    if (!strip) return;
    const cells = Array.from(strip.querySelectorAll('.bubble-strip-cell[data-ticker]'));
    await Promise.all(cells.map(c => refreshCell(c, c.dataset.ticker, msgEl)));
    const timeEl = strip.querySelector('.bsc-time');
    if (timeEl) {
        timeEl.textContent = new Date().toLocaleTimeString(
            'en-US', {hour12:false, hour:'2-digit', minute:'2-digit', second:'2-digit'}
        ) + ' ET';
    }
}

async function refreshCell(cell, ticker, msgEl) {
    try {
        const res  = await fetch(`/api/price/${encodeURIComponent(ticker)}`);
        if (!res.ok) return;
        const data = await res.json();
        if (data.price <= 0) return;

        const sign  = data.change >= 0 ? '+' : '';
        const cls   = data.change > 0.005 ? 'up' : (data.change < -0.005 ? 'down' : 'flat');
        const arrow = cls === 'up' ? '▲' : (cls === 'down' ? '▼' : '—');
        const color = cls === 'up' ? '#28a745' : (cls === 'down' ? '#dc3545' : '#00a550');

        // Update the strip cell
        const tickerEl = cell.querySelector('.bsc-ticker');
        const priceEl  = cell.querySelector('.bsc-price');
        const changeEl = cell.querySelector('.bsc-change');
        tickerEl.style.color = color;
        priceEl.textContent  = '$' + data.price.toFixed(2);
        priceEl.style.color  = color;
        changeEl.textContent = `${arrow} ${sign}${data.changePercent.toFixed(2)}%`;
        changeEl.className   = 'bsc-change ' + cls;

        // Update the inline TICKER ($PRICE) spans inside the bubble text
        if (msgEl) {
            // Update the price span
            const priceSpan = msgEl.querySelector(`[data-live-price][data-ticker="${ticker}"]`);
            if (priceSpan) {
                priceSpan.textContent = '$' + data.price.toFixed(2);
                priceSpan.style.color = color;
            }
            // Update the symbol span (same color as price — both reflect today's direction)
            const symbolSpan = msgEl.querySelector(`[data-live-symbol][data-ticker="${ticker}"]`);
            if (symbolSpan) {
                symbolSpan.style.color = color;
            }
            // Walk up from the price span and recolor the first ancestor <span> the AI
            // generated for the header (the outer style="color:..."> wrapper around the symbol)
            if (priceSpan) {
                let outer = priceSpan.parentElement;
                while (outer && outer !== msgEl) {
                    if (outer.tagName === 'SPAN' && outer.style.color) {
                        outer.style.color = color;
                        break;
                    }
                    outer = outer.parentElement;
                }
            }
        }
    } catch (e) { /* silent: stale price stays visible */ }
}

// ── Autocomplete ──────────────────────────────────────────────────────────
let selectedSuggestionIdx = -1;
let suggestDebounceTimer  = null;

function getLastToken(text) {
    const m = text.match(/(\S+)$/);
    return m ? m[1] : '';
}

async function handleInputChange(value) {
    const term = getLastToken(value);
    if (term.length < 2) { hideSuggestions(); return; }
    clearTimeout(suggestDebounceTimer);
    suggestDebounceTimer = setTimeout(async () => {
        try {
            const res = await fetch(`/api/search?q=${encodeURIComponent(term)}`);
            if (!res.ok) return;
            const items = await res.json();
            showSuggestions(items.map(i => [i.symbol, i.name]));
        } catch (e) { /* autocomplete is optional — never block the user */ }
    }, 280);
}

function showSuggestions(items) {
    const box = document.getElementById('suggestions');
    if (!items.length) { hideSuggestions(); return; }
    selectedSuggestionIdx = -1;
    box.innerHTML = items.map(([sym, name]) =>
        `<div class="suggestion-item" data-sym="${sym}" onmousedown="event.preventDefault();pickSuggestion('${sym}')">` +
        `<span class="sug-ticker">${sym}</span><span class="sug-name">${name}</span></div>`
    ).join('');
    box.style.display = 'block';
}

function hideSuggestions() {
    const box = document.getElementById('suggestions');
    if (box) { box.style.display = 'none'; box.innerHTML = ''; }
    selectedSuggestionIdx = -1;
}

function pickSuggestion(symbol) {
    const inputEl = document.getElementById('commandInput');
    const val   = inputEl.value;
    const token = getLastToken(val);
    inputEl.value = token ? val.slice(0, val.length - token.length) + symbol : symbol;
    hideSuggestions();
    inputEl.focus();
}

function navigateSuggestions(dir) {
    const box   = document.getElementById('suggestions');
    const items = box.querySelectorAll('.suggestion-item');
    if (!items.length) return;
    if (selectedSuggestionIdx >= 0) items[selectedSuggestionIdx].classList.remove('sug-active');
    selectedSuggestionIdx += dir;
    if (selectedSuggestionIdx < -1)            selectedSuggestionIdx = items.length - 1;
    if (selectedSuggestionIdx >= items.length) selectedSuggestionIdx = -1;
    if (selectedSuggestionIdx >= 0) {
        items[selectedSuggestionIdx].classList.add('sug-active');
        items[selectedSuggestionIdx].scrollIntoView({ block: 'nearest' });
    }
}

function handleInputKeydown(event) {
    const box    = document.getElementById('suggestions');
    const isOpen = box && box.style.display === 'block';
    if (event.key === 'ArrowDown') { if (isOpen) { event.preventDefault(); navigateSuggestions(1);  } return; }
    if (event.key === 'ArrowUp')   { if (isOpen) { event.preventDefault(); navigateSuggestions(-1); } return; }
    if (event.key === 'Escape')    { hideSuggestions(); return; }
    if (event.key === 'Enter') {
        if (isOpen && selectedSuggestionIdx >= 0) {
            event.preventDefault();
            pickSuggestion(box.querySelectorAll('.suggestion-item')[selectedSuggestionIdx].dataset.sym);
        } else {
            dispatchCommand();
        }
    }
}

// ── Chip helpers ─────────────────────────────────────────────────────────
function submitChip(text) {
    document.getElementById('commandInput').value = text;
    hideSuggestions();
    dispatchCommand();
}

function setChipsDisabled(disabled) {
    document.querySelectorAll('.chip').forEach(c => c.disabled = disabled);
}

// ── Mobile keyboard resize fallback (iOS < 16 / older Android) ───────────
if (window.visualViewport) {
    const container = document.querySelector('.chat-container');
    window.visualViewport.addEventListener('resize', () => {
        container.style.height = Math.floor(window.visualViewport.height * 0.94) + 'px';
    });
    window.visualViewport.addEventListener('scroll', () => {
        container.style.height = Math.floor(window.visualViewport.height * 0.94) + 'px';
    });
}

// ── Main chat dispatch ────────────────────────────────────────────────────
async function dispatchCommand() {
    const inputEl  = document.getElementById('commandInput');
    const sendBtn  = document.getElementById('sendButton');
    const rawText  = inputEl.value.trim();
    if (!rawText) return;

    hideSuggestions();
    if (activeAbortController) activeAbortController.abort();
    activeAbortController = new AbortController();

    inputEl.value    = '';
    inputEl.disabled = true;
    sendBtn.textContent = 'Cancel';
    sendBtn.onclick  = () => activeAbortController && activeAbortController.abort();
    setChipsDisabled(true);

    const chatWindow = document.getElementById('chat-window');

    const userMsg = document.createElement('div');
    userMsg.className   = 'message user';
    userMsg.textContent = rawText;
    chatWindow.appendChild(userMsg);

    const agentMsg = document.createElement('div');
    agentMsg.className = 'message agent';
    agentMsg.innerHTML = '<span class="loading-text">Fetching live market data...</span>';
    chatWindow.appendChild(agentMsg);
    chatWindow.scrollTop = chatWindow.scrollHeight;

    function resetUI() {
        inputEl.disabled    = false;
        sendBtn.textContent = 'Send';
        sendBtn.onclick     = dispatchCommand;
        setChipsDisabled(false);
        inputEl.focus();
    }

    try {
        const response = await fetch('/api/chat/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ input: rawText }),
            signal: activeAbortController.signal
        });
        if (!response.ok) throw new Error('Server rejected the request.');

        const reader  = response.body.getReader();
        const decoder = new TextDecoder();
        let finalOutputString = '';
        let processActive     = true;
        let streamComplete    = false;
        let renderTimer       = null;

        const scheduleRender = () => {
            if (renderTimer) return;
            renderTimer = setTimeout(() => {
                renderTimer = null;
                if (streamComplete) return;
                if (finalOutputString.trim()) {
                    agentMsg.innerHTML = sanitizeLlmOutput(marked.parse(finalOutputString));
                    wrapTables(agentMsg);
                    chatWindow.scrollTop = chatWindow.scrollHeight;
                }
            }, 250);
        };

        while (processActive) {
            const { value, done } = await reader.read();
            if (done) { processActive = false; break; }
            const chunk = decoder.decode(value, { stream: true });
            if (chunk.startsWith('__PROGRESS__:')) {
                agentMsg.innerHTML = `<span class="loading-text">${chunk.slice(13)}</span>`;
                chatWindow.scrollTop = chatWindow.scrollHeight;
                continue;
            }
            finalOutputString += chunk;
            scheduleRender();
        }

        if (finalOutputString.trim()) {
            agentMsg.innerHTML = sanitizeLlmOutput(marked.parse(finalOutputString));
            wrapTables(agentMsg);
        }
        chatWindow.scrollTop = chatWindow.scrollHeight;
        renderPending  = false;
        streamComplete = true;

        let tickers = extractAllTickers(finalOutputString);
        if (!tickers.length) {
            const t = detectTickerFromInput(rawText);
            if (t) tickers = [t];
        }
        if (tickers.length) activateBubbleStrip(agentMsg, tickers);

    } catch (error) {
        if (error.name === 'AbortError') {
            agentMsg.innerHTML = '<span class="loading-text">Request cancelled.</span>';
        } else {
            console.error(error);
            agentMsg.innerHTML = '<span class="error-text">Could not reach the server. Is the backend running?</span>';
        }
        chatWindow.scrollTop = chatWindow.scrollHeight;
    } finally {
        activeAbortController = null;
        resetUI();
    }
}

// ── Settings & model management ───────────────────────────────────────────
const SETTINGS_KEY = 'alphaquant_model_settings';
const SETTINGS_TTL = 30 * 24 * 60 * 60 * 1000;

// Per-provider default base URLs (updated from /api/model/status on load)
const PROVIDER_DEFAULT_URLS = {
    ollama:    'http://127.0.0.1:11434',
    openai:    'https://api.openai.com',
    anthropic: 'https://api.anthropic.com'
};

let modelSettings = {
    provider: 'ollama', model: '', apiKey: '',
    baseUrl: 'http://127.0.0.1:11434', temperature: 0.0,
    autoConnect: true, savedAt: 0
};

function loadSettings() {
    try {
        const raw = localStorage.getItem(SETTINGS_KEY);
        if (!raw) return false;
        const saved = JSON.parse(raw);
        if (Date.now() - saved.savedAt > SETTINGS_TTL) { localStorage.removeItem(SETTINGS_KEY); return false; }
        modelSettings = { ...modelSettings, ...saved };
        return true;
    } catch (e) { return false; }
}

function saveSettings() {
    modelSettings.savedAt = Date.now();
    localStorage.setItem(SETTINGS_KEY, JSON.stringify(modelSettings));
}

function populateModalFields() {
    const provider = modelSettings.provider || 'ollama';
    document.getElementById('sProvider').value          = provider;
    document.getElementById('sModel').value             = modelSettings.model || '';
    document.getElementById('sApiKey').value            = '';
    document.getElementById('sBaseUrl').value           = modelSettings.baseUrl || PROVIDER_DEFAULT_URLS[provider] || '';
    document.getElementById('sTemp').value              = modelSettings.temperature;
    document.getElementById('sTempDisplay').textContent = parseFloat(modelSettings.temperature || 0).toFixed(2);
    document.getElementById('sAutoConnect').checked     = modelSettings.autoConnect !== false;
    document.getElementById('sRemember').checked        = true;
    document.getElementById('modelSuggestions').style.display = 'none';
}

function openSettingsModal() {
    populateModalFields();
    if (modelSettings.model) {
        enterReadonlyMode();
    } else {
        enterEditMode();
    }
    document.getElementById('settingsModal').style.display = 'flex';
}

function enterEditMode() {
    // Re-enable all form inputs
    ['sModel', 'sBaseUrl', 'sApiKey'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.removeAttribute('readonly');
    });
    document.getElementById('sProvider').disabled = false;
    document.getElementById('sTemp').disabled     = false;

    // Reset API key placeholder
    const apiKeyEl = document.getElementById('sApiKey');
    if (apiKeyEl) apiKeyEl.placeholder = 'sk-… (leave blank to keep existing)';

    // Show action controls
    document.getElementById('sEditBtn').style.display        = 'none';
    document.getElementById('sCancelBtn').style.display      = '';
    document.getElementById('sApplyBtn').style.display       = '';
    document.getElementById('sTestBtn').disabled             = false;
    document.getElementById('sConfigFileGroup').style.display = '';
    document.getElementById('sOptionsRow').style.display     = '';

    updateProviderFields();
    document.getElementById('sConnStatus').innerHTML = '<span class="conn-testing">—</span>';
    document.getElementById('sModel').focus();
}

function enterReadonlyMode() {
    // Make text inputs read-only and disable interactive controls
    ['sModel', 'sBaseUrl', 'sApiKey'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.setAttribute('readonly', '');
    });
    document.getElementById('sProvider').disabled = true;
    document.getElementById('sTemp').disabled     = true;

    // Show provider-appropriate API key field in masked readonly state
    const provider = document.getElementById('sProvider').value || 'ollama';
    const apiKeyGroup = document.getElementById('sApiKeyGroup');
    if (provider !== 'ollama') {
        apiKeyGroup.style.display = '';
        const apiKeyEl = document.getElementById('sApiKey');
        apiKeyEl.value       = '';
        apiKeyEl.placeholder = '●●●●●●●● (saved)';
    } else {
        apiKeyGroup.style.display = 'none';
    }

    // Show Edit button, hide action controls
    document.getElementById('sEditBtn').style.display        = '';
    document.getElementById('sCancelBtn').style.display      = 'none';
    document.getElementById('sApplyBtn').style.display       = 'none';
    document.getElementById('sTestBtn').disabled             = true;
    document.getElementById('sConfigFileGroup').style.display = 'none';
    document.getElementById('sOptionsRow').style.display     = 'none';

    document.getElementById('sConnStatus').innerHTML = '<span class="conn-ok">✓ Credentials saved</span>';
}

function cancelEdit() {
    if (modelSettings.model) {
        populateModalFields();
        enterReadonlyMode();
    } else {
        closeSettingsModal();
    }
}

function closeSettingsModal() {
    document.getElementById('settingsModal').style.display = 'none';
}

function selectProvider(provider) {
    document.getElementById('sProvider').value = provider;
    updateProviderFields();
}

function updateProviderFields() {
    const provider = document.getElementById('sProvider').value || 'ollama';
    document.getElementById('sApiKeyGroup').style.display = provider !== 'ollama' ? '' : 'none';

    const labelMap = {
        ollama:    'Ollama Server URL',
        openai:    'OpenAI Endpoint URL',
        anthropic: 'Anthropic Endpoint URL'
    };
    const hintMap = {
        ollama:    'e.g. http://127.0.0.1:11434',
        openai:    'e.g. https://api.openai.com  or  http://your-host/proxy-path',
        anthropic: 'e.g. https://api.anthropic.com  or  http://your-host/proxy-path'
    };
    document.getElementById('sBaseUrlLabel').textContent   = labelMap[provider] || 'Endpoint URL';
    document.getElementById('sBaseUrl').placeholder        = PROVIDER_DEFAULT_URLS[provider] || '';
    const hintEl = document.getElementById('sBaseUrlHint');
    if (hintEl) hintEl.textContent = hintMap[provider] || '';

    // Pre-fill URL only when switching providers and the field is empty or has a previous default
    const urlField    = document.getElementById('sBaseUrl');
    const allDefaults = Object.values(PROVIDER_DEFAULT_URLS);
    if (!urlField.value || allDefaults.includes(urlField.value)) {
        urlField.value = PROVIDER_DEFAULT_URLS[provider] || '';
    }
}

// Show Ollama model list as suggestions when typing in the model field
async function onModelInput(value) {
    const box = document.getElementById('modelSuggestions');
    const provider = document.getElementById('sProvider').value || 'ollama';
    if (provider !== 'ollama' || value.length < 1) { box.style.display = 'none'; return; }
    try {
        const res  = await fetch('/api/model/status');
        const data = await res.json();
        const all  = (data.ollamaModels || []).filter(m => m.toLowerCase().includes(value.toLowerCase()));
        if (!all.length) { box.style.display = 'none'; return; }
        box.innerHTML = all.map(m =>
            `<div class="model-suggestion-item" onmousedown="event.preventDefault();pickModel('${m}')">${m}</div>`
        ).join('');
        box.style.display = 'block';
    } catch (e) { box.style.display = 'none'; }
}

function pickModel(name) {
    document.getElementById('sModel').value = name;
    document.getElementById('modelSuggestions').style.display = 'none';
}

async function testConnection() {
    const testBtn  = document.getElementById('sTestBtn');
    const statusEl = document.getElementById('sConnStatus');
    testBtn.disabled = true;
    statusEl.innerHTML = '<span class="conn-testing">Testing…</span>';

    const provider = document.getElementById('sProvider').value || 'ollama';
    const model    = document.getElementById('sModel').value.trim() || modelSettings.model;
    const apiKey   = document.getElementById('sApiKey').value.trim();
    const baseUrl  = document.getElementById('sBaseUrl').value.trim();

    try {
        // Test connectivity WITHOUT switching the active provider (body triggers testProviderConfig on the server)
        const payload = { provider, model, temperature: document.getElementById('sTemp').value };
        if (apiKey)  payload.apiKey  = apiKey;
        if (baseUrl) payload.baseUrl = baseUrl;

        const res  = await fetch('/api/model/test', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();

        if (data.connected) {
            const label = data.warning
                ? `⚠ ${data.warning}`
                : `● Connected  ·  ${model}`;
            statusEl.innerHTML = `<span class="conn-ok">${label}</span>`;
        } else {
            statusEl.innerHTML = `<span class="conn-fail">● ${data.error || 'Connection failed'}</span>`;
        }
    } catch (e) {
        statusEl.innerHTML = `<span class="conn-fail">● ${e.message}</span>`;
    } finally {
        testBtn.disabled = false;
    }
}

async function applyModelConfig() {
    const provider    = document.getElementById('sProvider').value || 'ollama';
    const model       = document.getElementById('sModel').value.trim();
    const apiKey      = document.getElementById('sApiKey').value.trim();
    const baseUrl     = document.getElementById('sBaseUrl').value.trim();
    const temperature = parseFloat(document.getElementById('sTemp').value || '0');
    const autoConnect = document.getElementById('sAutoConnect').checked;
    const remember    = document.getElementById('sRemember').checked;

    if (!model) { alert('Please enter a model name.'); return; }

    const payload = { provider, model, temperature: temperature.toString() };
    if (apiKey)  payload.apiKey  = apiKey;
    if (baseUrl) payload.baseUrl = baseUrl;

    try {
        const res  = await fetch('/api/model/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        const data = await res.json();

        if (!data.success) { alert('Could not apply config: ' + (data.error || 'unknown error')); return; }

        modelSettings = { provider, model, apiKey: '', baseUrl, temperature, autoConnect, savedAt: Date.now() };
        if (remember) saveSettings();

        updateHeaderIndicator(data.connected !== false, model);
        closeSettingsModal();
    } catch (e) {
        alert('Failed to reach the server: ' + e.message);
    }
}

async function handleConfigUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    try {
        const res  = await fetch('/api/model/upload-config', { method: 'POST', body: formData });
        const data = await res.json();

        if (!data.success) {
            document.getElementById('sConnStatus').innerHTML =
                `<span class="conn-fail">● ${data.error || 'Could not parse config'}</span>`;
            return;
        }

        const provider = data.provider || 'ollama';
        const model    = data.model    || '';

        // Fetch status first so PROVIDER_DEFAULT_URLS has the runtime URL before selectProvider runs
        const statusRes  = await fetch('/api/model/status');
        const statusData = await statusRes.json();
        if (statusData.ollamaBaseUrl)    PROVIDER_DEFAULT_URLS.ollama    = statusData.ollamaBaseUrl;
        if (statusData.openAiBaseUrl)    PROVIDER_DEFAULT_URLS.openai    = statusData.openAiBaseUrl;
        if (statusData.anthropicBaseUrl) PROVIDER_DEFAULT_URLS.anthropic = statusData.anthropicBaseUrl;

        // Populate form fields
        selectProvider(provider);
        document.getElementById('sModel').value = model;

        const runtimeUrl = statusData[provider === 'openai' ? 'openAiBaseUrl'
                                     : provider === 'anthropic' ? 'anthropicBaseUrl'
                                     : 'ollamaBaseUrl'] || '';
        document.getElementById('sBaseUrl').value = runtimeUrl;

        if (data.temperature !== undefined) {
            document.getElementById('sTemp').value = data.temperature;
            document.getElementById('sTempDisplay').textContent =
                parseFloat(data.temperature).toFixed(2);
        }

        // Show masked key indicator — leave field empty so Save & Apply keeps the server key
        const apiKeyEl = document.getElementById('sApiKey');
        apiKeyEl.value = '';
        apiKeyEl.placeholder = '●●●●●●●● (loaded from file — leave blank to keep)';

        // Prompt user to test then save — do NOT close modal or turn header green yet
        document.getElementById('sConnStatus').innerHTML =
            '<span class="conn-testing">✓ Config loaded — click Test Connection, then Save &amp; Apply</span>';

    } catch (e) {
        document.getElementById('sConnStatus').innerHTML =
            `<span class="conn-fail">● Upload failed: ${e.message}</span>`;
    }
}

function updateHeaderIndicator(connected, model) {
    const dot   = document.getElementById('connDot');
    const label = document.getElementById('headerModelLabel');
    const conn  = document.getElementById('headerConn');
    if (dot)   dot.className   = 'conn-dot ' + (connected ? 'conn-dot-green' : 'conn-dot-red');
    if (label) label.textContent = model || '—';
    if (conn)  conn.classList.toggle('disconnected', !connected);
}

// ── Auto-connect on page load ─────────────────────────────────────────────
async function autoConnect() {
    const hasSaved = loadSettings();

    // Always fetch status to seed URLs and sync model name
    try {
        const res  = await fetch('/api/model/status');
        const data = await res.json();
        if (data.ollamaBaseUrl)    PROVIDER_DEFAULT_URLS.ollama    = data.ollamaBaseUrl;
        if (data.openAiBaseUrl)    PROVIDER_DEFAULT_URLS.openai    = data.openAiBaseUrl;
        if (data.anthropicBaseUrl) PROVIDER_DEFAULT_URLS.anthropic = data.anthropicBaseUrl;

        if (hasSaved && modelSettings.autoConnect) {
            // User previously saved settings — trust server state as connected
            if (!modelSettings.model) modelSettings.model = data.model || '';
            updateHeaderIndicator(data.connected, data.model || modelSettings.model);
        } else {
            // No saved settings — stay red until user explicitly tests & saves
            updateHeaderIndicator(false, 'Not configured');
        }
    } catch (e) {
        updateHeaderIndicator(false, 'Not configured');
    }
}

setInterval(async () => {
    try {
        const res  = await fetch('/api/model/status');
        const data = await res.json();
        updateHeaderIndicator(data.connected, data.model);
    } catch (e) {
        const dot = document.getElementById('connDot');
        if (dot) dot.className = 'conn-dot conn-dot-red';
    }
}, 30000);

autoConnect();
