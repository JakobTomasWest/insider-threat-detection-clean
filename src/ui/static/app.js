let state = {
    sidebarTab: 'alerts', // 'alerts' or 'watchlist'
    sidebarFilter: 'all', // 'all' or 'escalated'
    detailTab: 'rules',   // 'rules', 'ml', 'anomaly', 'forecast'
    currentUser: null,
    paused: false,
    users: [],            // Cache users for filtering
    currentAlerts: [],    // Cache alerts for filtering
    forecastSummary: null,

    // Risk / notes state
    currentUserRiskMeta: null,
    showCaseNotes: false,
    userTimelineData: [],
    timelineWindowSize: 30, // Default to 30d as requested or prevalent
    timelineEndDay: null,   // The right-most day of the visible window
    timelineStartIndex: 0,  // DEPRECATED, but kept for safe removal if needed

    // Heartbeat State
    heartbeatDataFull: [],
    heartbeatStartIndex: 0,
    heartbeatWindowSize: 7,

    // Timeline Hitboxes
    timelineHitboxes: []
};

// Helper: Add N days to YYYY-MM-DD
function addDays(dateStr, n) {
    const d = new Date(dateStr);
    // Add time component to avoid UTC rolling issues if local time matches
    // But input YYYY-MM-DD is usually treated as UTC in JS new Date("YYYY-MM-DD")
    // Let's be careful.
    // Actually, new Date("2025-01-01") is parsed as UTC.
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().split('T')[0];
}

// Helper: Diff in days between two YYYY-MM-DD
function daysBetween(d1, d2) {
    const a = new Date(d1);
    const b = new Date(d2);
    const diffTime = b - a;
    return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
}

// Helper: Format Date YYYY-MM-DD -> MM-DD-YYYY
function formatDate(dateStr) {
    if (!dateStr) return '';
    const parts = dateStr.split('-');
    if (parts.length !== 3) return dateStr;
    return `${parts[1]}-${parts[2]}-${parts[0]}`;
}

// Helper: Format Date YYYY-MM-DD -> MM-DD-YY
function formatShortDateUS(day) {
    if (!day) return "";
    const parts = day.split("-");
    if (parts.length !== 3) return day;
    const [y, m, d] = parts;
    return `${m}-${d}-${y.slice(2)}`;
}

// Helper: Format Role (e.g. "ITAdmin" -> "IT Admin")
function formatRole(role) {
    if (!role) return '';
    return role
        .replace(/([a-z])([A-Z])/g, '$1 $2')
        .replace(/\s+/g, ' ')
        .trim();
}

function buildCaseNotesHtml(rm, currentDay) {
    if (!rm || !rm.analyst_notes || rm.analyst_notes.length === 0) {
        return `<div class="case-notes-empty">No case notes yet for this user.</div>`;
    }

    const notesHtml = rm.analyst_notes
        .filter(n => !currentDay || n.day <= currentDay)
        .map(n => {
            let dateStr = formatDate(n.day);
            const isRule = n.kind && n.kind.startsWith('rule_');
            let rowClass = isRule ? 'case-note-rule' : 'case-note-high-level';

            if (n.kind === 'termination') {
                dateStr = formatShortDateUS(n.day);
                rowClass = 'case-note-high-level';
            }

            return `
                <div class="case-note-row ${rowClass}">
                    <div class="case-note-date">${dateStr}</div>
                    <div class="case-note-msg">${n.message}</div>
                </div>
            `;
        })
        .join('');

    return notesHtml;
}

// Init
async function init() {
    console.log("App JS v3 loaded");
    await fetchState();
    await loadForecastSummary();
    await loadUsers();
    await loadHeartbeat();
    // Attach click handler for the User Risk Timeline (once)
    const rtc = document.getElementById('riskTimelineCanvas');
    if (rtc) {
        rtc.addEventListener('click', onRiskTimelineClick);
    }

    // Poll for updates (faster polling for smoother animation)
    setInterval(async () => {
        if (!state.paused) {
            await fetchState();
            await loadForecastSummary();
            await loadUsers();
            await loadHeartbeat();
            if (state.currentUser) {
                await loadUserAlerts(state.currentUser);
            }
        }
    }, 1000);
}

// API Calls
async function fetchState() {
    const res = await fetch('/api/state');
    const data = await res.json();
    state.paused = data.paused;

    const latestDayEl = document.getElementById('latestDayDisplay');
    if (latestDayEl) {
        latestDayEl.textContent = formatDate(data.current_day);
    }
    state.currentDay = data.current_day;

    const frozenLabel = document.getElementById('frozenLabel');
    if (frozenLabel) {
        frozenLabel.style.display = data.paused ? 'inline' : 'none';
    }

    // Update the Heartbeat header title
    const hbTitle = document.querySelector('.heartbeat-header h3');
    if (hbTitle) {
        hbTitle.textContent = 'Highest Risk Users';
    }

    updatePauseBtn();
    updateScrollBtns();
    updateTimelineControls();
}

async function togglePause() {
    const newState = !state.paused;
    await fetch('/api/state/pause', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ paused: newState })
    });
    await fetchState();
    // Force refresh immediately
    await loadUsers();
    await loadHeartbeat();
    if (state.currentUser) loadUserAlerts(state.currentUser);
}

function updatePauseBtn() {
    const icon = document.getElementById('pauseIcon');
    const text = document.getElementById('pauseText');
    if (!icon || !text) return;

    if (state.paused) {
        icon.textContent = '▶';
        text.textContent = 'Resume';
    } else {
        icon.textContent = '⏸';
        text.textContent = 'Pause';
    }
}

function updateScrollBtns() {
    const left = document.getElementById('scrollLeftBtn');
    const right = document.getElementById('scrollRightBtn');
    if (!left || !right) return;

    if (state.paused) {
        left.classList.remove('disabled');
        right.classList.remove('disabled');
    } else {
        left.classList.add('disabled');
        right.classList.add('disabled');
    }
}

async function loadUsers() {
    const previousUser = state.currentUser;

    let url = '/api/users?tab=';
    if (state.sidebarTab === 'watchlist') {
        url += 'watchlist';
    } else {
        url += state.sidebarFilter;
    }

    const res = await fetch(url);
    state.users = await res.json();

    // Sort by max_forecast descending if in watchlist mode
    if (state.sidebarTab === 'watchlist') {
        state.users.sort((a, b) => (b.max_forecast || 0) - (a.max_forecast || 0));
    }

    renderSidebar();

    // Selection Logic: Preserve or Select First
    if (previousUser && state.users.find(u => u.user_key === previousUser)) {
        // User preserved. Re-render alerts to update context (e.g. summary strip)
        // We re-fetch alerts to be safe and ensure UI is consistent
        await loadUserAlerts(previousUser);
    } else {
        // New selection
        if (state.users.length > 0) {
            // Select first user
            selectUser(state.users[0].user_key);
        } else {
            // No users available
            state.currentUser = null;
            document.getElementById('emptyState').style.display = 'block';
            document.getElementById('userView').style.display = 'none';
        }
    }
}

async function loadHeartbeat() {
    let url = '/api/heartbeat';
    const params = new URLSearchParams();

    // Determine days
    if (state.paused) params.append('days', '180');
    else params.append('days', '7');

    // Determine scope: global Alerts vs Watchlist aggregate vs Per-User
    if (state.sidebarTab === 'watchlist') {
        if (state.currentUser) {
            params.append('user_key', state.currentUser);
        } else {
            params.append('scope', 'watchlist');
        }
    } else {
        // Alerts tab: always global scope=alerts (default)
        params.append('scope', 'alerts');
    }

    const res = await fetch(`${url}?${params.toString()}`);
    const data = await res.json();

    if (state.paused) {
        state.heartbeatDataFull = data;
        // Only reset index if we just paused? Or always?
        // Let's try to preserve position if we are already paused? 
        // But loadHeartbeat isn't called in loop when paused.
        // So this is likely the first load after pause.
        if (state.heartbeatDataFull.length > 0) {
            state.heartbeatStartIndex = Math.max(0, state.heartbeatDataFull.length - state.heartbeatWindowSize);
        }
        renderHeartbeat();
    } else {
        renderHeartbeat(data);
    }
}

function scrollHeartbeat(direction) {
    if (!state.paused) return;

    const newIndex = state.heartbeatStartIndex + direction;
    const maxIndex = Math.max(0, state.heartbeatDataFull.length - state.heartbeatWindowSize);

    if (newIndex >= 0 && newIndex <= maxIndex) {
        state.heartbeatStartIndex = newIndex;
        renderHeartbeat();
    }
}

function setTimelineWindowSize(value) {
    state.timelineWindowSize = parseInt(value, 10);
    // Don't reset index or endDay here; just re-render. 
    // If not paused, renderRiskTimeline will auto-snap to latest.
    // If paused, it will use the existing timelineEndDay with the new window size.
    renderRiskTimeline();
}

function scrollTimeline(direction) {
    if (!state.paused) return;
    if (!state.timelineEndDay) return;

    // Shift by 1 day
    // Direction is +1 (right/future) or -1 (left/past)
    // We want to shift the END DAY.
    const newEndDay = addDays(state.timelineEndDay, direction);

    // Bounds check?
    // Right bound: Should not go far beyond "today" or the latest available data?
    // Let's cap it at state.currentDay (simulation time).
    if (direction > 0 && state.currentDay && newEndDay > state.currentDay) {
        // Don't scroll past 'Today'
        return;
    }

    // Left bound: Maybe arbitrarily far back? Or limit to earliest data?
    // Let's just let them scroll back.

    state.timelineEndDay = newEndDay;
    renderRiskTimeline();
}

function updateTimelineControls() {
    const left = document.getElementById('timelineLeftBtn');
    const right = document.getElementById('timelineRightBtn');
    if (!left || !right) return;

    if (state.paused) {
        left.classList.remove('disabled');
        right.classList.remove('disabled');
    } else {
        left.classList.add('disabled');
        right.classList.add('disabled');
    }
}

async function loadUserAlerts(userKey) {
    const res = await fetch(`/api/users/${userKey}/alerts`);
    const data = await res.json();

    state.currentAlerts = data.alerts || [];
    state.currentUserRiskMeta = data.risk_meta || null;

    // Build unique dictionary of days -> aggregated scores
    const dayMap = {};

    (state.currentAlerts || []).forEach(a => {
        if (!dayMap[a.day]) {
            dayMap[a.day] = {
                day: a.day,
                rules_score: 0,
                ml_score: 0,
                anomaly_score: 0,
                forecast_score: 0,
                ensemble_score: 0,
                escalated: false,
                has_rule_hit: false,
                alert_ids: [] // Store all alert IDs for this day
            };
        }
        const entry = dayMap[a.day];
        
        // Aggregation: MAX for scores
        entry.rules_score    = Math.max(entry.rules_score,    a.rules_score    || 0);
        entry.ml_score       = Math.max(entry.ml_score,       a.ml_score       || 0);
        entry.anomaly_score  = Math.max(entry.anomaly_score,  a.anomaly_score  || 0);
        entry.forecast_score = Math.max(entry.forecast_score, a.forecast_score || 0);
        entry.ensemble_score = Math.max(entry.ensemble_score, a.ensemble_score || 0);
        
        // Aggregation: OR for escalated
        if (a.escalated) entry.escalated = true;
        
        // Track & prioritize rule alerts for this day
        if (a.rule_hits && a.rule_hits.length > 0) {
            entry.has_rule_hit = true;
            // Make sure rule alerts are the "rep" alert for this day
            entry.alert_ids.unshift(a.alert_id);
        } else {
            entry.alert_ids.push(a.alert_id);
        }
    });

    // Convert to sorted array
    // Filter out any entries with potentially invalid dates if necessary, but usually safe.
    state.userTimelineData = Object.values(dayMap).sort((a, b) => a.day.localeCompare(b.day));

    console.log("Aggregated Timeline data", state.userTimelineData);
    console.log("Timeline data", state.userTimelineData);

    renderUserAlerts(); // Updates watchlist strip
    renderRiskTimeline();
    renderUserHeader();
    renderHeroOrCaseNotes();
}

function renderUserHeader() {
    const userKey = state.currentUser;
    if (!userKey) return;

    const userObj = state.users.find(u => u.user_key === userKey);
    const displayName = userObj ? userObj.name : userKey;
    const role = userObj ? formatRole(userObj.user_role) : '';

    const main = document.getElementById('userHeaderMain');
    const inline = document.getElementById('userHeaderCaseNotes');
    if (!main || !inline) return;

    if (!state.showCaseNotes) {
        // Normal header mode
        main.style.display = 'flex';
        inline.style.display = 'none';

        const headerEl = document.getElementById('selectedUserKey');
        if (!headerEl) return;

        let html = `User: ${displayName}`;
        if (role) {
            html += ` <span style="color:var(--text-muted); font-weight:400;">· ${role}</span>`;
        }

        const termDay = state.currentUserRiskMeta?.termination_day;
        const currentDay = state.currentDay;
        // if (termDay && currentDay && currentDay >= termDay) {
        //     const dateStr = formatShortDateUS(termDay);
        //     html += ` <span class="terminated-pill" style="
        //     background-color: #334155; 
        //     color: #cbd5e1; 
        //     font-size: 0.75rem; 
        //     padding: 2px 6px; 
        //     border-radius: 4px; 
        //     margin-left: 8px; 
        //     vertical-align: middle;
        //     font-weight: 500;
        // ">Terminated ${dateStr}</span>`;
        // }

        headerEl.innerHTML = html;
    } else {
        // Inline case-notes mode
        main.style.display = 'none';
        inline.style.display = 'block';

        const rm = state.currentUserRiskMeta;
        const notesHtml = buildCaseNotesHtml(rm, state.currentDay);

        inline.innerHTML = `
            <div class="user-header-casenotes-header">
                <span>Case notes for ${displayName}${role ? ' · ' + role : ''}</span>
                <button class="user-header-casenotes-close" onclick="toggleCaseNotes()">✕ Close</button>
            </div>
            <div class="case-notes-timeline">
                ${notesHtml}
            </div>
        `;
    }
}


async function loadAlertDetails(alertId) {
    const res = await fetch(`/api/alerts/${alertId}`);
    const alert = await res.json();
    openModal(alert);
}

async function loadAlertWindow(alertId, userKey, day) {
    const panel = document.getElementById('windowViewPanel');
    const title = document.getElementById('windowTitle');
    const tbody = document.getElementById('windowTableBody');

    title.textContent = `Loading window for ${userKey}...`;
    panel.style.display = 'block';
    tbody.innerHTML = '';

    try {
        const res = await fetch(`/api/alerts/${alertId}/window`);
        if (!res.ok) throw new Error("Failed to load");
        const data = await res.json();

        title.textContent = `14-day window for ${userKey} centered on ${day}`;
        renderWindowTable(data.rows);
    } catch (e) {
        title.textContent = `Error loading window for ${userKey}`;
        tbody.innerHTML = `<tr><td colspan="6" style="color:var(--danger)">Failed to load window data.</td></tr>`;
    }
}

function closeWindowPanel() {
    document.getElementById('windowViewPanel').style.display = 'none';
}

function renderWindowTable(rows) {
    const tbody = document.getElementById('windowTableBody');
    tbody.innerHTML = '';

    if (!rows || rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6">No data available.</td></tr>`;
        return;
    }

    rows.forEach(r => {
        const tr = document.createElement('tr');
        // Format floats
        const fmt = (n) => n !== undefined && n !== null ? n.toFixed(2) : 'N/A';
        const fmtInt = (n) => n !== undefined && n !== null ? n : 'N/A';

        tr.innerHTML = `
            <td>${formatDate(r.day)}</td>
            <td>${fmt(r.logon_after_hours_rate)}</td>
            <td>${fmt(r.device_after_hours_rate)}</td>
            <td>${fmt(r.file_after_hours_rate)}</td>
            <td>${fmtInt(r.device_n_usb_connects)}</td>
            <td>${fmtInt(r.http_n_wikileaks)}</td>
        `;
        tbody.appendChild(tr);
    });
}

// Sidebar Logic
function setSidebarTab(tab) {
    state.sidebarTab = tab;
    // Update tab UI
    document.querySelectorAll('.sidebar-tab').forEach(b => b.classList.remove('active'));
    // Assuming order: 0=Alerts, 1=Watchlist
    const tabs = document.querySelectorAll('.sidebar-tab');
    if (tab === 'alerts') tabs[0].classList.add('active');
    else tabs[1].classList.add('active');

    // Toggle Filters Visibility
    const filterContainer = document.getElementById('alertFilters');
    if (tab === 'alerts') filterContainer.style.display = 'flex';
    else filterContainer.style.display = 'none';

    // Reload data (loadUsers will handle selection preservation)
    loadUsers();
    loadHeartbeat();
}

function setSidebarFilter(filter) {
    state.sidebarFilter = filter;
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    const buttons = document.querySelectorAll('.filter-btn');
    if (filter === 'all') buttons[0].classList.add('active');
    else buttons[1].classList.add('active');

    loadUsers(); // Reload users with new filter
}

function renderSidebar() {
    const list = document.getElementById('userList');
    document.getElementById('userCount').textContent = state.users.length;
    list.innerHTML = '';

    state.users.forEach(u => {
        const div = document.createElement('div');
        div.className = `user-row ${state.currentUser === u.user_key ? 'active' : ''}`;
        div.onclick = () => selectUser(u.user_key, div);

        let contentHtml = '';

        if (state.sidebarTab === 'watchlist') {
            // --- Watchlist Tab ---
            // Layout:
            // Line 1: Name .......... [F:xx] [Total Alerts]
            // Line 2: (user_id)

            const maxForecast = u.max_forecast != null ? u.max_forecast.toFixed(2) : '0.00';

            const badgesHtml = `
                <span class="alert-count-badge" title="Max Forecast">F:${maxForecast}</span>
                <span class="alert-count-badge" title="Total Alerts">${u.total_alerts}</span>
            `;

            contentHtml = `
                <div style="width:100%">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.1rem;">
                        <span class="user-name">${u.name}</span>
                        <div class="badge-container">${badgesHtml}</div>
                    </div>
                    <div class="user-id" style="font-size:0.8rem; color:var(--text-muted);">(${u.user_key})</div>
                </div>
            `;
        } else {
            // --- Alerts Tab ---
            // Layout:
            // Line 1: [dot] Full Name (user_id) ........ badges
            // (user_id) uses the muted .user-id style; role is NOT shown here.

            const severityClass = u.escalated_alerts > 0 ? 'sev-escalated' : 'sev-normal';

            let badgesHtml = `<span class="alert-count-badge" title="Total Alerts">${u.total_alerts}</span>`;
            if (u.escalated_alerts > 0) {
                badgesHtml += `<span class="escalated-badge" title="Escalated Alerts">${u.escalated_alerts}</span>`;
            }

            contentHtml = `
                <div style="width:100%">
                    <div style="display:flex; align-items:center; justify-content:space-between;">
                        <div style="display:flex; align-items:center; gap:0.25rem;">
                            <span class="severity-dot ${severityClass}"></span>
                            <span class="user-name">${u.name}</span>
                            <span class="user-id" style="font-size:0.8rem; color:var(--text-muted);">(${u.user_key})</span>
                        </div>
                        <div class="badge-container">${badgesHtml}</div>
                    </div>
                </div>
            `;
        }

        div.innerHTML = contentHtml;
        list.appendChild(div);
    });
}

function selectUser(userKey, el) {
    state.currentUser = userKey;
    renderSidebar(); // Update active class

    document.getElementById('emptyState').style.display = 'none';
    document.getElementById('userView').style.display = 'block';

    // Header update is handled in loadUserAlerts -> renderUserHeader
    // But we set a temporary placeholder or just wait for loadUserAlerts
    // document.getElementById('selectedUserKey').textContent = `User: ${displayName}`;

    closeWindowPanel();
    loadUserAlerts(userKey);

    // If in Watchlist tab, selecting a user switches heartbeat to per-user mode
    if (state.sidebarTab === 'watchlist') {
        loadHeartbeat();
    }
}

// Detail Tab Logic
function switchDetailTab(tab) {
    state.detailTab = tab;
    document.querySelectorAll('.detail-tab-btn').forEach(b => b.classList.remove('active'));
    const buttons = document.querySelectorAll('.detail-tab-btn');
    // 0=Rules, 1=ML, 2=Anomaly, 3=Forecast
    if (tab === 'rules') buttons[0].classList.add('active');
    if (tab === 'ml') buttons[1].classList.add('active');
    if (tab === 'anomaly') buttons[2].classList.add('active');
    if (tab === 'forecast') buttons[3].classList.add('active');

    renderRiskTimeline();
}

function renderUserAlerts() {
    // Handle Watchlist Summary Strip
    const strip = document.getElementById('watchlistSummaryStrip');
    if (state.sidebarTab === 'watchlist') {
        strip.style.display = 'flex';
        // Compute metrics
        // Max Forecast
        const maxForecast = Math.max(...state.currentAlerts.map(a => a.forecast_score || 0), 0);
        document.getElementById('wsMax').textContent = maxForecast.toFixed(2);

        // First Forecast Day (>= 0.6 threshold assumption)
        const THRESHOLD = 0.6;
        // Alerts are sorted descending by day. Need to find earliest.
        const sortedAsc = [...state.currentAlerts].sort((a, b) => a.day.localeCompare(b.day));
        const firstForecast = sortedAsc.find(a => (a.forecast_score || 0) >= THRESHOLD);
        const firstForecastDay = firstForecast ? formatDate(firstForecast.day) : null;
        document.getElementById('wsFirst').textContent = firstForecastDay || 'None';

        // First Escalation (>= firstForecastDay)
        let firstEscalationDay = null;
        if (firstForecastDay) {
            const firstEsc = sortedAsc.find(a => a.escalated && a.day >= firstForecast.day);
            firstEscalationDay = firstEsc ? formatDate(firstEsc.day) : null;
        }
        document.getElementById('wsEsc').textContent = firstEscalationDay || 'None';

        // Lead Time
        if (firstForecastDay && firstEscalationDay) {
            const d1 = new Date(firstForecastDay);
            const d2 = new Date(firstEscalationDay);
            const diffTime = Math.abs(d2 - d1);
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            document.getElementById('wsLead').textContent = `${diffDays} days`;
        } else {
            document.getElementById('wsLead').textContent = 'N/A';
        }

    } else {
        strip.style.display = 'none';
    }
}

function renderRiskTimeline() {
    const canvas = document.getElementById('riskTimelineCanvas');
    if (!canvas) {
        state.timelineHitboxes = [];
        return;
    }

    const ctx = canvas.getContext('2d');
    if (!ctx) {
        state.timelineHitboxes = [];
        return;
    }

    // Ensure canvas matches the rendered size
    const rect = canvas.getBoundingClientRect();
    if (rect.width > 0 && rect.height > 0) {
        canvas.width = rect.width;
        canvas.height = rect.height;
    }

    const width = canvas.width;
    const height = canvas.height;

    ctx.clearRect(0, 0, width, height);

    state.timelineHitboxes = [];

    // 1. Determine Window Anchor (End Day)
    let endDay;
    const data = state.userTimelineData || [];

    // Find max data day
    let maxDataDay = state.currentDay;
    if (data.length > 0) {
        maxDataDay = data[data.length - 1].day;
    } else {
        maxDataDay = state.currentDay || new Date().toISOString().split('T')[0];
    }

    if (!state.paused) {
        // "Lives in the now" behavior
        let cDay = state.currentDay || "9999-99-99";
        if (cDay < maxDataDay) {
            endDay = cDay;
        } else {
            endDay = maxDataDay;
        }
        state.timelineEndDay = endDay;
    } else {
        // Paused: Use stored endDay, or fallback
        endDay = state.timelineEndDay || maxDataDay;
    }

    if (!endDay) {
        ctx.fillStyle = '#888';
        ctx.font = '12px Inter, system-ui, sans-serif';
        ctx.fillText('No timeline data available.', 16, height / 2);
        return;
    }

    // 2. Build Daily Series
    const startDay = addDays(endDay, -(state.timelineWindowSize - 1));

    // Generate dates
    const daysInWindow = [];
    let curr = startDay;
    let loops = 0;
    while (curr <= endDay && loops < 1000) {
        daysInWindow.push(curr);
        curr = addDays(curr, 1);
        loops++;
    }

    // Join with Data
    const plotData = daysInWindow.map(d => {
        const match = data.find(item => item.day === d);
        if (match) {
            return match;
        } else {
            // Empty / Zero record
            return {
                day: d,
                rules_score: 0,
                ml_score: 0,
                anomaly_score: 0,
                forecast_score: 0,
                ensemble_score: 0,
                escalated: false,
                alert_ids: []
            };
        }
    });

    // 3. Render
    const paddingLeft = 40;
    const paddingRight = 16;
    const paddingTop = 16;
    const paddingBottom = 24;

    const plotWidth = width - paddingLeft - paddingRight;
    const plotHeight = height - paddingTop - paddingBottom;

    const nPoints = plotData.length;
    const xForIndex = (i) => paddingLeft + (i / (nPoints - 1)) * plotWidth;
    const yFor = (score) => paddingTop + (1 - score) * plotHeight;

    const detectors = [
        { key: 'rules_score', color: '#3b82f6', label: 'rules' },
        { key: 'ml_score', color: '#a855f7', label: 'ml' },
        { key: 'anomaly_score', color: '#f97316', label: 'anomaly' },
        { key: 'forecast_score', color: '#ef4444', label: 'forecast' },
        { key: 'ensemble_score', color: '#e5e7eb', label: 'ensemble' },
    ];
    const activeTab = state.detailTab || 'rules';

    const detectorsOrdered = [...detectors].sort((a, b) => {
        if (a.label === activeTab && b.label !== activeTab) return 1;
        if (b.label === activeTab && a.label !== activeTab) return -1;
        return 0;
    });

    // Axes
    ctx.strokeStyle = '#444';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(paddingLeft, paddingTop);
    ctx.lineTo(paddingLeft, height - paddingBottom);
    ctx.moveTo(paddingLeft, height - paddingBottom);
    ctx.lineTo(width - paddingRight, height - paddingBottom);
    ctx.stroke();

    // Grid & Labels
    ctx.fillStyle = '#64748b';
    ctx.font = '10px "JetBrains Mono", "Fira Code", monospace';
    [0, 0.5, 1].forEach(val => {
        const y = paddingTop + (1 - val) * plotHeight;
        ctx.fillText(val.toFixed(1), 4, y + 3);
        ctx.beginPath();
        ctx.setLineDash([2, 4]);
        ctx.strokeStyle = 'rgba(255,255,255,0.1)';
        ctx.moveTo(paddingLeft, y);
        ctx.lineTo(width - paddingRight, y);
        ctx.stroke();
        ctx.setLineDash([]);
    });

    // X-axis labels
    const maxXTicks = 6;
    const xStep = Math.max(1, Math.floor((nPoints - 1) / (maxXTicks - 1))) || 1;

    ctx.fillStyle = '#64748b';
    ctx.textAlign = 'center';

    const xTickIndices = [];
    for (let i = 0; i < nPoints; i += xStep) {
        xTickIndices.push(i);
    }
    if (xTickIndices[xTickIndices.length - 1] !== nPoints - 1) {
        xTickIndices.push(nPoints - 1);
    }

    let lastLabelX = -Infinity;
    xTickIndices.forEach(idx => {
        const pt = plotData[idx];
        const x = xForIndex(idx);

        ctx.beginPath();
        ctx.setLineDash([2, 4]);
        ctx.strokeStyle = 'rgba(255,255,255,0.05)';
        ctx.moveTo(x, paddingTop);
        ctx.lineTo(x, height - paddingBottom);
        ctx.stroke();
        ctx.setLineDash([]);

        if (x - lastLabelX >= 40) {
            const dLabel = pt.day.slice(5); // MM-DD
            ctx.fillText(dLabel, x, height - paddingBottom + 12);
            lastLabelX = x;
        }
    });
    ctx.textAlign = 'left';

    // Lines
    detectorsOrdered.forEach(det => {
        const isPrimary = (det.label === activeTab || det.label === 'ensemble');

        ctx.globalAlpha = isPrimary ? 1.0 : 0.3;
        ctx.strokeStyle = det.color;
        ctx.lineWidth = isPrimary ? 3 : 1.5;
        ctx.lineJoin = 'round';
        ctx.lineCap = 'round';

        if (isPrimary) {
            ctx.shadowColor = det.color;
            ctx.shadowBlur = 10;
        } else {
            ctx.shadowBlur = 0;
        }

        // Filter points where this detector has a value > 0 to interpolate gaps
        const validPoints = [];
        plotData.forEach((pt, i) => {
            const val = pt[det.key] || 0;
            if (val > 0) {
                validPoints.push({ val, i });
            }
        });

        if (validPoints.length > 0) {
            ctx.beginPath();
            let first = true;
            validPoints.forEach(p => {
                const x = xForIndex(p.i);
                const capped = Math.max(0, Math.min(1, p.val));
                const y = yFor(capped);

                if (first) {
                    ctx.moveTo(x, y);
                    first = false;
                } else {
                    ctx.lineTo(x, y);
                }
            });
            ctx.stroke();
        }
        ctx.globalAlpha = 1.0;
        ctx.shadowBlur = 0;
    });

    // Dots & Hitboxes
    const radius = 4;
    plotData.forEach((pt, i) => {
        const hasAlerts = pt.alert_ids && pt.alert_ids.length > 0;
        if (!hasAlerts) return;

        const x = xForIndex(i);

        detectors.forEach(det => {
            const val = pt[det.key];
            if (val == null || val <= 0) return;

            const capped = Math.max(0, Math.min(1, val));
            const y = yFor(capped);

            if (det.label === 'ensemble' && pt.escalated) {
                ctx.beginPath();
                ctx.fillStyle = 'rgba(250, 204, 21, 0.3)';
                ctx.arc(x, y, radius + 4, 0, Math.PI * 2);
                ctx.fill();
            }

            ctx.beginPath();
            ctx.fillStyle = det.color;
            ctx.arc(x, y, radius, 0, Math.PI * 2);
            ctx.fill();
            ctx.strokeStyle = '#fff';
            ctx.lineWidth = 1;
            ctx.stroke();

            const repAlertId = pt.alert_ids[0];
            state.timelineHitboxes.push({
                x,
                y,
                r: radius + 3,
                alertId: repAlertId
            });
        });
    });

    // Termination Line
    const term = state.currentUserRiskMeta?.termination_day;
    if (term) {
        const termTime = new Date(term).getTime();
        const startDayTime = new Date(startDay).getTime();
        const endDayTime = new Date(endDay).getTime();

        if (termTime >= startDayTime && termTime <= endDayTime) {
            const span = endDayTime - startDayTime;
            const ratio = span === 0 ? 0 : (termTime - startDayTime) / span;
            const x = paddingLeft + ratio * plotWidth;

            ctx.beginPath();
            ctx.strokeStyle = 'rgba(148, 163, 184, 0.5)';
            ctx.lineWidth = 2;
            ctx.setLineDash([4, 4]);
            ctx.moveTo(x, paddingTop);
            ctx.lineTo(x, height - paddingBottom);
            ctx.stroke();
            ctx.setLineDash([]);
        }
    }
}



function onRiskTimelineClick(evt) {
    const canvas = document.getElementById('riskTimelineCanvas');
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    const x = evt.clientX - rect.left;
    const y = evt.clientY - rect.top;

    const hitboxes = state.timelineHitboxes || [];
    if (!hitboxes.length) return;

    for (const hb of hitboxes) {
        const dx = x - hb.x;
        const dy = y - hb.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist <= hb.r) {
            if (hb.alertId) {
                loadAlertDetails(hb.alertId);
            }
            return;
        }
    }
}

function renderForecastTable() {
    const tbody = document.getElementById('forecastTableBody');
    tbody.innerHTML = '';

    // Filter alerts with forecast_score
    const filteredAlerts = state.currentAlerts.filter(a => a.forecast_score !== undefined && a.forecast_score !== null);

    filteredAlerts.forEach(a => {
        const tr = document.createElement('tr');
        tr.onclick = () => loadAlertDetails(a.alert_id);

        const scoreClass = a.forecast_score > 0.6 ? 'forecast-score-high' : '';
        const escalatedText = a.escalated ? 'Yes' : 'No';
        const escalatedClass = a.escalated ? 'status-cell-escalated' : '';

        tr.innerHTML = `
            <td>${formatDate(a.day)}</td>
            <td class="${scoreClass}">${a.forecast_score.toFixed(2)}</td>
            <td class="${escalatedClass}">${escalatedText}</td>
            <td>${a.ensemble_score.toFixed(2)}</td>
            <td>
                <button class="view-window-btn" onclick="event.stopPropagation(); loadAlertWindow('${a.alert_id}', '${a.user_key}', '${a.day}')">
                    View
                </button>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

function renderHeartbeat(data) {
    let displayData = data;
    if (!displayData && state.paused) {
        displayData = state.heartbeatDataFull.slice(
            state.heartbeatStartIndex,
            state.heartbeatStartIndex + state.heartbeatWindowSize
        );
    }

    // Update Title based on mode
    const titleEl = document.querySelector('.heartbeat-header h3');
    if (state.sidebarTab === 'watchlist') {
        if (state.currentUser) {
            titleEl.textContent = `Highest Risk Users`;
        } else {
            titleEl.textContent = 'Highest Risk Users';
        }
    } else {
        titleEl.textContent = 'Highest Risk Users';
    }

    // Update Legend Visibility
    const legend = document.querySelector('.heartbeat-legend');
    if (!legend) {
        // New Hero Panel layout: legend element is not present.
        // Skip legacy heartbeat rendering to avoid null.innerHTML errors.
        return;
    }
    if (state.sidebarTab === 'watchlist') {
        // Show simplified legend for watchlist
        legend.innerHTML = `
            <div class="legend-item"><span class="legend-dot" style="background-color: #ef4444;"></span> Forecast Risk Score</div>
            <div class="legend-item"><span class="legend-dot" style="background-color: #94a3b8;"></span> Total Alerts</div>
        `;
    } else {
        // Global Alerts Mode
        legend.innerHTML = `
            <div class="legend-item"><span class="legend-dot legend-rule"></span> Rule-Based</div>
            <div class="legend-item"><span class="legend-dot legend-ml"></span> ML</div>
            <div class="legend-item"><span class="legend-dot legend-anomaly"></span> Anomaly</div>
        `;
    }

    if (!displayData) return;

    const canvas = document.getElementById('heartbeatCanvas');
    if (!canvas) {
        // Hero Panel layout: no legacy heartbeat canvas in the DOM.
        // Skip drawing instead of relying on a hidden canvas hack.
        return;
    }
    const ctx = canvas.getContext('2d');
    const container = canvas.parentElement || canvas;
    canvas.width = container.offsetWidth;
    canvas.height = container.offsetHeight;

    if (displayData.length < 2) return;

    // Determine max value
    let maxVal = Math.max(
        ...displayData.map(d => d.rule_count || 0),
        ...displayData.map(d => d.ml_count || 0),
        ...displayData.map(d => d.anomaly_count || 0),
        ...displayData.map(d => d.forecast_hit_count || 0),
        1
    );

    // Y-Axis Layout
    const leftMargin = 30;
    const bottomMargin = 20;
    const topMargin = 10;
    const rightMargin = 10;
    const w = canvas.width - leftMargin - rightMargin;
    const h = canvas.height - bottomMargin - topMargin;

    const ticks = [0, Math.round(maxVal / 2), Math.ceil(maxVal)];
    const uniqueTicks = [...new Set(ticks)].sort((a, b) => a - b);

    // Draw Grid & Labels
    ctx.fillStyle = '#94a3b8';
    ctx.font = '10px Inter';
    ctx.textAlign = 'right';
    ctx.strokeStyle = '#334155';
    ctx.lineWidth = 1;

    uniqueTicks.forEach(tick => {
        const y = topMargin + h - (tick / maxVal) * h;
        ctx.beginPath();
        ctx.moveTo(leftMargin, y);
        ctx.lineTo(canvas.width - rightMargin, y);
        ctx.stroke();
        ctx.fillText(tick, leftMargin - 5, y + 3);
    });

    const step = w / (displayData.length - 1);
    const getY = (val) => topMargin + h - (val / maxVal) * h;
    const getX = (i) => leftMargin + i * step;

    const drawLine = (key, color) => {
        ctx.beginPath();
        ctx.strokeStyle = color;
        ctx.lineWidth = 2;
        displayData.forEach((d, i) => {
            const val = d[key] || 0;
            const x = getX(i);
            const y = getY(val);
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();
    };

    // Draw Lines based on mode
    if (state.sidebarTab === 'watchlist' && !state.currentUser) {
        // Watchlist Aggregate Mode
        drawLine('count', '#94a3b8'); // Gray for total
        drawLine('forecast_hit_count', '#22c55e'); // Green for forecast hits
    } else if (state.sidebarTab === 'watchlist' && state.currentUser) {
        // Per-User Mode
        drawLine('count', '#94a3b8');
        drawLine('forecast_max', '#ef4444'); // Red for forecast score

        // Highlight high forecast days
        displayData.forEach((d, i) => {
            if (d.forecast_max > 0.6) {
                const x = getX(i);
                const y = topMargin; // Top of chart
                ctx.beginPath();
                ctx.fillStyle = '#ef4444';
                ctx.arc(x, y, 3, 0, 2 * Math.PI);
                ctx.fill();
            }
        });

    } else {
        // Global Alerts Mode
        drawLine('anomaly_count', '#f59e0b');
        drawLine('ml_count', '#a855f7');
        drawLine('rule_count', '#3b82f6');
    }

    // X-Axis Labels
    ctx.textAlign = 'center';
    ctx.fillStyle = '#94a3b8';
    displayData.forEach((d, i) => {
        ctx.fillText(formatDate(d.day).slice(0, 5), getX(i), canvas.height - 5);
    });
}

function toggleCaseNotes() {
    if (!state.currentUserRiskMeta) return;
    state.showCaseNotes = !state.showCaseNotes;
    renderUserHeader();
}

function openCaseNotesForUser(userKey, event) {
    if (event) event.stopPropagation();

    if (state.currentUser !== userKey) {
        selectUser(userKey);
        // loadUserAlerts(userKey) is already called in selectUser and will
        // set currentUserRiskMeta, then call renderUserHeader().
    }

    state.showCaseNotes = true;
    renderUserHeader();
}

function renderHeroOrCaseNotes() {
    const hero = document.getElementById('heroPanel');
    if (!hero) return;

    // Hero Mode: Show highest risk users
    // Sort users by severity (Critical > High > Medium > Low) then priority score
    const severityOrder = { 'critical': 3, 'high': 2, 'medium': 1, 'low': 0 };

    const sortedUsers = [...state.users]
        .filter(u => u.is_active !== false) // Optional: hide inactive?
        .sort((a, b) => {
            const sevA = severityOrder[a.severity_bucket] || 0;
            const sevB = severityOrder[b.severity_bucket] || 0;
            if (sevB !== sevA) return sevB - sevA;
            return (b.priority_score || 0) - (a.priority_score || 0);
        })
        .slice(0, 10); // Top 10

    hero.innerHTML = `<div class="hero-list"></div>`;
    const list = hero.querySelector('.hero-list');

    sortedUsers.forEach(u => {
        const div = document.createElement('div');
        div.className = `hero-user-row ${state.currentUser === u.user_key ? 'active' : ''}`;
        div.onclick = () => selectUser(u.user_key, div);

        const sevClass = `hero-severity-${u.severity_bucket || 'low'}`;

        let escalatedBadgeHtml = '';
        if (u.escalated_alerts > 0) {
            escalatedBadgeHtml = `<span class="escalated-badge" title="Escalated Alerts">${u.escalated_alerts}</span>`;
        }

        div.innerHTML = `
            <div class="hero-user-info">
                <button class="hero-casenotes-icon"
                        title="View case notes"
                        onclick="openCaseNotesForUser('${u.user_key}', event)">?</button>
                <div>
                    <div class="hero-user-name">${u.name}</div>
                    <div class="hero-user-key">${u.user_key}</div>
                </div>
            </div>
            <div class="hero-stats">
                ${escalatedBadgeHtml}
                <span class="hero-severity-badge ${sevClass}">${u.severity_bucket || 'low'}</span>
            </div>
        `;
        list.appendChild(div);
    });
}

// Modal
// Helper: Format Rule Hits for Display
function formatRuleHits(hits) {
    if (!hits || hits.length === 0) return 'None';
    if (hits.includes('s1_chain_post_departure')) {
        return 'Data theft pattern after termination.';
    }
    if (hits.includes('s1_chain')) {
        return 'Full data theft pattern while employed.';
    }
    if (hits.includes('s1_near_miss')) {
        return 'Possible preparation for data theft (high-risk activity).';
    }
    // Fallback for any other future rule IDs
    return 'Rule-based suspicious activity detected.';
}

// // Modal
// function openModal(alert) {
//     // --- Rule detector display logic ---
//     const hasRuleHits = alert.rule_hits && alert.rule_hits.length > 0;
//     const hasRuleScore = (alert.rules_score || 0) > 0;

//     // Default: nothing rule-y going on
//     let ruleBadgeLabel = 'Did Not Fire';
//     let ruleSummaryText = 'No rule-based pattern triggered this alert.';

//     if (hasRuleHits) {
//         // Actual S1 logic fired (chain / near-miss / post-departure)
//         ruleBadgeLabel = 'Fired';
//         ruleSummaryText = 'Rule-based Scenario-1 pattern matched in this alert.';
//     } else if (hasRuleScore) {
//         // Rules contributed to the ensemble for this user/day, but this record
//         // wasn’t the explicit Scenario-1 rule alert
//         ruleBadgeLabel = 'Active in window';
//         ruleSummaryText = 'Rule detector was active for this user/day, but this alert is driven primarily by other detectors.';
//     }
//     const modal = document.getElementById('alertModal');

//     const roleStr = formatRole(alert.user_role);
//     const headerText = roleStr ? `${alert.user_name} · ${roleStr} ` : alert.user_name;
//     document.getElementById('modalUser').textContent = headerText;
//     document.getElementById('modalDay').textContent = formatDate(alert.day);

//     // Post-termination notice
//     const modalMetaDiv = document.querySelector('.modal-meta');
//     if (modalMetaDiv) {
//         // Remove any existing termination notice
//         const existingNotice = modalMetaDiv.querySelector('.post-termination-notice');
//         if (existingNotice) {
//             existingNotice.remove();
//         }

//         // Add notice if this is a post-termination alert
//         if (alert.after_termination === true) {
//             const termDay = state.currentUserRiskMeta?.termination_day;
//             if (termDay) {
//                 const dateStr = formatShortDateUS(termDay);
//                 const notice = document.createElement('div');
//                 notice.className = 'post-termination-notice';
//                 notice.style.cssText = `
//         font - size: 0.85rem;
//         color: #94a3b8;
//         margin - top: 0.5rem;
//         font - style: italic;
//         `;
//                 notice.textContent = `This activity occurred after the user's employment ended (${dateStr}).`;
//                 modalMetaDiv.appendChild(notice);
//             }
//         }
//     }

//     // Scenario Text
//     let scenarioText = "Potential Future Data Theft";
//     if (alert.escalated) {
//         scenarioText = "Data Theft";
//     }
//     document.getElementById('modalScenario').textContent = scenarioText;

//     document.getElementById('modalScore').textContent = alert.ensemble_score.toFixed(2);

//     const statusEl = document.getElementById('modalStatus');
//     if (alert.escalated) {
//         statusEl.textContent = 'ESCALATED';
//         statusEl.className = 'status-badge status-escalated';
//     } else {
//         statusEl.textContent = 'NON-ESCALATED';
//         statusEl.className = 'status-badge status-normal';
//     }

//     const expl = alert.ensemble_explanation;
//     const comps = expl.components;

//     // --- Alert Description ---
//     const explContent = document.getElementById('explanationContent');

//     // 1. Rule Human Summary (Concise)
//     let descHtml = '';
//     if (alert.rule_human_summary) {
//         // Use the summary directly, but maybe truncated if it's too long?
//         // The prompt says "At most 1–2 sentences. Derived directly from alert.rule_human_summary."
//         // The current summaries are already reasonably short sentences.
//         descHtml += `<p><strong>Rule Summary:</strong> ${alert.rule_human_summary}</p>`;
//     }

//     // 2. Detector Findings
//     descHtml += `<p><strong>Detector Findings:</strong></p><ul>`;

//     // Rule
//     if (comps.rule.fired) {
//         const ruleDesc = formatRuleHits(comps.rule.hits);
//         descHtml += `<li><strong>Rule:</strong> ${ruleDesc}</li>`;
//     } else {
//         descHtml += `<li><strong>Rule:</strong> No rule-based evidence of data theft in this window.</li>`;
//     }

//     // ML
//     descHtml += `<li><strong>ML:</strong> ML model estimates ~${Math.round(comps.ml.score * 100)}% likelihood of data theft in this window.</li>`;

//     // Anomaly
//     if (comps.anomaly.score !== null) {
//         descHtml += `<li><strong>Anomaly:</strong> Unusual activity compared to this user's normal behavior (score ${comps.anomaly.score.toFixed(2)}).</li>`;
//     } else {
//         // Detector ran but did not flag anything meaningful in this window
//         descHtml += `<li><strong>Anomaly:</strong> No anomalous activity detected in this window.</li>`;
//     }

//     // Forecast
//     if (comps.forecast.score !== null) {
//         descHtml += `<li><strong>Forecast:</strong> Forecast model predicts this user is likely to exfiltrate within the next few days (${Math.round(comps.forecast.score * 100)}% risk).</li>`;
//     }

//     descHtml += `</ul>`;
//     explContent.innerHTML = descHtml;

//     // --- Detector Cards ---

//     // Rule Card
//     const ruleBadge = document.getElementById('badge-rule');
//     const ruleBody = document.getElementById('body-rule');
//     ruleBadge.textContent = comps.rule.fired ? 'FIRED' : 'Did Not Fire';
//     ruleBadge.className = comps.rule.fired ? 'fired-badge fired-true' : 'fired-badge fired-false';

//     const ruleCardText = formatRuleHits(comps.rule.hits);

//     ruleBody.innerHTML = `
//         <div>${ruleCardText}</div>
//     `;

//     // ML Card
//     const mlBadge = document.getElementById('badge-ml');
//     const mlBody = document.getElementById('body-ml');
//     mlBadge.textContent = `${Math.round(comps.ml.score * 100)}%`;
//     mlBadge.className = 'fired-badge fired-false';
//     mlBody.innerHTML = `
//         <div>${Math.round(comps.ml.score * 100)}% likelihood of data theft</div>
//     `;

//     // Anomaly Card
//     const anomBadge = document.getElementById('badge-anomaly');
//     const anomBody = document.getElementById('body-anomaly');

//     if (comps.anomaly.score !== null) {
//         anomBadge.textContent = comps.anomaly.score.toFixed(2);
//         anomBody.innerHTML = `<div>Unusual compared to past activity</div>`;
//     } else {
//         anomBadge.textContent = 'N/A';
//         anomBody.innerHTML = `<div>No anomalous activity detected.</div>`;
//     }
//     anomBadge.className = 'fired-badge fired-false';

//     // Forecast Card
//     const foreBadge = document.getElementById('badge-forecast');
//     const foreBody = document.getElementById('body-forecast');

//     if (comps.forecast.score !== null) {
//         foreBadge.textContent = `${Math.round(comps.forecast.score * 100)}%`;
//         foreBody.innerHTML = `<div>Likelihood of upcoming potential exfiltration</div>`;
//     } else {
//         foreBadge.textContent = 'N/A';
//         foreBody.innerHTML = `<div>No score</div>`;
//     }
//     foreBadge.className = 'fired-badge fired-false';

//     modal.style.display = 'block';
// }

function openModal(alert) {
    // --- Rule detector display logic ---
    const hasRuleHits = alert.rule_hits && alert.rule_hits.length > 0;
    const hasRuleScore = (alert.rules_score || 0) > 0;

    // Default: nothing rule-y going on
    let ruleBadgeLabel = 'Did Not Fire';
    let ruleSummaryText = 'No rule-based pattern triggered this alert.';

    if (hasRuleHits) {
        // Actual S1 logic fired (chain / near-miss / post-departure)
        ruleBadgeLabel = 'Fired';
        ruleSummaryText = 'Rule-based Scenario-1 pattern matched in this alert.';
    } else if (hasRuleScore) {
        // Rules contributed to the ensemble for this user/day, but this record
        // wasn’t the explicit Scenario-1 rule alert
        ruleBadgeLabel = 'Active in window';
        ruleSummaryText = 'Rule detector was active for this user/day, but this alert is driven primarily by other detectors.';
    }

    const modal = document.getElementById('alertModal');

    const roleStr = formatRole(alert.user_role);
    const headerText = roleStr ? `${alert.user_name} · ${roleStr} ` : alert.user_name;
    document.getElementById('modalUser').textContent = headerText;
    document.getElementById('modalDay').textContent = formatDate(alert.day);

    // Post-termination notice
    const modalMetaDiv = document.querySelector('.modal-meta');
    if (modalMetaDiv) {
        // Remove any existing termination notice
        const existingNotice = modalMetaDiv.querySelector('.post-termination-notice');
        if (existingNotice) {
            existingNotice.remove();
        }

        // Add notice if this is a post-termination alert
        if (alert.after_termination === true) {
            const termDay = state.currentUserRiskMeta?.termination_day;
            if (termDay) {
                const dateStr = formatShortDateUS(termDay);
                const notice = document.createElement('div');
                notice.className = 'post-termination-notice';
                notice.style.cssText = `
                    font-size: 0.85rem;
                    color: #94a3b8;
                    margin-top: 0.5rem;
                    font-style: italic;
                `;
                notice.textContent = `This activity occurred after the user's employment ended (${dateStr}).`;
                modalMetaDiv.appendChild(notice);
            }
        }
    }

    // Scenario Text
    let scenarioText = "Potential Future Data Theft";
    if (alert.escalated) {
        scenarioText = "Data Theft";
    }
    document.getElementById('modalScenario').textContent = scenarioText;

    document.getElementById('modalScore').textContent = alert.ensemble_score.toFixed(2);

    const statusEl = document.getElementById('modalStatus');
    if (alert.escalated) {
        statusEl.textContent = 'ESCALATED';
        statusEl.className = 'status-badge status-escalated';
    } else {
        statusEl.textContent = 'NON-ESCALATED';
        statusEl.className = 'status-badge status-normal';
    }

    // --- Explanation + scores ---
    const expl = alert.ensemble_explanation || {};
    const comps = expl.components || {};
    const ruleComp = comps.rule || {};
    const mlComp = comps.ml || {};
    const anomalyComp = comps.anomaly || {};
    const forecastComp = comps.forecast || {};

    // Prefer alert-level numeric scores; fall back to components
    const mlScore = (typeof alert.ml_score === 'number')
        ? alert.ml_score
        : (typeof mlComp.score === 'number' ? mlComp.score : 0);

    const anomalyScore = (typeof alert.anomaly_score === 'number')
        ? alert.anomaly_score
        : (typeof anomalyComp.score === 'number' ? anomalyComp.score : null);

    const forecastScore = (typeof alert.forecast_score === 'number')
        ? alert.forecast_score
        : (typeof forecastComp.score === 'number' ? forecastComp.score : null);

    // --- Alert Description ---
    const explContent = document.getElementById('explanationContent');

    // 1. Rule Human Summary (Concise)
    let descHtml = '';
    if (alert.rule_human_summary) {
        descHtml += `
            <div class="rule-summary-block">
                <div class="rule-summary-label"><strong>Rule Summary:</strong></div>
                <div class="rule-summary-text">
                    ${alert.rule_human_summary}
                </div>
            </div>
        `;
    } else if (hasRuleHits || hasRuleScore) {
        descHtml += `<p><strong>Rule Summary:</strong> ${ruleSummaryText}</p>`;
    }

    // 2. Detector Findings (dynamic per-detector)
    const findings = [];

    // Rule finding
    if (hasRuleHits) {
        const ruleDesc = formatRuleHits(ruleComp.hits || alert.rule_hits || []);
        findings.push(
            `<li><strong>Rule:</strong> ${ruleDesc}</li>`
        );
    } else if (hasRuleScore) {
        findings.push(
            `<li><strong>Rule:</strong> Rule detector was active in this window but did not complete the Scenario-1 pattern.</li>`
        );
    } else {
        findings.push(
            `<li><strong>Rule:</strong> No rule-based signal in this window.</li>`
        );
    }

    // ML finding
    if (mlScore > 0) {
        const mlPct = Math.round(mlScore * 100);
        findings.push(
            `<li><strong>ML:</strong> ML model estimates ~${mlPct}% likelihood of data theft in this window.</li>`
        );
    } else {
        findings.push(
            `<li><strong>ML:</strong> ML model did not detect elevated risk in this window (score ~0).</li>`
        );
    }

    // Anomaly finding
    if (anomalyScore != null && anomalyScore > 0) {
        const sevText = anomalyComp.severity
            ? `${anomalyComp.severity.toLowerCase()} anomaly`
            : 'Unusual activity';
        findings.push(
            `<li><strong>Anomaly:</strong> ${sevText} compared to this user's normal behavior (score ${anomalyScore.toFixed(2)}).</li>`
        );
    } else {
        findings.push(
            `<li><strong>Anomaly:</strong> No anomalous activity detected in this window.</li>`
        );
    }

    // Forecast finding
    if (forecastScore != null && forecastScore > 0) {
        const forePct = Math.round(forecastScore * 100);
        findings.push(
            `<li><strong>Forecast:</strong> Forecast model predicts this user is likely to exfiltrate within the next few days (${forePct}% risk).</li>`
        );
    } else {
        findings.push(
            `<li><strong>Forecast:</strong> Forecast model does not show elevated near-term risk.</li>`
        );
    }

    descHtml += `
        <p><strong>Detector Findings:</strong></p>
        <ul class="detector-findings-list">
            ${findings.join('')}
        </ul>
    `;
    explContent.innerHTML = descHtml;

    // --- Detector Cards ---

    // Rule Card
    const ruleBadge = document.getElementById('badge-rule');
    const ruleBody = document.getElementById('body-rule');
    ruleBadge.textContent = ruleBadgeLabel;
    ruleBadge.className = hasRuleHits
        ? 'fired-badge fired-true'
        : 'fired-badge fired-false';
    ruleBody.innerHTML = `<div>${ruleSummaryText}</div>`;

    // ML Card
    const mlBadge = document.getElementById('badge-ml');
    const mlBody = document.getElementById('body-ml');
    const mlPct = Math.round(mlScore * 100);
    mlBadge.textContent = `${mlPct}%`;
    mlBadge.className = 'fired-badge fired-false';
    mlBody.innerHTML = `<div>${mlPct}% likelihood of data theft</div>`;

    // Anomaly Card
    const anomBadge = document.getElementById('badge-anomaly');
    const anomBody = document.getElementById('body-anomaly');

    if (anomalyScore != null && anomalyScore > 0) {
        anomBadge.textContent = anomalyScore.toFixed(2);
        anomBody.innerHTML = `<div>Unusual compared to past activity</div>`;
    } else {
        anomBadge.textContent = 'N/A';
        anomBody.innerHTML = `<div>No anomalous activity detected.</div>`;
    }
    anomBadge.className = 'fired-badge fired-false';

    // Forecast Card
    const foreBadge = document.getElementById('badge-forecast');
    const foreBody = document.getElementById('body-forecast');

    if (forecastScore != null && forecastScore > 0) {
        const forePctCard = Math.round(forecastScore * 100);
        foreBadge.textContent = `${forePctCard}%`;
        foreBody.innerHTML = `<div>Likelihood of upcoming potential exfiltration</div>`;
    } else {
        foreBadge.textContent = 'N/A';
        foreBody.innerHTML = `<div>No score</div>`;
    }
    foreBadge.className = 'fired-badge fired-false';

    modal.style.display = 'block';
}

function closeModal() {
    document.getElementById('alertModal').style.display = 'none';
}

window.onclick = function (event) {
    const modal = document.getElementById('alertModal');
    if (event.target == modal) {
        closeModal();
    }
}

async function loadForecastSummary() {
    const res = await fetch('/api/forecast/summary');
    const data = await res.json();
    state.forecastSummary = data;

    const card = document.getElementById('forecastSummaryCard');
    if (state.forecastSummary) {
        card.style.display = 'block';
        document.getElementById('fsTotal').textContent = data.total_forecasted_users;
        document.getElementById('fsCorrect').textContent = data.correct_forecasts;
        document.getElementById('fsBestLead').textContent = data.best_lead_time_days !== null ? `${data.best_lead_time_days} days` : '-';
        document.getElementById('fsMedianLead').textContent = data.median_lead_time_days !== null ? data.median_lead_time_days.toFixed(1) : '-';
    }
}
// Kick off the dashboard once the DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    if (typeof init === 'function') {
        init();
    } else {
        console.error('init() is not defined; dashboard cannot start.');
    }
});

// Start
init();
