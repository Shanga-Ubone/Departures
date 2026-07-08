// ── State ──────────────────────────────────────────────────────────────────────
let lastSuccessfulFetch = null;
let leaveNowActive = false;
let walkMinutes = parseInt(localStorage.getItem('walkMinutes') || '3', 10);
let countdownTimerId = null;
let staleCheckTimerId = null;
let activeLineVizTimers = []; // interval ids for expanded logical-line trackers, cleared on every board redraw

// ── Weather ────────────────────────────────────────────────────────────────────
function debounce(func, delay) {
    let timeoutId;
    return function(...args) {
        clearTimeout(timeoutId);
        timeoutId = setTimeout(() => func.apply(this, args), delay);
    };
}

async function updateWeather() {
    try {
        const response = await fetch('https://api.open-meteo.com/v1/forecast?latitude=59.3293&longitude=18.0686&current=temperature_2m,weather_code&hourly=temperature_2m,weather_code&temperature_unit=celsius&timezone=auto');
        const data = await response.json();
        const current = data.current;
        const hourly = data.hourly;

        const weatherCodes = {
            0: 'Clear', 1: 'Mostly Clear', 2: 'Partly Cloudy', 3: 'Overcast',
            45: 'Foggy', 48: 'Foggy', 51: 'Drizzle', 53: 'Drizzle', 55: 'Drizzle',
            61: 'Rain', 63: 'Rain', 65: 'Rain', 71: 'Snow', 73: 'Snow', 75: 'Snow',
            80: 'Showers', 81: 'Showers', 82: 'Showers',
            85: 'Snow Showers', 86: 'Snow Showers', 95: 'Thunderstorm'
        };

        document.getElementById('temp').textContent = `${current.temperature_2m}°C`;
        document.getElementById('condition').textContent = weatherCodes[current.weather_code] || 'Unknown';

        const hourlyEl = document.getElementById('hourly-forecast');
        hourlyEl.innerHTML = '';
        const now = new Date();
        const times = hourly.time;
        const temps = hourly.temperature_2m;
        const codes = hourly.weather_code;
        const startIndex = times.findIndex(t => new Date(t).getHours() === now.getHours());
        const endIndex = Math.min(startIndex + 8, times.length);
        for (let i = Math.max(0, startIndex); i < endIndex; i++) {
            const hour = new Date(times[i]).getHours().toString().padStart(2, '0');
            const item = document.createElement('div');
            item.className = 'hour-item';
            item.innerHTML = `<span class="hour-time">${hour}:00</span><span class="hour-temp">${Math.round(temps[i])}°</span><span class="hour-condition">${weatherCodes[codes[i]] || ''}</span>`;
            hourlyEl.appendChild(item);
        }
    } catch (e) {
        document.getElementById('condition').textContent = 'Weather unavailable';
    }
}

// ── Status helpers ─────────────────────────────────────────────────────────────
const STATUS_RANK = { ok: 0, warn: 1, crit: 2 };

function getDelayMinutes(statusText) {
    if (statusText === 'On Time') return 0;
    const m = statusText.match(/([+-]?\d+)\s*min/);
    return m ? parseInt(m[1], 10) : 0;
}

function cardStatusFromDepartures(departures) {
    let max = 0;
    for (const dep of departures) max = Math.max(max, getDelayMinutes(dep.status_text));
    if (max > 7) return 'crit';
    if (max >= 3) return 'warn';
    return 'ok';
}

function deviationStatus(deviations) {
    const relevant = (deviations || []).filter(d => !d.station_wide);
    if (!relevant.length) return 'ok';
    return relevant.some(d => d.consequence === 'CANCELLED' || d.consequence === 'DIVERSION') ? 'crit' : 'warn';
}

function worstOf(a, b) {
    return STATUS_RANK[a] >= STATUS_RANK[b] ? a : b;
}

// ── Countdown ──────────────────────────────────────────────────────────────────
function minutesUntil(isoStr) {
    if (!isoStr) return null;
    return Math.round((new Date(isoStr) - Date.now()) / 60000);
}

function updateCountdowns() {
    document.querySelectorAll('.dep-time[data-expected-iso]').forEach(el => {
        const mins = minutesUntil(el.dataset.expectedIso);
        if (mins === null) return;
        if (mins < 0) {
            el.textContent = '—';
        } else if (mins < 60) {
            el.textContent = `${mins} min`;
            el.classList.toggle('imminent', mins <= 2);
        } else {
            el.textContent = el.dataset.clockTime;
        }
    });
    // Keep card-next summaries current
    document.querySelectorAll('.card-next[data-expected-iso]').forEach(el => {
        const mins = minutesUntil(el.dataset.expectedIso);
        const timeStr = mins === null ? '' : mins < 0 ? 'departed' : `${mins} min`;
        const statusSpan = el.querySelector('.next-status');
        if (statusSpan && timeStr) {
            const orig = el.dataset.statusText;
            statusSpan.textContent = `Next: ${timeStr}  ${orig}`;
        }
    });
}

// ── Stale check ────────────────────────────────────────────────────────────────
function checkStale() {
    if (!lastSuccessfulFetch) return;
    const age = (Date.now() - lastSuccessfulFetch) / 1000;
    const banner = document.getElementById('stale-banner');
    if (age > 45) {
        banner.textContent = `⚠ Data may be outdated — last updated ${Math.floor(age)}s ago`;
        banner.style.display = 'block';
    } else {
        banner.style.display = 'none';
    }
    updateConnectionStatus();
}

// ── Connection status ──────────────────────────────────────────────────────────
function updateConnectionStatus() {
    const dot = document.getElementById('conn-dot');
    if (!navigator.onLine) {
        dot.className = 'conn-dot offline';
        dot.title = 'Offline';
    } else if (lastSuccessfulFetch && (Date.now() - lastSuccessfulFetch) > 45000) {
        dot.className = 'conn-dot stale';
        dot.title = 'Data stale';
    } else {
        dot.className = 'conn-dot online';
        dot.title = 'Connected';
    }
}

// ── Leave Now filter ───────────────────────────────────────────────────────────
function toggleLeaveNow() {
    leaveNowActive = !leaveNowActive;
    const btn = document.getElementById('leave-now-btn');
    btn.classList.toggle('active', leaveNowActive);
    btn.textContent = leaveNowActive
        ? `✕ LEAVE NOW ON (${walkMinutes}m)`
        : `🚶 LEAVE NOW (${walkMinutes}m)`;
    applyLeaveNowFilter();
}

function applyLeaveNowFilter() {
    document.querySelectorAll('tr.dep-row').forEach(row => {
        if (!leaveNowActive) {
            row.style.display = '';
            return;
        }
        const mins = minutesUntil(row.dataset.expectedIso);
        row.style.display = (mins !== null && mins < walkMinutes) ? 'none' : '';
    });
}

// ── Board rendering ────────────────────────────────────────────────────────────
async function updateBoard() {
    try {
        const response = await fetch('/api/data');
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();

        lastSuccessfulFetch = Date.now();
        updateConnectionStatus();
        document.getElementById('stale-banner').style.display = 'none';

        activeLineVizTimers.forEach(id => clearInterval(id));
        activeLineVizTimers = [];

        const board = document.getElementById('board');
        board.innerHTML = '';
        document.getElementById('loading').style.display = 'none';

        let overallStatus = 'ok';

        for (const item of data) {
            const deviations = item.deviations || [];

            const groupHeader = document.createElement('h2');
            groupHeader.className = 'group-header';
            groupHeader.textContent = item.group;
            board.appendChild(groupHeader);

            if (deviations.length > 0) {
                overallStatus = worstOf(overallStatus, deviationStatus(deviations));
                const devContainer = document.createElement('div');
                devContainer.className = 'deviations-container';
                const devHeader = document.createElement('div');
                devHeader.className = 'deviation-header';
                devHeader.innerHTML = `<span>⚠ Service Alerts</span><span class="chevron">▼</span>`;
                devContainer.appendChild(devHeader);
                const devList = document.createElement('div');
                devList.className = 'deviations-list';
                deviations.forEach(dev => {
                    const devItem = document.createElement('div');
                    devItem.className = `deviation-item ${dev.consequence}`;
                    devItem.textContent = dev.message;
                    devList.appendChild(devItem);
                });
                devContainer.appendChild(devList);
                board.appendChild(devContainer);
            }

            item.stations.forEach(station => {
                const deps = station.departures;
                const siteId = station.site_id;
                const cardStatus = cardStatusFromDepartures(deps);
                overallStatus = worstOf(overallStatus, cardStatus);

                const firstDep = deps[0];
                const nextClass = firstDep
                    ? (firstDep.status_text === 'On Time' ? 'ontime' : 'late')
                    : '';

                const nextAttrs = firstDep
                    ? `data-expected-iso="${firstDep.expected_iso}" data-status-text="${firstDep.status_text}"`
                    : '';

                // Group departures by (line, destination) — each group gets its own
                // collapsible logical-line tracker shared by all its upcoming trips.
                const groups = new Map();
                deps.forEach(dep => {
                    const key = `${dep.line_num}|${dep.destination}`;
                    if (!groups.has(key)) {
                        groups.set(key, { line_num: dep.line_num, destination: dep.destination, deps: [] });
                    }
                    groups.get(key).deps.push(dep);
                });

                let groupsHtml = '';
                groups.forEach(group => {
                    let depRows = '';
                    group.deps.forEach(dep => {
                        const sc = dep.status_text === 'On Time' ? 'ontime' : 'late';
                        const mins = minutesUntil(dep.expected_iso);
                        const timeDisplay = (mins !== null && mins >= 0 && mins < 60)
                            ? `${mins} min`
                            : dep.display_time;
                        const imminent = mins !== null && mins <= 2 && mins >= 0 ? ' imminent' : '';
                        const gtfsTitle = dep.gtfs_alert
                            ? dep.gtfs_alert.header
                            : (dep.gtfs_cross_check === 'delay_diff' ? 'Trafiklab real-time data disagrees with this estimate' : '');
                        const gtfsFlag = gtfsTitle ? `<span class="gtfs-flag" title="${escapeAttr(gtfsTitle)}">&#8224;</span>` : '';
                        depRows += `<tr class="dep-row" data-expected-iso="${dep.expected_iso}">
                            <td class="dep-time time${imminent}" data-expected-iso="${dep.expected_iso}" data-clock-time="${dep.display_time}">${timeDisplay}</td>
                            <td class="status ${sc}">${dep.status_text}${gtfsFlag}</td>
                        </tr>`;
                    });

                    const depsJson = escapeAttr(JSON.stringify(
                        group.deps.map(d => ({ trip_id: d.trip_id || null, display_time: d.display_time, status_text: d.status_text }))
                    ));

                    groupsHtml += `<div class="line-group"
                        data-site-id="${siteId}"
                        data-station-name="${escapeAttr(station.station)}"
                        data-line-num="${group.line_num}"
                        data-destination="${escapeAttr(group.destination)}"
                        data-deps="${depsJson}">
                        <div class="line-group-header">
                            <span class="line-num">${group.line_num}</span>
                            <span class="line-dest">→ ${group.destination}</span>
                            <span class="chevron">▼</span>
                        </div>
                        <table class="line-departures"><tbody>${depRows}</tbody></table>
                        <div class="line-viz"></div>
                    </div>`;
                });

                const card = document.createElement('div');
                card.className = `station-card ${cardStatus}`;
                card.innerHTML = `
                    <div class="card-header">
                        <div class="status-dot"></div>
                        <div class="station-name">${station.station}</div>
                        <div class="chevron">▼</div>
                    </div>
                    <div class="card-next ${nextClass}" ${nextAttrs}>
                        <span class="next-status">${firstDep ? `Next: ${firstDep.display_time}  ${firstDep.status_text}` : 'No departures found'}</span>
                    </div>
                    <div class="departures-wrapper">${groupsHtml}</div>
                `;
                board.appendChild(card);
            });
        }

        const bannerLabels = { ok: '● ALL CLEAR', warn: '● MINOR DISRUPTION', crit: '● SIGNIFICANT DELAYS' };
        const banner = document.getElementById('commute-status-banner');
        banner.textContent = bannerLabels[overallStatus];
        banner.className = `status-banner ${overallStatus}`;

        document.getElementById('last-updated').textContent = `Last Updated: ${new Date().toLocaleTimeString()}`;

        updateCountdowns();
        if (leaveNowActive) applyLeaveNowFilter();

    } catch (error) {
        console.error('Fetch error:', error);
        checkStale();
    }
}

function escapeAttr(str) {
    return String(str || '').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// ── Logical-line vehicle tracker ────────────────────────────────────────────────
// Your station is the LEFT anchor of the track; upstream stops extend to the
// right in travel order, so a vehicle's marker moves right→left as it approaches.

function toggleLineGroup(groupEl) {
    const expanding = !groupEl.classList.contains('expanded');
    groupEl.classList.toggle('expanded', expanding);
    if (expanding) {
        startLineViz(groupEl);
    } else {
        stopLineViz(groupEl);
    }
}

function startLineViz(groupEl) {
    const fetchAndRender = () => fetchLineProgress(groupEl);
    fetchAndRender();
    const timerId = setInterval(fetchAndRender, 10000);
    groupEl._vizTimerId = timerId;
    activeLineVizTimers.push(timerId);
}

function stopLineViz(groupEl) {
    if (groupEl._vizTimerId) {
        clearInterval(groupEl._vizTimerId);
        activeLineVizTimers = activeLineVizTimers.filter(id => id !== groupEl._vizTimerId);
        groupEl._vizTimerId = null;
    }
}

async function fetchLineProgress(groupEl) {
    const vizEl = groupEl.querySelector('.line-viz');
    const { lineNum, siteId, stationName, destination } = groupEl.dataset;
    const deps = JSON.parse(groupEl.dataset.deps || '[]');
    const tripIds = deps.map(d => d.trip_id).filter(Boolean);

    if (tripIds.length === 0) {
        vizEl.innerHTML = '<div class="line-viz-unavailable">Live tracking unavailable for this line</div>';
        return;
    }

    try {
        const url = `/api/lines/${encodeURIComponent(lineNum)}/progress` +
            `?destination=${encodeURIComponent(destination)}&site_id=${encodeURIComponent(siteId)}` +
            `&station_name=${encodeURIComponent(stationName)}&trip_ids=${encodeURIComponent(tripIds.join(','))}`;
        const resp = await fetch(url);
        const data = await resp.json();
        renderLineViz(vizEl, data, deps);
    } catch (e) {
        vizEl.innerHTML = '<div class="line-viz-unavailable">Live tracking unavailable</div>';
    }
}

function renderLineViz(vizEl, data, deps) {
    if (!data.available || !data.stops || data.stops.length < 2) {
        vizEl.innerHTML = '<div class="line-viz-unavailable">Live tracking unavailable for this line</div>';
        return;
    }

    const stops = data.stops; // furthest-upstream(0) -> your station(last), by dist_from_start
    const total = stops[stops.length - 1].dist_from_start || 1;
    const reversed = [...stops].slice().reverse(); // your station first, since it's the left anchor

    const ticksHtml = reversed.map((s, i) => {
        const leftPct = 100 - (s.dist_from_start / total * 100);
        const isTarget = i === 0;
        return `<div class="line-stop${isTarget ? ' target' : ''}" style="left:${leftPct}%">
            <div class="stop-dot"></div>
            <div class="stop-label">${isTarget ? 'YOUR STOP' : escapeAttr(s.name)}</div>
        </div>`;
    }).join('');

    const labelByTripId = new Map(deps.filter(d => d.trip_id).map(d => [d.trip_id, `${d.display_time} ${d.status_text}`]));
    const vehiclesHtml = (data.vehicles || []).map(v => {
        if (v.progress == null) return '';
        const leftPct = (1 - v.progress) * 100;
        const label = labelByTripId.get(v.trip_id) || '';
        return `<div class="line-vehicle" style="left:${leftPct}%" title="${escapeAttr(label)}">🚋</div>`;
    }).join('');

    vizEl.innerHTML = `<div class="line-track">
        <div class="line-track-bar"></div>
        ${ticksHtml}
        ${vehiclesHtml}
    </div>`;
}

// ── Event delegation ───────────────────────────────────────────────────────────
document.getElementById('board').addEventListener('click', e => {
    // Line group header tap → expand / collapse its logical-line tracker
    const lineHeader = e.target.closest('.line-group-header');
    if (lineHeader) {
        toggleLineGroup(lineHeader.closest('.line-group'));
        return;
    }

    // Card header tap → expand / collapse
    const card = e.target.closest('.station-card');
    if (card) {
        card.classList.toggle('expanded');
        return;
    }

    // Service alerts header tap → expand / collapse
    const devHeader = e.target.closest('.deviation-header');
    if (devHeader) {
        devHeader.closest('.deviations-container').classList.toggle('expanded');
    }
});

// ── Connection events ──────────────────────────────────────────────────────────
window.addEventListener('online', () => updateConnectionStatus());
window.addEventListener('offline', () => updateConnectionStatus());

// ── Init ───────────────────────────────────────────────────────────────────────
updateWeather();
updateBoard();

setInterval(updateWeather, 600000);         // weather every 10 min
setInterval(updateBoard, 20000);            // departures every 20 s
countdownTimerId = setInterval(updateCountdowns, 1000);  // countdown every 1 s
staleCheckTimerId = setInterval(checkStale, 5000);       // stale check every 5 s

if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js').catch(() => {});
}
