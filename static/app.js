// ── State ──────────────────────────────────────────────────────────────────────
let lastSuccessfulFetch = null;
let leaveNowActive = false;
let walkMinutes = parseInt(localStorage.getItem('walkMinutes') || '3', 10);
let leafletMap = null;
let countdownTimerId = null;
let staleCheckTimerId = null;
let vehiclePollTimerId = null;
let vehicleMarkers = [];

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
                devHeader.textContent = '⚠ Service Alerts';
                devContainer.appendChild(devHeader);
                deviations.forEach(dev => {
                    const devItem = document.createElement('div');
                    devItem.className = `deviation-item ${dev.consequence}`;
                    devItem.textContent = dev.message;
                    devContainer.appendChild(devItem);
                });
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
                    ? `data-expected-iso="${firstDep.expected_iso}" data-status-text="${firstDep.status_text}" data-site-id="${siteId}" data-station-name="${escapeAttr(station.station)}" data-line-num="${firstDep.line_num}" data-destination="${escapeAttr(firstDep.destination)}"`
                    : '';

                let rows = '';
                deps.forEach(dep => {
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
                    rows += `<tr class="dep-row"
                        data-site-id="${siteId}"
                        data-station-name="${escapeAttr(station.station)}"
                        data-line-num="${dep.line_num}"
                        data-destination="${escapeAttr(dep.destination)}"
                        data-expected-iso="${dep.expected_iso}">
                        <td class="line">${dep.line_num}</td>
                        <td class="dest">${dep.destination}</td>
                        <td class="dep-time time${imminent}" data-expected-iso="${dep.expected_iso}" data-clock-time="${dep.display_time}">${timeDisplay}</td>
                        <td class="status ${sc}">${dep.status_text}${gtfsFlag}</td>
                        <td class="map-tap">&#x1F4CD;</td>
                    </tr>`;
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
                        ${firstDep ? '<span class="map-hint"> — tap row for map</span>' : ''}
                    </div>
                    <div class="departures-wrapper"><table>${rows}</table></div>
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

// ── Map ────────────────────────────────────────────────────────────────────────
function openMap(siteId, stationName, lineNum, destination, expectedIso) {
    const modal = document.getElementById('map-modal');
    modal.style.display = 'flex';
    document.body.style.overflow = 'hidden';

    const mins = minutesUntil(expectedIso);
    const timeStr = mins === null ? '' : mins < 0 ? 'now' : `in ${mins} min`;
    document.getElementById('map-dep-info').innerHTML =
        `<span class="map-line-badge">${lineNum}</span>` +
        `<span class="map-dest-text"> → ${destination}</span>` +
        `<span class="map-time-text">${timeStr}</span>` +
        `<div class="map-station-text">${stationName}</div>`;

    // Remove previous map instance
    if (leafletMap) {
        leafletMap.remove();
        leafletMap = null;
    }

    const mapEl = document.getElementById('leaflet-map');
    // Default: Stockholm city centre
    leafletMap = L.map(mapEl, { zoomControl: true }).setView([59.3293, 18.0686], 14);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors © <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 19
    }).addTo(leafletMap);

    // Fetch station coordinates
    fetch(`/api/sites/${siteId}/location?name=${encodeURIComponent(stationName)}`)
        .then(r => r.ok ? r.json() : Promise.reject('not found'))
        .then(loc => {
            if (loc.error) throw new Error(loc.error);
            leafletMap.setView([loc.lat, loc.lon], 16);
            const icon = L.divIcon({
                className: 'station-marker',
                html: `<div class="marker-pin"><span>${lineNum}</span></div>`,
                iconSize: [44, 44],
                iconAnchor: [22, 44]
            });
            L.marker([loc.lat, loc.lon], { icon })
                .addTo(leafletMap)
                .bindPopup(`<b>${stationName}</b><br>Line ${lineNum} → ${destination}<br>${timeStr}`)
                .openPopup();
        })
        .catch(() => {
            const info = document.getElementById('map-dep-info');
            info.innerHTML += '<div class="map-no-loc">⚠ Station coordinates not yet available — check back after first data refresh</div>';
        });

    // Live vehicle positions for this line/direction
    pollVehicles(lineNum, destination);
    vehiclePollTimerId = setInterval(() => pollVehicles(lineNum, destination), 10000);
}

function vehicleIcon() {
    return L.divIcon({
        className: 'vehicle-marker',
        html: '<div class="vehicle-dot"></div>',
        iconSize: [18, 18],
        iconAnchor: [9, 9]
    });
}

function pollVehicles(lineNum, destination) {
    if (!leafletMap) return;
    fetch(`/api/lines/${encodeURIComponent(lineNum)}/vehicles?direction=${encodeURIComponent(destination)}`)
        .then(r => r.json())
        .then(data => {
            vehicleMarkers.forEach(m => m.remove());
            vehicleMarkers = [];

            const info = document.getElementById('map-dep-info');
            const existingNote = info.querySelector('.map-no-vehicles');
            if (existingNote) existingNote.remove();

            const vehicles = data.vehicles || [];
            if (data.available && vehicles.length === 0) {
                info.insertAdjacentHTML('beforeend',
                    '<div class="map-no-vehicles">No live vehicles currently reported for this line</div>');
            }

            vehicles.forEach(v => {
                if (v.lat == null || v.lon == null || !leafletMap) return;
                vehicleMarkers.push(
                    L.marker([v.lat, v.lon], { icon: vehicleIcon() }).addTo(leafletMap)
                );
            });
        })
        .catch(() => { /* best-effort — leave existing markers/state untouched on transient failure */ });
}

function closeMap() {
    document.getElementById('map-modal').style.display = 'none';
    document.body.style.overflow = '';
    if (vehiclePollTimerId) {
        clearInterval(vehiclePollTimerId);
        vehiclePollTimerId = null;
    }
    vehicleMarkers.forEach(m => m.remove());
    vehicleMarkers = [];
    if (leafletMap) {
        leafletMap.remove();
        leafletMap = null;
    }
}

// ── Event delegation ───────────────────────────────────────────────────────────
document.getElementById('board').addEventListener('click', e => {
    // Departure row tap → open map
    const row = e.target.closest('tr.dep-row');
    if (row) {
        openMap(
            parseInt(row.dataset.siteId),
            row.dataset.stationName,
            row.dataset.lineNum,
            row.dataset.destination,
            row.dataset.expectedIso
        );
        return;
    }

    // Card-next tap → open map for first departure
    const next = e.target.closest('.card-next[data-site-id]');
    if (next) {
        openMap(
            parseInt(next.dataset.siteId),
            next.dataset.stationName,
            next.dataset.lineNum,
            next.dataset.destination,
            next.dataset.expectedIso
        );
        e.stopPropagation();
        return;
    }

    // Card header tap → expand / collapse
    const card = e.target.closest('.station-card');
    if (card) {
        card.classList.toggle('expanded');
    }
});

// Close map when tapping outside the modal content
document.getElementById('map-modal').addEventListener('click', e => {
    if (e.target === document.getElementById('map-modal')) closeMap();
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
