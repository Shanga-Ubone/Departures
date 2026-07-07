// ── State ──────────────────────────────────────────────────────────────────────
let lastSuccessfulFetch = null;
let leaveNowActive = false;
let walkMinutes = parseInt(localStorage.getItem('walkMinutes') || '3', 10);
let leafletMap = null;
let countdownTimerId = null;
let staleCheckTimerId = null;
let vehiclePollTimerId = null;
let vehicleMarkersById = new Map(); // key -> {marker, lat, lon, sameDirection}
let routePolyline = null;
let stationMarker = null;
let hasFitBounds = false;
let lastVehiclePollAt = null;
let mapLoadState = 'loading'; // 'loading' | 'live' | 'stalled' | 'unavailable'
let firstPollDone = false;

function resetMapState() {
    vehicleMarkersById.forEach(entry => entry.marker.remove());
    vehicleMarkersById = new Map();
    if (routePolyline) { routePolyline.remove(); routePolyline = null; }
    stationMarker = null;
    hasFitBounds = false;
    lastVehiclePollAt = null;
    mapLoadState = 'loading';
    firstPollDone = false;
}

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
    if (leafletMap) {
        updateMapLiveStatus();
        vehicleMarkersById.forEach(entry => {
            if (entry.destination) entry.marker.setTooltipContent(vehicleTooltipHtml(entry.destination, entry.etaIso));
        });
    }
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
const MAP_LIVE_GRACE_MS = 25000; // ~2.5 missed 10s polls before we call it "stalled"

function openMap(siteId, stationName, lineNum, destination, expectedIso) {
    resetMapState();

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

    document.getElementById('map-loading-spinner').style.display = 'flex';
    updateMapLiveStatus();

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
            const icon = L.divIcon({
                className: 'station-marker',
                html: `<div class="marker-pin"></div>`,
                iconSize: [44, 44],
                iconAnchor: [22, 44]
            });
            stationMarker = L.marker([loc.lat, loc.lon], { icon })
                .addTo(leafletMap)
                .bindTooltip(stationName, { permanent: true, direction: 'top', className: 'station-label', offset: [0, -40] })
                .bindPopup(`<b>${stationName}</b><br>Line ${lineNum} → ${destination}<br>${timeStr}`)
                .openPopup();
            fitMapToMarkers();
        })
        .catch(() => {
            const info = document.getElementById('map-dep-info');
            info.innerHTML += '<div class="map-no-loc">⚠ Station coordinates not yet available — check back after first data refresh</div>';
        });

    // Route path for this line/direction (fetched once — the path doesn't change during a viewing session)
    fetch(`/api/lines/${encodeURIComponent(lineNum)}/shape?direction=${encodeURIComponent(destination)}`)
        .then(r => r.json())
        .then(data => {
            if (routePolyline) { routePolyline.remove(); routePolyline = null; }
            if (data.points && data.points.length > 1 && leafletMap) {
                routePolyline = L.polyline(data.points, { color: '#ffffff', weight: 2, opacity: 0.7 }).addTo(leafletMap);
            }
        })
        .catch(() => { /* best-effort — no route line drawn */ });

    // Live vehicle positions for this line/direction
    pollVehicles(lineNum, destination, siteId, stationName);
    vehiclePollTimerId = setInterval(() => pollVehicles(lineNum, destination, siteId, stationName), 10000);
}

function vehicleTooltipHtml(destination, etaIso) {
    const mins = etaIso ? minutesUntil(etaIso) : null;
    const etaText = mins === null ? '' : mins <= 0 ? ' · arriving' : ` · ${mins} min`;
    return `<span class="vehicle-label-dest">→ ${destination || ''}</span><span class="vehicle-label-eta">${etaText}</span>`;
}

// sameDirection: true (heading your way) | false (opposite) | null/undefined (unknown)
function vehicleIcon(sameDirection) {
    const dirClass = sameDirection === true ? 'dir-same' : sameDirection === false ? 'dir-opposite' : 'dir-unknown';
    return L.divIcon({
        className: 'vehicle-marker',
        html: `<div class="vehicle-dot ${dirClass}"></div>`,
        iconSize: [18, 18],
        iconAnchor: [9, 9]
    });
}

function fitMapToMarkers() {
    if (!leafletMap) return;
    const markers = [stationMarker, ...[...vehicleMarkersById.values()].map(e => e.marker)].filter(Boolean);
    if (markers.length < 1) return;
    if (markers.length === 1) {
        leafletMap.setView(markers[0].getLatLng(), 16);
        return;
    }
    leafletMap.fitBounds(L.featureGroup(markers).getBounds(), { padding: [40, 40], maxZoom: 16 });
}

function updateMapLiveStatus() {
    const dot = document.getElementById('map-live-dot');
    const label = document.getElementById('map-live-label');
    if (!dot || !label) return;

    if (mapLoadState === 'unavailable') {
        dot.className = 'conn-dot';
        label.textContent = 'Live tracking unavailable';
        return;
    }

    const stalled = !lastVehiclePollAt || (Date.now() - lastVehiclePollAt) > MAP_LIVE_GRACE_MS;
    if (stalled) {
        dot.className = 'conn-dot stale';
        label.textContent = 'reconnecting...';
    } else {
        dot.className = 'conn-dot online';
        const secs = Math.max(0, Math.floor((Date.now() - lastVehiclePollAt) / 1000));
        label.textContent = `updated ${secs}s ago`;
    }
}

function pollVehicles(lineNum, destination, siteId, stationName) {
    if (!leafletMap) return;
    fetch(`/api/lines/${encodeURIComponent(lineNum)}/vehicles?direction=${encodeURIComponent(destination)}&site_id=${encodeURIComponent(siteId)}&station_name=${encodeURIComponent(stationName)}`)
        .then(r => r.json())
        .then(data => {
            if (!data.available) {
                mapLoadState = 'unavailable';
            } else {
                mapLoadState = 'live';
                lastVehiclePollAt = Date.now();
            }
            finishFirstPoll();
            updateMapLiveStatus();

            const info = document.getElementById('map-dep-info');
            const existingNote = info.querySelector('.map-no-vehicles');
            if (existingNote) existingNote.remove();

            const vehicles = data.vehicles || [];
            if (data.available && vehicles.length === 0) {
                info.insertAdjacentHTML('beforeend',
                    '<div class="map-no-vehicles">No live vehicles currently reported for this line</div>');
            }

            const seenKeys = new Set();
            vehicles.forEach(v => {
                if (v.lat == null || v.lon == null || !leafletMap) return;
                const key = v.vehicle_id || v.trip_id || null;

                if (key == null) {
                    // Unkeyable — can't track identity across polls, render a plain one-off marker.
                    L.marker([v.lat, v.lon], { icon: vehicleIcon(v.same_direction ?? null) }).addTo(leafletMap);
                    return;
                }

                seenKeys.add(key);
                const existing = vehicleMarkersById.get(key);
                if (existing) {
                    const sameDirection = v.same_direction ?? null;

                    existing.marker.setLatLng([v.lat, v.lon]);
                    if (sameDirection !== existing.sameDirection) {
                        existing.marker.setIcon(vehicleIcon(sameDirection));
                    }
                    existing.lat = v.lat;
                    existing.lon = v.lon;
                    existing.sameDirection = sameDirection;

                    if (v.destination && (v.destination !== existing.destination || v.eta_iso !== existing.etaIso)) {
                        const html = vehicleTooltipHtml(v.destination, v.eta_iso);
                        if (existing.marker.getTooltip()) {
                            existing.marker.setTooltipContent(html);
                        } else {
                            existing.marker.bindTooltip(html, { permanent: true, direction: 'right', className: 'vehicle-label', offset: [10, 0] });
                        }
                    }
                    existing.destination = v.destination ?? null;
                    existing.etaIso = v.eta_iso ?? null;
                } else {
                    const marker = L.marker([v.lat, v.lon], { icon: vehicleIcon(v.same_direction ?? null) }).addTo(leafletMap);
                    if (v.destination) {
                        marker.bindTooltip(vehicleTooltipHtml(v.destination, v.eta_iso), {
                            permanent: true, direction: 'right', className: 'vehicle-label', offset: [10, 0]
                        });
                    }
                    vehicleMarkersById.set(key, {
                        marker, lat: v.lat, lon: v.lon, sameDirection: v.same_direction ?? null,
                        destination: v.destination ?? null, etaIso: v.eta_iso ?? null
                    });
                }
            });

            // Drop markers for vehicles no longer reported by the feed.
            vehicleMarkersById.forEach((entry, key) => {
                if (!seenKeys.has(key)) {
                    entry.marker.remove();
                    vehicleMarkersById.delete(key);
                }
            });

            if (!hasFitBounds && vehicleMarkersById.size > 0) {
                fitMapToMarkers();
                hasFitBounds = true;
            }
        })
        .catch(() => {
            finishFirstPoll();
            updateMapLiveStatus();
            /* best-effort — leave existing markers/state untouched on transient failure */
        });
}

function finishFirstPoll() {
    if (firstPollDone) return;
    firstPollDone = true;
    const spinner = document.getElementById('map-loading-spinner');
    if (spinner) spinner.style.display = 'none';
}

function closeMap() {
    document.getElementById('map-modal').style.display = 'none';
    document.body.style.overflow = '';
    if (vehiclePollTimerId) {
        clearInterval(vehiclePollTimerId);
        vehiclePollTimerId = null;
    }
    resetMapState();
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
