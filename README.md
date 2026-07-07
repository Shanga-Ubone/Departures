# Departures - Commute Monitor

A Flask-based application for monitoring public transit departures in Stockholm.

## Configuration

### config.json
All monitored routes and settings are now in `config.json`:
- **monitored_routes**: List of routes to monitor with group, station ID, line, destination
- **api_timeout**: Timeout for SL API requests (default: 10 seconds)
- **max_departures_per_station**: Maximum departures to display per station (default: 10)
- **cache_ttl_seconds**: Cache duration for API responses (default: 8 seconds)
- **group_order**: Order to display groups (default: ["TO WORK", "FROM WORK"])

**To change monitored routes:** Edit `config.json` - no code changes needed!

### Environment Variables
Optional settings via `.env` file or environment:
- **FLASK_DEBUG**: Enable debug mode (default: True)
- **FLASK_PORT**: Server port (default: 5000)
- **TRAFIKLAB_STATIC_API_KEY**: Trafiklab GTFS Regional *static* dataset key (see [Trafiklab Integration](#trafiklab-integration-real-time-vehicles--board-accuracy) below)
- **TRAFIKLAB_REALTIME_API_KEY**: Trafiklab GTFS Regional *realtime* (GTFS-RT) key — a separate subscription/key from the static one
- **TRAFIKLAB_CACHE_DIR**: Directory for the persisted static GTFS cache file. **Must point at a mounted Railway volume in production** (e.g. `/data`) — see the rate-limit warning below. Defaults to the app directory, which is fine for local dev only.

All four Trafiklab variables are optional — the app runs exactly as before if they're unset (no live vehicle markers, no board cross-checks, everything else unaffected).

## Features

### Optimizations Implemented
- **External Configuration**: Routes moved to `config.json` for easy editing
- **Configuration Caching**: Grouped config built once at startup, not on every request
- **API Call Deduplication**: If a station appears in multiple groups, it's fetched once
- **Result Caching**: 8-second cache reduces API calls (frontend refreshes every 20 seconds)
- **Helper Functions**: Cleaner code with dedicated functions for parsing, filtering, and enrichment
- **Better Error Handling**: Proper logging instead of silent failures
- **Environment Support**: Configurable debug mode and port via environment variables

### Frontend Optimizations (index.html)
- **Mobile-responsive design**: Automatically adapts to small screens
- **Service Worker**: Basic offline caching support
- **Efficient updates**: Debounced API calls prevent rapid refreshes
- **Touch-friendly**: Larger tap targets on mobile devices

## Trafiklab Integration (real-time vehicles & board accuracy)

The app integrates [Trafiklab's GTFS Regional API](https://www.trafiklab.se/api/gtfs-datasets/gtfs-regional/) to:
- Show **live, moving vehicle markers** on the map for the line/direction you tap (previously the map only ever showed one static station pin, and often failed with "coordinates not yet available").
- Provide **reliable station coordinates**, matched against Trafiklab's static GTFS stop data instead of SL's flaky opportunistic cache.
- **Cross-check the departure board** against live GTFS-RT `TripUpdates`/`ServiceAlerts`, flagging (via a small `†` icon) when Trafiklab has alert info SL's own feed hasn't attached to a departure yet.

All of this is a best-effort enrichment layer: every Trafiklab lookup fails closed, so a missing/invalid key or a Trafiklab outage leaves the app behaving exactly as it did before this integration existed.

### ⚠️ Static API key is rate-limited to 60 calls/30 days — read before changing anything here

Trafiklab issues **two separate keys** for what looks like one API in the docs:
- **Realtime key** (`TRAFIKLAB_REALTIME_API_KEY`): 30,000 calls/30 days (Bronze tier). Plenty of headroom — the app caches each GTFS-RT feed (VehiclePositions/TripUpdates/ServiceAlerts) for `trafiklab_realtime_cache_ttl_seconds` (default 15s) regardless of how many browser tabs are polling.
- **Static key** (`TRAFIKLAB_STATIC_API_KEY`): **only 60 calls/30 days**. This downloads SL's full schedule zip (stops/routes/trips, ~15MB parsed) used to resolve line numbers → GTFS routes and station IDs → coordinates.

Because Railway rebuilds the container filesystem on every deploy, the static dataset is cached to disk **on a mounted Railway volume** (`TRAFIKLAB_CACHE_DIR=/data` in production) so redeploys reuse the last download instead of spending another call. The refresh interval (`trafiklab_static_refresh_hours`, default 168 = weekly) only actually saves calls if that disk survives between fetches — **without the volume, every redeploy would burn one static call regardless of how recent the data is**, and could exhaust the 60/month budget in days during active development.

**When working on this integration:**
- Don't lower `trafiklab_static_refresh_hours` without good reason — weekly is already a deliberately conservative default.
- Don't delete `.trafiklab_static_cache.json` locally (it's gitignored) unless you're specifically testing the fetch path — deleting it forces a real network call against the *same* static key/quota used in production.
- If you need to inspect the static dataset repeatedly during development, do it from the already-downloaded local cache file rather than re-fetching.
- A `railway restart` reuses the *previous* deployment's environment snapshot — if you change a Trafiklab-related Railway variable, you need `railway redeploy` for it to actually take effect, not just `restart`.

### Architecture notes for future changes
- `trafiklab_client.py` — all Trafiklab logic (static GTFS caching, GTFS-RT feed fetching/parsing, line/station matching). Takes plain config dicts, not a direct import of `app.py`, to avoid a circular import.
- `http_utils.py` — shared `fetch_with_retry`, used by both `app.py` and `trafiklab_client.py`.
- SL's own site IDs (used throughout `config.json`) and GTFS `stop_id`s use **completely unrelated numbering schemes** — matching is done by station name, not ID (see `get_station_coords`).
- SL's regional GTFS feed bundles other agencies under the same numbering (e.g. Waxholmsbolaget ferries can reuse the same line number as an SL bus/metro line) — route matching filters to the primary agency (`trafiklab_primary_agency_name` in `config.json`, default "AB Storstockholms Lokaltrafik") to avoid cross-matching an unrelated service.
- Many realtime entities (especially buses) leave `route_id` **empty on the entity itself** — it's only resolvable via the entity's `trip_id` → static `trips.txt` → `route_id`. Rail/metro entities tend to do the opposite (populate `route_id` directly but omit `trip_id`). `_resolve_route_id()` in `trafiklab_client.py` handles both cases — don't filter on a raw entity's `route_id` field alone.

## Installation

```bash
pip install -r requirements.txt
```

## Running

```bash
python app.py
```

Or with custom settings:
```bash
FLASK_PORT=8000 FLASK_DEBUG=False python app.py
```

## API Endpoints

- `GET /` - Main page
- `GET /api/data` - Departure data (cached for 8 seconds; includes `gtfs_cross_check`/`gtfs_alert` fields on departures when Trafiklab is configured)
- `GET /api/sites/<site_id>/location?name=<station name>` - Station coordinates (GTFS static data first, SL API as fallback)
- `GET /api/lines/<line>/vehicles?direction=<destination>` - Live vehicle positions for a line/direction (always returns HTTP 200; `available: false` if Trafiklab isn't configured)
- `GET /config` - Configuration page
- `GET /api/config`, `POST /api/config` - Read/save monitored routes
- `GET /api/search/stations?q=` - Station name search
- `GET /api/stations/<site_id>/routes` - Line/destination pairs available at a station

## Example config.json Structure

```json
{
  "monitored_routes": [
    {"group": "TO WORK", "id": 1555, "line": "30", "dest": "Solna", "label": null},
    ...
  ],
  "api_timeout": 10,
  "max_departures_per_station": 10,
  "cache_ttl_seconds": 8,
  "group_order": ["TO WORK", "FROM WORK"],
  "trafiklab_operator_id": "sl",
  "trafiklab_primary_agency_name": "AB Storstockholms Lokaltrafik",
  "trafiklab_static_refresh_hours": 168,
  "trafiklab_realtime_cache_ttl_seconds": 15,
  "trafiklab_vehicle_poll_seconds": 10
}
```

The `trafiklab_*` fields are all optional (shown here with their defaults) and only matter if the Trafiklab API keys are configured — see [Trafiklab Integration](#trafiklab-integration-real-time-vehicles--board-accuracy) above, **especially the static key's 60 calls/30 days limit** before changing `trafiklab_static_refresh_hours`.
