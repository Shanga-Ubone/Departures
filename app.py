from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
import json
import os
import logging
from dataclasses import dataclass
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from http_utils import fetch_with_retry
import trafiklab_client

load_dotenv()

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Data models ────────────────────────────────────────────────────────────────

@dataclass
class RouteFilter:
    line: str  # normalized to string
    dest: str  # normalized to lowercase

@dataclass
class SiteResult:
    site_name: Optional[str]
    departures: List[dict]
    stop_deviations: List[dict]

# ── Config cache ────────────────────────────────────────────────────────────────

_config_cache = {'data': {}, 'grouped': {}, 'mtime': 0}

def get_config():
    """Load configuration, reloading if file changed."""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        current_mtime = os.path.getmtime(config_path)
        if current_mtime > _config_cache['mtime']:
            logger.info("Reloading configuration from config.json")
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            grouped = {}
            for route in config.get('monitored_routes', []):
                group = route['group']
                site_id = route['id']
                if group not in grouped:
                    grouped[group] = {}
                if site_id not in grouped[group]:
                    grouped[group][site_id] = {'label': route.get('label'), 'filters': []}
                grouped[group][site_id]['filters'].append(
                    RouteFilter(line=str(route['line']), dest=route['dest'].lower())
                )

            _config_cache['data'] = config
            _config_cache['grouped'] = grouped
            _config_cache['mtime'] = current_mtime

        return _config_cache['data'], _config_cache['grouped']
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return _config_cache['data'], _config_cache['grouped']

# ── Data caches ────────────────────────────────────────────────────────────────

_cache = {'data': None, 'timestamp': None}
_location_cache: dict = {}  # site_id -> {lat, lon, name}

# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_datetime(iso_string):
    """Parse ISO 8601 datetime string safely."""
    if not iso_string:
        return None
    try:
        return datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to parse datetime '{iso_string}': {e}")
        return None

def calculate_delay_status(scheduled_str, expected_str):
    """Calculate delay status based on scheduled vs expected time."""
    s_dt = parse_datetime(scheduled_str)
    e_dt = parse_datetime(expected_str or scheduled_str)
    if not s_dt or not e_dt:
        return None, None
    delta_minutes = (e_dt - s_dt).total_seconds() / 60
    if delta_minutes > 1:
        status = f"+{int(delta_minutes)} min"
    elif delta_minutes < -1:
        status = f"{int(delta_minutes)} min"
    else:
        status = "On Time"
    return status, e_dt

def matches_filter(line_num, destination, filters):
    """Check if departure matches any of the given filters."""
    line_str = str(line_num) if line_num is not None else ''
    dest_lower = (destination or '').lower()
    for f in filters:
        if isinstance(f, RouteFilter):
            f_line, f_dest = f.line, f.dest
        else:
            f_line, f_dest = f['line'], f['dest']
        if line_str == f_line and f_dest in dest_lower:
            return True
    return False

def enrich_departure(departure, line_num, filters):
    """Add display information to a departure."""
    sched_str = departure.get('scheduled')
    exp_str = departure.get('expected') or sched_str
    status, e_dt = calculate_delay_status(sched_str, exp_str)
    if not status or not e_dt:
        return None
    return {
        **departure,
        'display_time': e_dt.strftime("%H:%M"),
        'expected_iso': e_dt.isoformat(),
        'status_text': status,
        'line_num': line_num
    }

def _add_gtfs_cross_check(dep: dict, site_id, config: dict, station_deviations: Optional[list] = None) -> None:
    """Best-effort GTFS-RT cross-check against SL's own delay/deviation data.

    Mutates `dep` in place with optional `gtfs_cross_check`/`gtfs_alert` keys.
    Never raises — a failed or absent match just means no enrichment for this row.
    """
    try:
        line_num = dep.get('line_num')
        destination = dep.get('destination')
        scheduled = dep.get('scheduled')
        if not line_num or not scheduled:
            return

        match = trafiklab_client.get_trip_delay_info(site_id, line_num, destination, scheduled, config)
        if match and match.get('delay_seconds') is not None:
            sched_dt = parse_datetime(scheduled)
            exp_dt = parse_datetime(dep.get('expected') or scheduled)
            if sched_dt and exp_dt:
                sl_delay_minutes = (exp_dt - sched_dt).total_seconds() / 60
                gtfs_delay_minutes = match['delay_seconds'] / 60
                dep['gtfs_cross_check'] = 'match' if abs(sl_delay_minutes - gtfs_delay_minutes) <= 1 else 'delay_diff'

        alerts = trafiklab_client.get_active_alerts_for_route(line_num, config)
        if alerts:
            existing_messages = {
                (d.get('message') or '').lower()
                for d in list(dep.get('deviations') or []) + list(station_deviations or [])
                if isinstance(d, dict)
            }
            # Exact-text dedup only — SL and Trafiklab word the same disruption
            # differently, so this won't catch every overlap, but it's cheap
            # and avoids the obvious literal duplicates.
            new_alert = next(
                (a for a in alerts if a.get('header', '').lower() not in existing_messages), None
            )
            if new_alert:
                dep['gtfs_alert'] = {'header': new_alert['header'], 'effect': new_alert['effect']}
    except Exception as e:
        logger.warning(f"GTFS cross-check failed for site {site_id} line {dep.get('line_num')}: {e}")


def _try_cache_location(site_id: int, departures: list, site_name: Optional[str]) -> None:
    """Cache station coordinates extracted from departure stop data."""
    if site_id in _location_cache or not departures:
        return
    first = departures[0]
    for area_key in ('stop_point', 'stop_area'):
        area = first.get(area_key, {})
        if not isinstance(area, dict):
            continue
        lat = area.get('lat') or area.get('latitude') or area.get('lat_wgs84')
        lon = area.get('lon') or area.get('longitude') or area.get('lon_wgs84')
        if lat and lon:
            _location_cache[site_id] = {
                'lat': float(lat), 'lon': float(lon),
                'name': site_name or area.get('name', f'Site {site_id}')
            }
            return

def get_departures(site_id: int, filters: list) -> SiteResult:
    """Fetch and filter departures for a site, with retry and coordinate caching."""
    config, _ = get_config()
    api_base_url = config.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
    api_timeout = config.get('api_timeout', 10)

    url = f"{api_base_url}/{site_id}/departures"
    headers = {"User-Agent": "SLTrafficMonitor/1.0"}
    past_window = 20
    future_window = 60
    start_time = (datetime.utcnow() - timedelta(minutes=past_window)).strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        response = fetch_with_retry(
            url, headers,
            params={'forecast': past_window + future_window, 'time': start_time},
            timeout=api_timeout
        )
        data = response.json()
        raw_departures = data.get('departures', [])
        stop_deviations = data.get('stop_deviations', [])

        site_name = None
        if raw_departures:
            site_name = raw_departures[0].get('stop_area', {}).get('name')

        _try_cache_location(site_id, raw_departures, site_name)

        filtered = []
        for dep in raw_departures:
            line_info = dep.get('line', {})
            line_num = line_info.get('designation') if isinstance(line_info, dict) else dep.get('line_designation')
            destination = dep.get('destination', 'Unknown')
            if matches_filter(line_num, destination, filters):
                enriched = enrich_departure(dep, line_num, filters)
                if enriched:
                    filtered.append(enriched)

        filtered.sort(key=lambda x: x.get('expected') or x.get('scheduled'))
        return SiteResult(site_name=site_name, departures=filtered, stop_deviations=stop_deviations)

    except Exception as e:
        logger.error(f"Error fetching departures for site {site_id}: {e}")
        return SiteResult(site_name=None, departures=[], stop_deviations=[])


@app.route('/')
def index():
    return render_template('index.html')

def get_cached_data(ttl):
    if _cache['data'] is None or _cache['timestamp'] is None:
        return None
    age = (datetime.now() - _cache['timestamp']).total_seconds()
    if age < ttl:
        return _cache['data']
    return None

def cache_data(data):
    _cache['data'] = data
    _cache['timestamp'] = datetime.now()

@app.route('/api/data')
def get_data():
    """Get departure data for all monitored routes."""
    config, grouped_config = get_config()
    cache_ttl = config.get('cache_ttl_seconds', 8)
    group_order = config.get('group_order', ['TO WORK', 'FROM WORK'])
    max_departures = config.get('max_departures_per_station', 10)

    cached = get_cached_data(cache_ttl)
    if cached is not None:
        return jsonify(cached)

    site_data = {}
    for group in grouped_config.values():
        for site_id in group.keys():
            if site_id not in site_data:
                site_data[site_id] = {'site_name': None, 'groups': {}}

    for site_id in site_data.keys():
        all_filters = []
        for group_name, sites in grouped_config.items():
            if site_id in sites:
                all_filters.extend(sites[site_id]['filters'])
                site_data[site_id]['groups'][group_name] = sites[site_id]

        try:
            result = get_departures(site_id, all_filters)
            site_data[site_id]['site_name'] = result.site_name
            site_data[site_id]['departures'] = result.departures
            site_data[site_id]['stop_deviations'] = result.stop_deviations
        except Exception as e:
            logger.error(f"Unexpected error processing site {site_id}: {e}")
            site_data[site_id]['departures'] = []
            site_data[site_id]['stop_deviations'] = []

    results = []
    for group_name in group_order:
        if group_name not in grouped_config:
            continue

        group_stations = []
        group_deviations = []
        sites = grouped_config[group_name]

        for site_id, site_config in sites.items():
            site_info = site_data.get(site_id, {})
            departures = site_info.get('departures', [])

            group_filters = site_config['filters']
            filtered_deps = [
                dep for dep in departures
                if matches_filter(dep.get('line_num'), dep.get('destination', ''), group_filters)
            ]

            if filtered_deps:
                display_deps = filtered_deps[:max_departures]
                if trafiklab_client.is_enabled():
                    station_deviations = site_info.get('stop_deviations') or []
                    for dep in display_deps:
                        _add_gtfs_cross_check(dep, site_id, config, station_deviations)

                display_name = site_config['label'] or site_info.get('site_name') or f"Site {site_id}"
                group_stations.append({
                    "station": display_name,
                    "site_id": site_id,
                    "departures": display_deps
                })

                if site_info.get('stop_deviations'):
                    for dev in site_info['stop_deviations']:
                        d = dev.copy()
                        d['lines'] = set()
                        d['station_wide'] = True
                        group_deviations.append(d)

                for dep in filtered_deps:
                    if dep.get('deviations'):
                        line_num = dep.get('line_num')
                        for dev in dep['deviations']:
                            d = dev.copy()
                            d['lines'] = {str(line_num)} if line_num else set()
                            d['station_wide'] = False
                            group_deviations.append(d)

        if group_stations:
            dev_map = {}
            for dev in group_deviations:
                msg = dev.get('message')
                if not msg:
                    continue
                if msg not in dev_map:
                    dev_map[msg] = dev
                else:
                    dev_map[msg]['lines'].update(dev['lines'])
                    if not dev.get('station_wide', True):
                        dev_map[msg]['station_wide'] = False

            unique_deviations = []
            for dev in dev_map.values():
                effect = dev.get('consequence', 'ALERT')
                text = dev.get('message', '')
                if dev['lines']:
                    sorted_lines = sorted(list(dev['lines']), key=lambda x: (len(x), x))
                    dev['message'] = f"Line {', '.join(sorted_lines)}: [{effect}] {text}"
                else:
                    dev['message'] = f"[{effect}] {text}"
                dev.pop('lines', None)
                dev.setdefault('station_wide', True)
                unique_deviations.append(dev)

            results.append({
                "group": group_name,
                "stations": group_stations,
                "deviations": unique_deviations
            })

    cache_data(results)
    response = jsonify(results)
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response


@app.route('/api/sites/<int:site_id>/location')
def get_site_location(site_id: int):
    """Return geographic coordinates for a station."""
    if site_id in _location_cache:
        return jsonify(_location_cache[site_id])

    config, _ = get_config()
    api_base_url = config.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
    api_timeout = config.get('api_timeout', 10)

    # SL's site IDs and GTFS stop_ids use unrelated numbering schemes, so a
    # station display name is the only reliable way to match them — prefer
    # the name the client already has (e.g. from the departure board) over
    # a config label, since labels are usually unset.
    name_hint = request.args.get('name') or next(
        (r.get('label') for r in config.get('monitored_routes', []) if r.get('id') == site_id and r.get('label')),
        None
    )
    gtfs_result = trafiklab_client.get_station_coords(site_id, config, name_hint=name_hint)
    if gtfs_result:
        _location_cache[site_id] = gtfs_result
        return jsonify(gtfs_result)

    try:
        resp = requests.get(
            f"{api_base_url}/{site_id}",
            headers={"User-Agent": "SLTrafficMonitor/1.0"},
            timeout=api_timeout
        )
        if resp.ok:
            data = resp.json()
            if isinstance(data, dict):
                lat = data.get('lat') or data.get('latitude')
                lon = data.get('lon') or data.get('longitude')
                if lat and lon:
                    result = {'lat': float(lat), 'lon': float(lon), 'name': data.get('name')}
                    _location_cache[site_id] = result
                    return jsonify(result)
    except Exception as e:
        logger.error(f"Location lookup failed for site {site_id}: {e}")

    return jsonify({'error': 'Location not available'}), 404


@app.route('/api/lines/<line>/vehicles')
def get_line_vehicles(line):
    """Live vehicle positions for a line/direction, for the map. Best-effort — always 200."""
    destination = request.args.get('direction', '')
    config, _ = get_config()
    try:
        vehicles = trafiklab_client.get_vehicle_positions(line, destination, config)
    except Exception as e:
        logger.warning(f"Vehicle position lookup failed for line {line}: {e}")
        vehicles = []
    return jsonify({'vehicles': vehicles, 'available': trafiklab_client.is_enabled()})


@app.route('/config')
def config_page():
    return render_template('config.html')


def _fetch_site_name(site_id, api_base_url, api_timeout):
    """Fetch station name for a site by querying its departures endpoint."""
    try:
        url = f"{api_base_url}/{site_id}/departures"
        response = requests.get(url, headers={"User-Agent": "SLTrafficMonitor/1.0"}, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        departures = data.get('departures', [])
        if departures:
            return site_id, departures[0].get('stop_area', {}).get('name')
    except Exception:
        pass
    return site_id, None


@app.route('/api/config', methods=['GET'])
def get_config_routes():
    """Return monitored routes enriched with station names."""
    config, _ = get_config()
    routes = config.get('monitored_routes', [])
    group_order = config.get('group_order', ['TO WORK', 'FROM WORK', 'OTHER'])
    api_base_url = config.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
    api_timeout = config.get('api_timeout', 10)

    unique_ids = list({r['id'] for r in routes})
    site_names = {}
    with ThreadPoolExecutor(max_workers=min(len(unique_ids), 8)) as executor:
        futures = {executor.submit(_fetch_site_name, sid, api_base_url, api_timeout): sid for sid in unique_ids}
        for future in as_completed(futures):
            sid, name = future.result()
            site_names[sid] = name

    enriched = []
    for route in routes:
        r = dict(route)
        r['station_name'] = site_names.get(route['id'])
        enriched.append(r)

    return jsonify({'routes': enriched, 'group_order': group_order})


@app.route('/api/config', methods=['POST'])
def save_config_routes():
    """Save updated monitored_routes to config.json."""
    body = request.get_json(silent=True)
    if not body or 'routes' not in body:
        return jsonify({'error': 'Missing routes'}), 400

    routes = body['routes']
    config, _ = get_config()
    known_groups = set(config.get('group_order', ['TO WORK', 'FROM WORK', 'OTHER']))

    validated = []
    for r in routes:
        if not all(k in r for k in ('group', 'id', 'line', 'dest')):
            return jsonify({'error': 'Each route must have group, id, line, dest'}), 400
        if r['group'] not in known_groups:
            return jsonify({'error': f"Unknown group: {r['group']}"}), 400
        validated.append({
            'group': r['group'],
            'id': int(r['id']),
            'line': str(r['line']),
            'dest': str(r['dest']),
            'label': r.get('label') or None
        })

    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            full_config = json.load(f)
        full_config['monitored_routes'] = validated
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(full_config, f, indent=2, ensure_ascii=False)
        _config_cache['mtime'] = 0
        _cache['data'] = None
        return jsonify({'ok': True})
    except Exception as e:
        logger.error(f"Error saving config: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/search/stations')
def search_stations():
    """Search for stations by name via SL API."""
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({'stations': []})

    config, _ = get_config()
    api_base_url = config.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
    api_timeout = config.get('api_timeout', 10)

    try:
        response = requests.get(
            api_base_url,
            params={'name': q, 'expand': 'true'},
            headers={"User-Agent": "SLTrafficMonitor/1.0"},
            timeout=api_timeout
        )
        response.raise_for_status()
        data = response.json()
        sites = data.get('sites', []) if isinstance(data, dict) else data

        results = [
            {'id': s['id'], 'name': s['name']}
            for s in sites
            if q.lower() in (s.get('name') or '').lower() and s.get('id') and s.get('name')
        ]
        return jsonify({'stations': results[:20]})
    except Exception as e:
        logger.error(f"Station search failed: {e}")
        return jsonify({'stations': [], 'error': str(e)})


@app.route('/api/stations/<int:site_id>/routes')
def get_station_routes(site_id):
    """Return unique line/destination pairs available at a station."""
    config, _ = get_config()
    api_base_url = config.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
    api_timeout = config.get('api_timeout', 10)

    try:
        url = f"{api_base_url}/{site_id}/departures"
        response = requests.get(url, headers={"User-Agent": "SLTrafficMonitor/1.0"}, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        departures = data.get('departures', [])

        site_name = None
        routes_seen = set()
        routes = []

        for dep in departures:
            if site_name is None:
                site_name = dep.get('stop_area', {}).get('name')
            line_info = dep.get('line', {})
            line_num = line_info.get('designation') if isinstance(line_info, dict) else dep.get('line_designation')
            dest = dep.get('destination')
            if line_num and dest:
                key = (str(line_num), dest)
                if key not in routes_seen:
                    routes_seen.add(key)
                    routes.append({'line': str(line_num), 'dest': dest})

        routes.sort(key=lambda x: (len(x['line']), x['line'], x['dest']))
        return jsonify({'site_name': site_name, 'routes': routes})
    except Exception as e:
        logger.error(f"Routes fetch failed for site {site_id}: {e}")
        return jsonify({'site_name': None, 'routes': [], 'error': str(e)})


if __name__ == '__main__':
    debug_mode = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    port = int(os.getenv('FLASK_PORT', 5000))
    app.run(debug=debug_mode, port=port)
