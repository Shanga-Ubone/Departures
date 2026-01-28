from flask import Flask, render_template, jsonify
import requests
from datetime import datetime, timedelta
import json
import os
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration from config.json
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found at {config_path}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        return {}

CONFIG = load_config()
API_BASE_URL = CONFIG.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
API_TIMEOUT = CONFIG.get('api_timeout', 10)
MAX_DEPARTURES = CONFIG.get('max_departures_per_station', 10)
CACHE_TTL = CONFIG.get('cache_ttl_seconds', 8)
GROUP_ORDER = CONFIG.get('group_order', ['TO WORK', 'FROM WORK'])

# Cache storage
_cache = {'data': None, 'timestamp': None}

# Build grouped config once at startup (instead of on every request)
def build_grouped_config():
    """Build grouped and deduplicated configuration from monitored routes."""
    grouped = {}
    for route in CONFIG.get('monitored_routes', []):
        group = route['group']
        site_id = route['id']
        
        if group not in grouped:
            grouped[group] = {}
        if site_id not in grouped[group]:
            grouped[group][site_id] = {'label': route.get('label'), 'filters': []}
        
        grouped[group][site_id]['filters'].append({
            'line': str(route['line']),  # Normalize to string
            'dest': route['dest'].lower()  # Normalize to lowercase
        })
    
    return grouped

GROUPED_CONFIG = build_grouped_config()


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
    line_str = str(line_num)
    dest_lower = destination.lower()
    
    for criteria in filters:
        if line_str == criteria['line'] and criteria['dest'] in dest_lower:
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
        'status_text': status,
        'line_num': line_num
    }

def get_departures(site_id, filters):
    """Fetch and filter departures for a site."""
    url = f"{API_BASE_URL}/{site_id}/departures"
    headers = {"User-Agent": "SLTrafficMonitor/1.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        departures = data.get('departures', [])
        
        # Extract site name from first departure
        site_name = None
        if departures:
            site_name = departures[0].get('stop_area', {}).get('name')
        
        # Filter and enrich departures
        filtered = []
        for dep in departures:
            line_info = dep.get('line', {})
            line_num = line_info.get('designation') if isinstance(line_info, dict) else dep.get('line_designation')
            destination = dep.get('destination', 'Unknown')
            
            # Check if this departure matches any filter
            if matches_filter(line_num, destination, filters):
                enriched = enrich_departure(dep, line_num, filters)
                if enriched:
                    filtered.append(enriched)
        
        # Sort by departure time and limit results
        filtered.sort(key=lambda x: x.get('expected') or x.get('scheduled'))
        return site_name, filtered[:MAX_DEPARTURES]
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for site {site_id}: {e}")
        return None, []
    except Exception as e:
        logger.error(f"Unexpected error fetching departures for site {site_id}: {e}")
        return None, []


@app.route('/')
def index():
    return render_template('index.html')

def get_cached_data():
    """Get cached data if still fresh, otherwise return None."""
    if _cache['data'] is None or _cache['timestamp'] is None:
        return None
    
    age = (datetime.now() - _cache['timestamp']).total_seconds()
    if age < CACHE_TTL:
        return _cache['data']
    
    return None

def cache_data(data):
    """Cache data with current timestamp."""
    _cache['data'] = data
    _cache['timestamp'] = datetime.now()

@app.route('/api/data')
def get_data():
    """Get departure data for all monitored routes."""
    # Check cache first
    cached = get_cached_data()
    if cached is not None:
        return jsonify(cached)
    
    # Build results by grouping sites first to avoid duplicate API calls
    site_data = {}  # Map of site_id -> {site_name, groups_needed}
    
    # First pass: collect all sites and their associated groups
    for group in GROUPED_CONFIG.values():
        for site_id in group.keys():
            if site_id not in site_data:
                site_data[site_id] = {'site_name': None, 'groups': {}}
    
    # Second pass: fetch each site only once, then distribute results
    for site_id in site_data.keys():
        # Collect all filters for this site from all groups
        all_filters = []
        for group_name, sites in GROUPED_CONFIG.items():
            if site_id in sites:
                all_filters.extend(sites[site_id]['filters'])
                site_data[site_id]['groups'][group_name] = sites[site_id]
        
        # Fetch departures once for this site
        site_name, departures = get_departures(site_id, all_filters)
        site_data[site_id]['site_name'] = site_name
        site_data[site_id]['departures'] = departures
    
    # Third pass: organize results by group
    results = {}
    for group_name in GROUP_ORDER:
        if group_name not in GROUPED_CONFIG:
            continue
        
        results[group_name] = []
        sites = GROUPED_CONFIG[group_name]
        
        for site_id, site_config in sites.items():
            site_info = site_data.get(site_id, {})
            departures = site_info.get('departures', [])
            
            # Filter departures for this group's specific filters
            group_filters = site_config['filters']
            filtered_deps = [
                dep for dep in departures
                if matches_filter(dep.get('line_num'), dep.get('destination', ''), group_filters)
            ]
            
            if filtered_deps:
                display_name = site_config['label'] or site_info.get('site_name') or f"Site {site_id}"
                results[group_name].append({
                    "station": display_name,
                    "departures": filtered_deps
                })
    
    # Cache the results
    cache_data(results)
    
    return jsonify(results)

if __name__ == '__main__':
    debug_mode = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    port = int(os.getenv('FLASK_PORT', 5000))
    app.run(debug=debug_mode, port=port)