"""Trafiklab GTFS Regional integration (static schedule data + GTFS-RT feeds).

This module is a best-effort enrichment layer on top of SL's own departures
API. Every public function must fail closed: if the API key is missing or
invalid, or Trafiklab is unreachable, functions return empty/None results
instead of raising, so the rest of the app behaves exactly as it did before
this integration existed.
"""
import csv
import io
import json
import logging
import os
import threading
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional

from google.transit import gtfs_realtime_pb2

from http_utils import fetch_with_retry

logger = logging.getLogger(__name__)

# Trafiklab issues separate API keys per subscription — the static GTFS
# dataset and the realtime GTFS-RT feeds are two different products (both
# listed under "GTFS Regional" in the developer portal), each with its own key.
TRAFIKLAB_STATIC_API_KEY = os.getenv('TRAFIKLAB_STATIC_API_KEY')
TRAFIKLAB_REALTIME_API_KEY = os.getenv('TRAFIKLAB_REALTIME_API_KEY')
BASE_URL = 'https://opendata.samtrafiken.se'


def is_static_enabled() -> bool:
    return bool(TRAFIKLAB_STATIC_API_KEY)


def is_realtime_enabled() -> bool:
    return bool(TRAFIKLAB_REALTIME_API_KEY)


def is_enabled() -> bool:
    """True if both keys are present, i.e. the full feature set (map + board enrichment) is usable."""
    return is_static_enabled() and is_realtime_enabled()


# ── Static GTFS cache (stops / routes / trips) ──────────────────────────────

_static_cache = {
    'stops_by_id': {},        # stop_id -> {lat, lon, name, parent_station}
    'stops_by_parent': {},    # parent_station -> [stop_id, ...]
    'stops_by_name': {},      # normalized name -> [stop_id, ...]
    'routes_by_short_name': {},  # line designation -> [route_id, ...]
    'trips_by_id': {},        # trip_id -> {route_id, trip_headsign, shape_id}
    'shapes_by_id': {},       # shape_id -> [[lat, lon], ...] ordered by shape_pt_sequence
    'fetched_at': None,
}
_static_lock = threading.Lock()

# The static API key is capped at 60 calls/30 days by Trafiklab. Persisting the
# parsed data to disk means a process restart (e.g. a Railway redeploy) reuses
# the last download instead of spending another call — but only if the disk
# survives the restart, which on Railway requires a mounted volume (Railway's
# default filesystem is rebuilt from scratch on every deploy). Set
# TRAFIKLAB_CACHE_DIR to the volume's mount path in production; it defaults to
# the app directory, which is fine for local dev but won't survive redeploys.
_CACHE_DIR = os.getenv('TRAFIKLAB_CACHE_DIR', os.path.dirname(__file__))
_STATIC_CACHE_FILE = os.path.join(_CACHE_DIR, '.trafiklab_static_cache.json')


def _normalize_name(name: str) -> str:
    return (name or '').strip().lower()


def _load_disk_cache() -> bool:
    """Load a previously-persisted static cache from disk into memory, if present. Never raises."""
    try:
        with open(_STATIC_CACHE_FILE, 'r', encoding='utf-8') as f:
            saved = json.load(f)
        _static_cache['stops_by_id'] = saved['stops_by_id']
        _static_cache['stops_by_parent'] = saved['stops_by_parent']
        _static_cache['stops_by_name'] = saved['stops_by_name']
        _static_cache['routes_by_short_name'] = saved['routes_by_short_name']
        _static_cache['trips_by_id'] = saved['trips_by_id']
        # .get() with a default: caches persisted before shapes.txt support was added won't have this key.
        _static_cache['shapes_by_id'] = saved.get('shapes_by_id', {})
        _static_cache['fetched_at'] = datetime.fromisoformat(saved['fetched_at'])
        return True
    except Exception:
        return False


def _save_disk_cache() -> None:
    try:
        with open(_STATIC_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'stops_by_id': _static_cache['stops_by_id'],
                'stops_by_parent': _static_cache['stops_by_parent'],
                'stops_by_name': _static_cache['stops_by_name'],
                'routes_by_short_name': _static_cache['routes_by_short_name'],
                'trips_by_id': _static_cache['trips_by_id'],
                'shapes_by_id': _static_cache['shapes_by_id'],
                'fetched_at': _static_cache['fetched_at'].isoformat(),
            }, f)
    except Exception as e:
        logger.warning(f"Failed to persist Trafiklab static GTFS cache to disk: {e}")


def _is_fresh(fetched_at, refresh_hours) -> bool:
    return bool(fetched_at) and (datetime.now() - fetched_at).total_seconds() < refresh_hours * 3600


def _ensure_static_data(settings: dict) -> bool:
    """Refresh the static GTFS dataset if missing or stale. Returns True if usable data is available.

    Checks, in order: fresh in-memory data -> fresh on-disk data (no network call) ->
    network fetch (persisted to disk on success) -> stale in-memory/disk data as a last resort.
    """
    if not is_static_enabled():
        return False

    refresh_hours = settings.get('trafiklab_static_refresh_hours', 168)  # default: weekly, well within the 60/30d cap
    if _is_fresh(_static_cache['fetched_at'], refresh_hours):
        return True

    if _static_cache['fetched_at'] is None and _load_disk_cache():
        if _is_fresh(_static_cache['fetched_at'], refresh_hours):
            return True

    with _static_lock:
        # Re-check after acquiring the lock in case another request already refreshed it.
        if _is_fresh(_static_cache['fetched_at'], refresh_hours):
            return True

        operator = settings.get('trafiklab_operator_id', 'sl')
        url = f"{BASE_URL}/gtfs/{operator}/{operator}.zip"
        try:
            resp = fetch_with_retry(
                url,
                headers={'Accept-Encoding': 'gzip, deflate'},
                params={'key': TRAFIKLAB_STATIC_API_KEY},
                timeout=30,
            )
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                stops_by_id = {}
                stops_by_parent = {}
                stops_by_name = {}
                with zf.open('stops.txt') as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                    for row in reader:
                        stop_id = row.get('stop_id')
                        lat, lon = row.get('stop_lat'), row.get('stop_lon')
                        if not stop_id or not lat or not lon:
                            continue
                        parent = row.get('parent_station') or None
                        entry = {
                            'lat': float(lat),
                            'lon': float(lon),
                            'name': row.get('stop_name'),
                            'parent_station': parent,
                        }
                        stops_by_id[stop_id] = entry
                        if parent:
                            stops_by_parent.setdefault(parent, []).append(stop_id)
                        stops_by_name.setdefault(_normalize_name(row.get('stop_name')), []).append(stop_id)

                # Regional feeds bundle multiple agencies (e.g. Waxholmsbolaget ferries)
                # that reuse the same numeric line designations as the primary operator's
                # buses/metro/tram — matching route_short_name alone can silently pick up
                # an unrelated ferry route. Restrict to the configured primary agency.
                primary_agency_name = settings.get('trafiklab_primary_agency_name', 'AB Storstockholms Lokaltrafik')
                primary_agency_ids = set()
                with zf.open('agency.txt') as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                    for row in reader:
                        if row.get('agency_name') == primary_agency_name:
                            primary_agency_ids.add(row.get('agency_id'))

                routes_by_short_name = {}
                with zf.open('routes.txt') as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                    for row in reader:
                        route_id = row.get('route_id')
                        short_name = row.get('route_short_name')
                        if not route_id or not short_name:
                            continue
                        if primary_agency_ids and row.get('agency_id') not in primary_agency_ids:
                            continue
                        routes_by_short_name.setdefault(short_name, []).append(route_id)

                trips_by_id = {}
                with zf.open('trips.txt') as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                    for row in reader:
                        trip_id = row.get('trip_id')
                        if not trip_id:
                            continue
                        trips_by_id[trip_id] = {
                            'route_id': row.get('route_id'),
                            'trip_headsign': row.get('trip_headsign') or '',
                            'shape_id': row.get('shape_id') or None,
                        }

                # shapes.txt is an optional GTFS file — some feeds omit it entirely.
                shapes_by_id = {}
                if 'shapes.txt' in zf.namelist():
                    with zf.open('shapes.txt') as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                        raw_points = {}  # shape_id -> [(seq, lat, lon), ...]
                        for row in reader:
                            shape_id = row.get('shape_id')
                            lat, lon = row.get('shape_pt_lat'), row.get('shape_pt_lon')
                            seq = row.get('shape_pt_sequence')
                            if not shape_id or not lat or not lon:
                                continue
                            raw_points.setdefault(shape_id, []).append(
                                (int(seq) if seq else 0, float(lat), float(lon))
                            )
                        for shape_id, pts in raw_points.items():
                            pts.sort(key=lambda p: p[0])  # shape_pt_sequence order, not file order
                            shapes_by_id[shape_id] = [[lat, lon] for _, lat, lon in pts]

            _static_cache['stops_by_id'] = stops_by_id
            _static_cache['stops_by_parent'] = stops_by_parent
            _static_cache['stops_by_name'] = stops_by_name
            _static_cache['routes_by_short_name'] = routes_by_short_name
            _static_cache['trips_by_id'] = trips_by_id
            _static_cache['shapes_by_id'] = shapes_by_id
            _static_cache['fetched_at'] = datetime.now()
            _save_disk_cache()
            logger.info(
                "Loaded Trafiklab static GTFS: %d stops, %d routes, %d trips, %d shapes",
                len(stops_by_id), len(routes_by_short_name), len(trips_by_id), len(shapes_by_id)
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to refresh Trafiklab static GTFS data: {e}")
            # Fall back to whatever we have — stale in-memory data, or a stale disk cache
            # we haven't loaded yet — rather than a hard failure, since a few days of
            # slightly-stale schedule data beats none, and we don't want a rate-limited
            # or transient failure to spend towards yet another retry.
            if _static_cache['fetched_at']:
                return True
            return _load_disk_cache()


def get_station_coords(site_id, settings: dict, name_hint: Optional[str] = None) -> Optional[dict]:
    """Look up a station's coordinates from GTFS static data. Best-effort, returns None if not found."""
    if not _ensure_static_data(settings):
        return None

    site_id_str = str(site_id)
    stops_by_id = _static_cache['stops_by_id']

    if site_id_str in stops_by_id:
        entry = stops_by_id[site_id_str]
        return {'lat': entry['lat'], 'lon': entry['lon'], 'name': entry['name'], 'source': 'gtfs-static'}

    # Site ID might be a parent station with only child platform stops in stops.txt.
    child_ids = _static_cache['stops_by_parent'].get(site_id_str)
    if child_ids:
        entry = stops_by_id[child_ids[0]]
        return {'lat': entry['lat'], 'lon': entry['lon'], 'name': entry['name'], 'source': 'gtfs-static'}

    if name_hint:
        candidates = _static_cache['stops_by_name'].get(_normalize_name(name_hint))
        if candidates:
            entry = stops_by_id[candidates[0]]
            return {'lat': entry['lat'], 'lon': entry['lon'], 'name': entry['name'], 'source': 'gtfs-static'}

    return None


def _site_stop_ids(site_id) -> set:
    """All GTFS stop_ids (platforms + itself) belonging to a site/station."""
    site_id_str = str(site_id)
    ids = set(_static_cache['stops_by_parent'].get(site_id_str, []))
    ids.add(site_id_str)
    return ids


# ── Realtime GTFS-RT feeds (short TTL cache) ────────────────────────────────

_realtime_cache = {
    'VehiclePositions': {'data': None, 'ts': None},
    'TripUpdates': {'data': None, 'ts': None},
    'ServiceAlerts': {'data': None, 'ts': None},
}
_realtime_locks = {name: threading.Lock() for name in _realtime_cache}


def _parse_vehicle_positions(feed) -> list:
    entities = []
    for entity in feed.entity:
        if not entity.HasField('vehicle'):
            continue
        v = entity.vehicle
        trip_id = v.trip.trip_id if v.HasField('trip') else None
        route_id = v.trip.route_id if v.HasField('trip') else None
        if not v.HasField('position'):
            continue
        entities.append({
            'trip_id': trip_id or None,
            'route_id': route_id or None,
            'lat': v.position.latitude,
            'lon': v.position.longitude,
            'bearing': v.position.bearing if v.position.HasField('bearing') else None,
            'vehicle_id': v.vehicle.id if v.HasField('vehicle') else None,
            'timestamp': v.timestamp if v.HasField('timestamp') else None,
        })
    return entities


def _parse_trip_updates(feed) -> list:
    entities = []
    for entity in feed.entity:
        if not entity.HasField('trip_update'):
            continue
        tu = entity.trip_update
        trip_id = tu.trip.trip_id if tu.HasField('trip') else None
        route_id = tu.trip.route_id if tu.HasField('trip') else None
        stop_time_updates = []
        for stu in tu.stop_time_update:
            arrival_delay = stu.arrival.delay if stu.HasField('arrival') and stu.arrival.HasField('delay') else None
            arrival_time = stu.arrival.time if stu.HasField('arrival') and stu.arrival.HasField('time') else None
            departure_delay = stu.departure.delay if stu.HasField('departure') and stu.departure.HasField('delay') else None
            departure_time = stu.departure.time if stu.HasField('departure') and stu.departure.HasField('time') else None
            stop_time_updates.append({
                'stop_id': stu.stop_id or None,
                'arrival_delay': arrival_delay,
                'arrival_time': arrival_time,
                'departure_delay': departure_delay,
                'departure_time': departure_time,
            })
        entities.append({'trip_id': trip_id, 'route_id': route_id, 'stop_time_updates': stop_time_updates})
    return entities


def _parse_service_alerts(feed) -> list:
    entities = []
    for entity in feed.entity:
        if not entity.HasField('alert'):
            continue
        a = entity.alert
        informed = [
            {'route_id': ie.route_id or None, 'stop_id': ie.stop_id or None}
            for ie in a.informed_entity
        ]
        header = a.header_text.translation[0].text if a.header_text.translation else ''
        description = a.description_text.translation[0].text if a.description_text.translation else ''
        entities.append({
            'informed_entities': informed,
            'header': header,
            'description': description,
            'effect': gtfs_realtime_pb2.Alert.Effect.Name(a.effect) if a.HasField('effect') else 'UNKNOWN_EFFECT',
        })
    return entities


_FEED_PARSERS = {
    'VehiclePositions': _parse_vehicle_positions,
    'TripUpdates': _parse_trip_updates,
    'ServiceAlerts': _parse_service_alerts,
}


def _get_realtime_feed(feed_name: str, settings: dict) -> list:
    if not is_realtime_enabled():
        return []

    ttl = settings.get('trafiklab_realtime_cache_ttl_seconds', 15)
    cached = _realtime_cache[feed_name]
    if cached['data'] is not None and cached['ts'] is not None:
        if (datetime.now() - cached['ts']).total_seconds() < ttl:
            return cached['data']

    with _realtime_locks[feed_name]:
        cached = _realtime_cache[feed_name]
        if cached['data'] is not None and cached['ts'] is not None:
            if (datetime.now() - cached['ts']).total_seconds() < ttl:
                return cached['data']

        operator = settings.get('trafiklab_operator_id', 'sl')
        url = f"{BASE_URL}/gtfs-rt/{operator}/{feed_name}.pb"
        try:
            resp = fetch_with_retry(
                url,
                headers={'Accept-Encoding': 'gzip, deflate'},
                params={'key': TRAFIKLAB_REALTIME_API_KEY},
                timeout=10,
            )
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(resp.content)
            parsed = _FEED_PARSERS[feed_name](feed)
            _realtime_cache[feed_name] = {'data': parsed, 'ts': datetime.now()}
            return parsed
        except Exception as e:
            logger.warning(f"Failed to fetch Trafiklab {feed_name} feed: {e}")
            return cached['data'] or []


# ── Public matching helpers ──────────────────────────────────────────────────

def _resolve_route_id(entity_route_id, trip_id, trips_by_id) -> Optional[str]:
    """Many SL realtime entities (esp. buses/trams) leave route_id empty on the
    entity itself — it's only resolvable via trips.txt's trip_id -> route_id
    mapping. Rail entities tend to populate route_id directly but omit trip_id.
    Prefer the entity's own route_id; fall back to the static trip lookup.
    """
    if entity_route_id:
        return entity_route_id
    if trip_id:
        return trips_by_id.get(trip_id, {}).get('route_id')
    return None


def get_vehicle_positions(line: str, destination: Optional[str], settings: dict) -> list:
    """Live vehicle positions currently in service on the given line/direction."""
    if not _ensure_static_data(settings):
        return []

    route_ids = set(_static_cache['routes_by_short_name'].get(str(line), []))
    if not route_ids:
        return []

    trips_by_id = _static_cache['trips_by_id']
    dest_lower = (destination or '').lower()
    results = []
    for vp in _get_realtime_feed('VehiclePositions', settings):
        resolved_route_id = _resolve_route_id(vp['route_id'], vp['trip_id'], trips_by_id)
        if resolved_route_id not in route_ids:
            continue
        headsign = trips_by_id.get(vp['trip_id'], {}).get('trip_headsign', '')
        if dest_lower and headsign and dest_lower not in headsign.lower():
            continue
        results.append({
            'lat': vp['lat'],
            'lon': vp['lon'],
            'bearing': vp['bearing'],
            'vehicle_id': vp['vehicle_id'],
            'updated_at': vp['timestamp'],
        })
    return results


def get_route_shape(line: str, destination: Optional[str], settings: dict) -> list:
    """Ordered [[lat, lon], ...] points for the line/direction's route path, for drawing on the map.

    A route can have multiple trip patterns (branches, short-turns) each with their own
    shape_id, so this picks the most common shape_id among matching trips as the
    representative path rather than the first one found — avoids landing on an atypical
    short-turn/depot trip. Best-effort — returns [] if no route/shape data is available.
    """
    if not _ensure_static_data(settings):
        return []

    route_ids = set(_static_cache['routes_by_short_name'].get(str(line), []))
    if not route_ids:
        return []

    trips_by_id = _static_cache['trips_by_id']
    dest_lower = (destination or '').lower()

    def matching_shape_ids(require_destination_match):
        counts = Counter()
        for trip in trips_by_id.values():
            if trip.get('route_id') not in route_ids or not trip.get('shape_id'):
                continue
            if require_destination_match and dest_lower:
                headsign = (trip.get('trip_headsign') or '').lower()
                if not headsign or dest_lower not in headsign:
                    continue
            counts[trip['shape_id']] += 1
        return counts

    counts = matching_shape_ids(require_destination_match=True)
    if not counts:
        counts = matching_shape_ids(require_destination_match=False)
    if not counts:
        return []

    best_shape_id, _ = counts.most_common(1)[0]
    return _static_cache['shapes_by_id'].get(best_shape_id, [])


def get_trip_delay_info(site_id, line: str, destination: Optional[str], scheduled_iso: str, settings: dict) -> Optional[dict]:
    """Best-effort GTFS-RT delay for the SL departure identified by (site, line, destination, scheduled time).

    Returns None on no confident match — callers must treat that as "skip enrichment," not an error.
    """
    if not _ensure_static_data(settings) or not scheduled_iso:
        return None

    site_stop_ids = _site_stop_ids(site_id)
    route_ids = set(_static_cache['routes_by_short_name'].get(str(line), []))
    if not route_ids:
        return None

    try:
        scheduled_dt = datetime.fromisoformat(scheduled_iso.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None

    trips_by_id = _static_cache['trips_by_id']
    dest_lower = (destination or '').lower()
    tolerance_seconds = 600

    best, best_diff = None, tolerance_seconds
    for tu in _get_realtime_feed('TripUpdates', settings):
        resolved_route_id = _resolve_route_id(tu['route_id'], tu['trip_id'], trips_by_id)
        if resolved_route_id not in route_ids:
            continue
        headsign = trips_by_id.get(tu['trip_id'], {}).get('trip_headsign', '')
        if dest_lower and headsign and dest_lower not in headsign.lower():
            continue

        for stu in tu['stop_time_updates']:
            if stu['stop_id'] not in site_stop_ids:
                continue

            predicted_time = stu['arrival_time'] or stu['departure_time']
            delay_seconds = stu['arrival_delay'] if stu['arrival_delay'] is not None else stu['departure_delay']
            if predicted_time is not None:
                predicted_dt = datetime.fromtimestamp(predicted_time, tz=timezone.utc)
            elif delay_seconds is not None:
                predicted_dt = scheduled_dt + timedelta(seconds=delay_seconds)
            else:
                continue

            diff = abs((predicted_dt - scheduled_dt).total_seconds())
            if diff < best_diff:
                best_diff = diff
                best = {
                    'trip_id': tu['trip_id'],
                    'predicted_iso': predicted_dt.isoformat(),
                    'delay_seconds': delay_seconds,
                }

    return best


def get_active_alerts_for_route(line: str, settings: dict) -> list:
    """Active ServiceAlerts for the given line."""
    if not _ensure_static_data(settings):
        return []

    route_ids = set(_static_cache['routes_by_short_name'].get(str(line), []))
    if not route_ids:
        return []

    results = []
    for alert in _get_realtime_feed('ServiceAlerts', settings):
        if any(e['route_id'] in route_ids for e in alert['informed_entities'] if e['route_id']):
            results.append({
                'header': alert['header'],
                'description': alert['description'],
                'effect': alert['effect'],
            })
    return results
