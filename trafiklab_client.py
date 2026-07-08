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
import math
import os
import sqlite3
import threading
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

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

# stop_times.txt is far too large to parse into an in-memory dict like the other
# static GTFS tables (SL's regional feed's stop_times.txt dwarfs stops/trips/shapes
# combined, which already produce a 100+ MB JSON cache) — it's streamed into this
# small on-disk SQLite index instead, keyed by trip_id, so per-trip stop-sequence
# lookups are fast indexed queries rather than a multi-GB in-memory structure.
_STOP_TIMES_DB_FILE = os.path.join(_CACHE_DIR, '.trafiklab_stop_times.sqlite')
_STOCKHOLM_TZ = ZoneInfo('Europe/Stockholm')


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
                'fetched_at': _static_cache['fetched_at'].isoformat(),
            }, f)
    except Exception as e:
        logger.warning(f"Failed to persist Trafiklab static GTFS cache to disk: {e}")


def _is_fresh(fetched_at, refresh_hours) -> bool:
    return bool(fetched_at) and (datetime.now() - fetched_at).total_seconds() < refresh_hours * 3600


def _stop_times_db_ready() -> bool:
    return os.path.exists(_STOP_TIMES_DB_FILE)


def _parse_gtfs_time_to_seconds(value: Optional[str]) -> Optional[int]:
    """GTFS 'HH:MM:SS' -> seconds since midnight. Can exceed 24:00:00 for
    post-midnight trips (e.g. '25:10:00') — that's preserved, not clamped,
    since the hour-count itself is what signals a post-midnight service time.
    """
    if not value:
        return None
    try:
        h, m, s = value.strip().split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return None


def _build_stop_times_db(zf, dest_path: str) -> bool:
    """Stream stop_times.txt into a small on-disk SQLite index. Never raises.

    Writes to a temp file and atomically renames into place so concurrent
    readers never observe a half-built database. Guarded by an exclusive
    lock file so that multiple gunicorn worker processes (each with their
    own in-memory state) don't all redundantly download+build at once —
    whichever worker gets there first wins; the rest skip silently.
    """
    if 'stop_times.txt' not in zf.namelist():
        return False

    tmp_path = dest_path + '.tmp'
    lock_path = dest_path + '.lock'
    conn = None
    lock_fd = None
    try:
        try:
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False  # another process is already building this

        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        conn = sqlite3.connect(tmp_path)
        conn.execute(
            'CREATE TABLE stop_times ('
            'trip_id TEXT, stop_id TEXT, stop_sequence INTEGER, '
            'arrival_seconds INTEGER, departure_seconds INTEGER, stop_headsign TEXT)'
        )
        with zf.open('stop_times.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
            batch = []
            for row in reader:
                trip_id = row.get('trip_id')
                stop_id = row.get('stop_id')
                if not trip_id or not stop_id:
                    continue
                seq = row.get('stop_sequence')
                batch.append((
                    trip_id,
                    stop_id,
                    int(seq) if seq else None,
                    _parse_gtfs_time_to_seconds(row.get('arrival_time')),
                    _parse_gtfs_time_to_seconds(row.get('departure_time')),
                    row.get('stop_headsign') or None,
                ))
                if len(batch) >= 5000:
                    conn.executemany('INSERT INTO stop_times VALUES (?, ?, ?, ?, ?, ?)', batch)
                    batch = []
            if batch:
                conn.executemany('INSERT INTO stop_times VALUES (?, ?, ?, ?, ?, ?)', batch)
        conn.execute('CREATE INDEX idx_stop_times_trip ON stop_times(trip_id)')
        conn.commit()
        conn.close()
        conn = None
        os.replace(tmp_path, dest_path)
        return True
    except Exception as e:
        logger.warning(f"Failed to build Trafiklab stop_times index: {e}")
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        return False
    finally:
        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except Exception:
                pass
            try:
                os.remove(lock_path)
            except Exception:
                pass


_stop_times_build_lock = threading.Lock()
_stop_times_build_in_progress = False
_stop_times_build_last_attempt: Optional[datetime] = None
# Without a cooldown, every request that finds the index missing spawns its own
# fresh-download retry (the in-progress flag only dedupes *concurrent* attempts,
# not sequential ones) — on a failure (e.g. the static API rate-limiting us) that
# turns into a retry storm hitting the rate-limited endpoint every few seconds,
# which both guarantees it never recovers and burns the 60-calls/30-days budget.
_STOP_TIMES_BUILD_COOLDOWN_SECONDS = 900


def _kick_off_stop_times_build(settings: dict, zip_bytes: Optional[bytes] = None) -> None:
    """Build the stop_times SQLite index in a background thread — never blocks
    the caller. ETA lookups fail open (None/[]) until the index appears, which
    is far preferable to holding an HTTP request open for the ~30s+ it takes
    to download and stream-parse the full regional stop_times.txt.

    Pass zip_bytes when the caller already has a freshly-downloaded static zip
    in hand (the normal _ensure_static_data refresh path) to avoid a second,
    redundant download against the rate-limited static API; omitted, the
    background job fetches its own copy (the one-time backfill path, where
    the rest of static data was already fresh and no download just happened).

    Deduped in-process via a flag (so a burst of requests before the first
    background build finishes doesn't spawn a pile of redundant threads) plus a
    cooldown after each attempt (successful or not) so repeated requests don't
    retry a failing download every few seconds; the file lock inside
    _build_stop_times_db separately dedupes across the multiple OS processes a
    production WSGI server typically runs.
    """
    global _stop_times_build_in_progress, _stop_times_build_last_attempt
    with _stop_times_build_lock:
        if _stop_times_build_in_progress:
            return
        if _stop_times_build_last_attempt is not None:
            elapsed = (datetime.now() - _stop_times_build_last_attempt).total_seconds()
            if elapsed < _STOP_TIMES_BUILD_COOLDOWN_SECONDS:
                return
        _stop_times_build_in_progress = True
        _stop_times_build_last_attempt = datetime.now()

    def _job():
        global _stop_times_build_in_progress
        try:
            data = zip_bytes
            if data is None:
                operator = settings.get('trafiklab_operator_id', 'sl')
                url = f"{BASE_URL}/gtfs/{operator}/{operator}.zip"
                resp = fetch_with_retry(
                    url,
                    headers={'Accept-Encoding': 'gzip, deflate'},
                    params={'key': TRAFIKLAB_STATIC_API_KEY},
                    timeout=30,
                )
                data = resp.content
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                _build_stop_times_db(zf, _STOP_TIMES_DB_FILE)
        except Exception as e:
            logger.warning(f"Background stop_times index build failed: {e}")
        finally:
            with _stop_times_build_lock:
                _stop_times_build_in_progress = False

    threading.Thread(target=_job, daemon=True, name='stop_times_index_build').start()


def _ensure_static_data(settings: dict) -> bool:
    """Refresh the static GTFS dataset if missing or stale. Returns True if usable data is available.

    Checks, in order: fresh in-memory data -> fresh on-disk data (no network call) ->
    network fetch (persisted to disk on success) -> stale in-memory/disk data as a last resort.

    The stop_times SQLite index is treated as a lagging, best-effort sidecar
    here, never a blocking precondition: if it's missing, a background build
    is kicked off (see _kick_off_stop_times_build) and this function returns
    as soon as the rest of the static data (stops/routes/trips/shapes) is
    usable, exactly as it did before that index existed.
    """
    if not is_static_enabled():
        return False

    refresh_hours = settings.get('trafiklab_static_refresh_hours', 168)  # default: weekly, well within the 60/30d cap

    def _ready() -> bool:
        if not _stop_times_db_ready():
            _kick_off_stop_times_build(settings)
        return True

    if _is_fresh(_static_cache['fetched_at'], refresh_hours):
        return _ready()

    if _static_cache['fetched_at'] is None and _load_disk_cache():
        if _is_fresh(_static_cache['fetched_at'], refresh_hours):
            return _ready()

    with _static_lock:
        # Re-check after acquiring the lock in case another request already refreshed it.
        if _is_fresh(_static_cache['fetched_at'], refresh_hours):
            return _ready()

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
                        }

            _static_cache['stops_by_id'] = stops_by_id
            _static_cache['stops_by_parent'] = stops_by_parent
            _static_cache['stops_by_name'] = stops_by_name
            _static_cache['routes_by_short_name'] = routes_by_short_name
            _static_cache['trips_by_id'] = trips_by_id
            _static_cache['fetched_at'] = datetime.now()
            _save_disk_cache()
            logger.info(
                "Loaded Trafiklab static GTFS: %d stops, %d routes, %d trips",
                len(stops_by_id), len(routes_by_short_name), len(trips_by_id)
            )
            if not _stop_times_db_ready():
                # Reuse the zip bytes we already have in hand — avoids a second,
                # redundant download against the rate-limited static API.
                _kick_off_stop_times_build(settings, zip_bytes=resp.content)
            return True
        except Exception as e:
            logger.warning(f"Failed to refresh Trafiklab static GTFS data: {e}")
            # Fall back to whatever we have — stale in-memory data, or a stale disk cache
            # we haven't loaded yet — rather than a hard failure, since a few days of
            # slightly-stale schedule data beats none, and we don't want a rate-limited
            # or transient failure to spend towards yet another retry.
            if _static_cache['fetched_at']:
                return _ready()
            return _load_disk_cache()


def _site_stop_ids(site_id, name_hint: Optional[str] = None) -> set:
    """All GTFS stop_ids (platforms + itself) belonging to a site/station.

    SL's site_id and GTFS stop_id are frequently unrelated numbering schemes —
    a direct ID match often finds nothing at all, so when a name_hint is
    available it's used to match by normalized stop name instead.
    """
    site_id_str = str(site_id)
    ids = set(_static_cache['stops_by_parent'].get(site_id_str, []))
    ids.add(site_id_str)
    if name_hint:
        for stop_id in _static_cache['stops_by_name'].get(_normalize_name(name_hint), []):
            ids.add(stop_id)
            ids.update(_static_cache['stops_by_parent'].get(stop_id, []))
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


def get_trip_delay_info(site_id, line: str, destination: Optional[str], scheduled_iso: str, settings: dict, name_hint: Optional[str] = None) -> Optional[dict]:
    """Best-effort GTFS-RT delay for the SL departure identified by (site, line, destination, scheduled time).

    name_hint (the station's display name) matters here — SL's site_id and GTFS
    stop_id are frequently unrelated numbering schemes (see _site_stop_ids), so
    without it this often resolves to an empty stop set and silently matches
    nothing.

    Returns None on no confident match — callers must treat that as "skip enrichment," not an error.
    """
    if not _ensure_static_data(settings) or not scheduled_iso:
        return None

    site_stop_ids = _site_stop_ids(site_id, name_hint=name_hint)
    route_ids = set(_static_cache['routes_by_short_name'].get(str(line), []))
    if not route_ids:
        return None

    try:
        scheduled_dt = datetime.fromisoformat(scheduled_iso.replace('Z', '+00:00'))
        # SL's own API returns naive local (Europe/Stockholm) wall-clock time here —
        # unlike GTFS-RT's tz-aware UTC timestamps below, it carries no offset at
        # all, so it must be localized before the two can be compared/subtracted.
        if scheduled_dt.tzinfo is None:
            scheduled_dt = scheduled_dt.replace(tzinfo=_STOCKHOLM_TZ)
        scheduled_dt = scheduled_dt.astimezone(timezone.utc)
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


# ── Logical-line progress (inline vehicle tracker) ──────────────────────────

_PROGRESS_CONFIDENCE_THRESHOLD_M = 1500  # beyond this, a vehicle's GPS projection is untrustworthy


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance between two points, in meters."""
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _to_local_xy(lat, lon, ref_lat) -> tuple:
    """Cheap equirectangular projection to local meters, accurate enough over the
    few-stop windows this is used for (a handful of km at most)."""
    R = 6371000.0
    x = math.radians(lon) * R * math.cos(math.radians(ref_lat))
    y = math.radians(lat) * R
    return x, y


def get_trip_stop_chain(trip_id: str, site_id, before: int = 3, name_hint: Optional[str] = None) -> Optional[dict]:
    """The last `before` stops leading up to (and including) site_id's stop on this
    trip, ordered furthest-upstream-first, each tagged with cumulative haversine
    distance from the first stop in the window ('dist_from_start').

    name_hint (the station's display name) matters — see _site_stop_ids — without
    it, site_id frequently resolves to no real GTFS stop_ids at all.

    Returns None if the trip/stop can't be resolved (index not built yet, trip
    doesn't serve this stop, fewer than 2 usable stops in the window, etc.) —
    always fails open.
    """
    if not trip_id or not _stop_times_db_ready():
        return None
    site_stop_ids = _site_stop_ids(site_id, name_hint=name_hint)
    if not site_stop_ids:
        return None

    try:
        conn = sqlite3.connect(f'file:{_STOP_TIMES_DB_FILE}?mode=ro', uri=True, timeout=2)
        try:
            rows = conn.execute(
                'SELECT stop_id, stop_sequence FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence',
                (trip_id,),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as e:
        logger.warning(f"stop chain lookup failed for trip {trip_id}: {e}")
        return None

    if not rows:
        return None

    target_idx = next((i for i, (stop_id, _seq) in enumerate(rows) if stop_id in site_stop_ids), None)
    if target_idx is None:
        return None

    window = rows[max(0, target_idx - before):target_idx + 1]
    stops_by_id = _static_cache['stops_by_id']
    chain = []
    for stop_id, _seq in window:
        entry = stops_by_id.get(stop_id)
        if not entry:
            continue
        chain.append({'name': entry['name'], 'lat': entry['lat'], 'lon': entry['lon']})

    if len(chain) < 2:
        return None

    dist = 0.0
    chain[0]['dist_from_start'] = 0.0
    for i in range(1, len(chain)):
        dist += _haversine_m(chain[i - 1]['lat'], chain[i - 1]['lon'], chain[i]['lat'], chain[i]['lon'])
        chain[i]['dist_from_start'] = dist

    return {'stops': chain, 'total_dist': dist}


def get_vehicle_position_for_trip(trip_id: str, settings: dict) -> Optional[dict]:
    """The live VehiclePositions entry for this exact trip_id, or None."""
    if not trip_id:
        return None
    for vp in _get_realtime_feed('VehiclePositions', settings):
        if vp['trip_id'] == trip_id:
            return vp
    return None


def _project_onto_chain(lat: float, lon: float, stops: list) -> Optional[dict]:
    """Nearest-segment projection of a lat/lon onto the polyline formed by `stops`.

    Returns the along-chain distance ('dist_from_start') of the closest point on
    the polyline, plus the perpendicular distance from it ('perp_dist', meters) —
    callers should treat a large perp_dist as "this vehicle isn't really on this
    stretch of track" rather than trusting the projection.
    """
    if len(stops) < 2:
        return None

    ref_lat = stops[0]['lat']
    vx, vy = _to_local_xy(lat, lon, ref_lat)

    best = None
    for i in range(len(stops) - 1):
        ax, ay = _to_local_xy(stops[i]['lat'], stops[i]['lon'], ref_lat)
        bx, by = _to_local_xy(stops[i + 1]['lat'], stops[i + 1]['lon'], ref_lat)
        dx, dy = bx - ax, by - ay
        seg_len_sq = dx * dx + dy * dy
        t = 0.0 if seg_len_sq == 0 else max(0.0, min(1.0, ((vx - ax) * dx + (vy - ay) * dy) / seg_len_sq))
        proj_x, proj_y = ax + t * dx, ay + t * dy
        perp_dist = math.hypot(vx - proj_x, vy - proj_y)
        if best is None or perp_dist < best['perp_dist']:
            seg_dist = stops[i]['dist_from_start'] + t * (stops[i + 1]['dist_from_start'] - stops[i]['dist_from_start'])
            best = {'perp_dist': perp_dist, 'dist_from_start': seg_dist}

    return best


def get_line_progress(line: str, destination: Optional[str], site_id, trip_ids: list, settings: dict, before: int = 3, name_hint: Optional[str] = None) -> dict:
    """Stop chain + live vehicle progress for a group of trips on the same line/destination.

    The soonest trip_id (callers should pass them in departure-time order) that
    resolves a stop chain becomes the shared track; every trip_id is then
    independently matched to its own vehicle and projected onto that track, so
    several upcoming vehicles for the same line/direction can be plotted
    together. `progress` is 0 at the furthest-upstream stop in the window and 1
    at the target station. Always fails open — {'stops': [], 'vehicles': []}
    if nothing can be resolved.
    """
    if not _ensure_static_data(settings):
        return {'stops': [], 'vehicles': []}

    chain = None
    for tid in trip_ids:
        chain = get_trip_stop_chain(tid, site_id, before=before, name_hint=name_hint)
        if chain:
            break

    if not chain:
        return {'stops': [], 'vehicles': []}

    stops = chain['stops']
    total_dist = chain['total_dist']

    vehicles = []
    for tid in trip_ids:
        vp = get_vehicle_position_for_trip(tid, settings)
        if not vp:
            continue
        proj = _project_onto_chain(vp['lat'], vp['lon'], stops)
        if not proj or proj['perp_dist'] > _PROGRESS_CONFIDENCE_THRESHOLD_M:
            continue
        progress = (proj['dist_from_start'] / total_dist) if total_dist > 0 else 0.0
        vehicles.append({
            'trip_id': tid,
            'progress': max(0.0, min(1.0, progress)),
            'updated_at': vp['timestamp'],
        })

    return {
        'stops': [{'name': s['name'], 'dist_from_start': s['dist_from_start']} for s in stops],
        'vehicles': vehicles,
    }


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
