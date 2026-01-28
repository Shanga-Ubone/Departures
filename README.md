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
- `GET /api/data` - Departure data (cached for 8 seconds)

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
  "group_order": ["TO WORK", "FROM WORK"]
}
```
