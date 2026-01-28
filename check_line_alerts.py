import json
import os
import sys
import requests

def load_config():
    """Load configuration from config.json."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config.json: {e}")
        sys.exit(1)

def main():
    config = load_config()
    api_base_url = config.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
    timeout = config.get('api_timeout', 10)
    
    print("SL Transport API - Global Alert Checker")
    print("=======================================")

    monitored_routes = config.get('monitored_routes', [])
    site_ids = set(route['id'] for route in monitored_routes)
    
    if not site_ids:
        print("No sites found in config.json.")
        return

    print(f"\nScanning {len(site_ids)} monitored sites for all active alerts...")
    
    headers = {"User-Agent": "SLTrafficMonitor/LineChecker"}
    found_deviations = []

    for site_id in site_ids:
        url = f"{api_base_url}/{site_id}/departures"
        try:
            # Fetch departures with a forecast window
            response = requests.get(url, headers=headers, params={'forecast': 60}, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            
            # Check stop deviations
            stop_deviations = data.get('stop_deviations', [])
            for dev in stop_deviations:
                if dev not in found_deviations:
                    found_deviations.append(dev)

            # Check departure deviations
            departures = data.get('departures', [])
            
            for dep in departures:
                deviations = dep.get('deviations', [])
                for dev in deviations:
                    if dev not in found_deviations:
                        found_deviations.append(dev)
            
            print(f"  Site {site_id}: Scanned")

        except Exception:
            # Skip errors (e.g. timeouts) to keep scanning other sites
            continue

    print(f"\nFound {len(found_deviations)} unique alerts:\n")
    
    if found_deviations:
        print(json.dumps(found_deviations, indent=2, ensure_ascii=False))
    else:
        print("No alerts found.")

if __name__ == "__main__":
    main()