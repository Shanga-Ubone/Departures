import json
import os
import sys
import requests
import time

# ANSI colors for console output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def load_config():
    """Load configuration from config.json."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"{RED}Error loading config.json: {e}{RESET}")
        sys.exit(1)

def main():
    # Enable ANSI colors in Windows terminal
    os.system('color')
    
    config = load_config()
    api_base_url = config.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
    timeout = config.get('api_timeout', 10)
    monitored_routes = config.get('monitored_routes', [])

    print(f"Validating {len(monitored_routes)} routes from config.json...\n")

    # Group by site to avoid spamming API
    sites_to_check = {}
    for route in monitored_routes:
        site_id = route['id']
        if site_id not in sites_to_check:
            sites_to_check[site_id] = []
        sites_to_check[site_id].append(route)

    headers = {"User-Agent": "SLTrafficMonitor/1.0"}
    
    total_ok = 0
    total_fail = 0

    for site_id, routes in sites_to_check.items():
        print(f"Checking Site ID {site_id} ({len(routes)} routes)...")
        url = f"{api_base_url}/{site_id}/departures"
        
        try:
            # Add forecast param and delay to match interactive behavior and avoid rate limits
            response = requests.get(url, headers=headers, params={'forecast': 60}, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            departures = data.get('departures', [])
            
            site_name = "Unknown Station"
            if departures:
                site_name = departures[0].get('stop_area', {}).get('name', 'Unknown Station')

            if not departures:
                print(f"  {YELLOW}Warning: No departures returned for site {site_id}. Cannot validate routes.{RESET}")
                print(f"  DEBUG: Status: {response.status_code}")
                print(f"  DEBUG: Response: {response.text[:100]}...")
                for route in routes:
                    print(f"  ? {route['group']} | Line {route['line']} to {route['dest']} - {YELLOW}NO DATA{RESET}")
                continue

            print(f"  Station: {site_name}")

            for route in routes:
                target_line = str(route['line'])
                target_dest = route['dest'].lower()
                
                found = False
                matched_destinations = []
                
                # Check against all departures
                for dep in departures:
                    line_info = dep.get('line', {})
                    line_num = str(line_info.get('designation') if isinstance(line_info, dict) else dep.get('line_designation'))
                    dest = dep.get('destination', '')
                    
                    if line_num == target_line:
                        matched_destinations.append(dest)
                        # Logic matches app.py: configured dest must be IN the api dest
                        if target_dest in dest.lower():
                            found = True
                            break
                
                if found:
                    print(f"  {GREEN}OK{RESET}   {route['group']} | Line {target_line} to {route['dest']}")
                    total_ok += 1
                else:
                    print(f"  {RED}FAIL{RESET} {route['group']} | Line {target_line} to {route['dest']}")
                    if matched_destinations:
                        unique_dests = sorted(list(set(matched_destinations)))
                        print(f"       {YELLOW}Found Line {target_line} but destinations were: {', '.join(unique_dests)}{RESET}")
                        print(f"       {YELLOW}Configured '{route['dest']}' must be inside one of those.{RESET}")
                    else:
                        print(f"       {YELLOW}Line {target_line} not found in current departures.{RESET}")
                        # Show available lines to help
                        available_lines = sorted(list(set([
                            str(d.get('line', {}).get('designation') or d.get('line_designation')) 
                            for d in departures
                        ])))
                        print(f"       Available lines: {', '.join(available_lines)}")
                    total_fail += 1

        except Exception as e:
            print(f"  {RED}API Error for site {site_id}: {e}{RESET}")
            total_fail += len(routes)
        
        # Small delay to avoid hitting API rate limits
        time.sleep(0.5)
        print("") # Newline between sites

    print("-" * 40)
    print(f"Summary: {GREEN}{total_ok} OK{RESET}, {RED}{total_fail} Failed/Warning{RESET}")

if __name__ == "__main__":
    main()
