import json
import os
import sys
import requests

def load_config():
    """Load configuration from config.json in the same directory."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: config.json not found at {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config.json: {e}")
        sys.exit(1)

CONFIG = load_config()
API_BASE_URL = CONFIG.get('api_base_url', 'https://transport.integration.sl.se/v1/sites')
API_TIMEOUT = CONFIG.get('api_timeout', 10)

def search_station(search_term):
    """Search for stations by name."""
    print(f"Searching for '{search_term}'...")
    headers = {"User-Agent": "SLTrafficMonitor/1.0"}
    
    try:
        # Try searching using 'name' parameter
        response = requests.get(
            API_BASE_URL, 
            params={'name': search_term, 'expand': 'true'}, 
            headers=headers, 
            timeout=API_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        
        # Handle different response structures (list or dict with 'sites' key)
        sites = data.get('sites', []) if isinstance(data, dict) else data
        
        if not sites:
            print("No stations found.")
            return

        # Filter locally just in case the API didn't filter strictly
        filtered_sites = [
            s for s in sites 
            if search_term.lower() in (s.get('name') or '').lower()
        ]
        
        if not filtered_sites:
            print("No matching stations found after filtering.")
            return

        print(f"\nFound {len(filtered_sites)} stations:")
        print(f"{'ID':<10} | {'Name'}")
        print("-" * 40)
        
        for site in filtered_sites:
            site_id = site.get('id')
            site_name = site.get('name')
            if site_id and site_name:
                print(f"{site_id:<10} | {site_name}")
                
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")

def get_lines_and_destinations(site_id):
    """Fetch departures and list unique lines and destinations."""
    print(f"Fetching departures for Site ID {site_id}...")
    url = f"{API_BASE_URL}/{site_id}/departures"
    headers = {"User-Agent": "SLTrafficMonitor/1.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        departures = data.get('departures', [])
        
        if not departures:
            print("No departures found.")
            return

        site_name = departures[0].get('stop_area', {}).get('name', 'Unknown Station')
        
        # Extract unique lines and destinations
        routes = set()
        for dep in departures:
            line_info = dep.get('line', {})
            line_num = line_info.get('designation') if isinstance(line_info, dict) else dep.get('line_designation')
            dest = dep.get('destination')
            
            if line_num and dest:
                routes.add((str(line_num), dest))
        
        print(f"\nDepartures from {site_name} (ID: {site_id}):")
        print(f"{'Line':<8} | {'Destination'}")
        print("-" * 40)
        
        # Sort by line number then destination
        for line, dest in sorted(list(routes)):
            print(f"{line:<8} | {dest}")
            
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")

def main():
    print("SL Transport API Explorer")
    print("=========================")
    
    try:
        while True:
            print("\nOptions:")
            print("1. Search for Station ID by Name")
            print("2. List Lines and Destinations by Station ID")
            print("q. Quit")
            
            choice = input("\nEnter choice: ").strip().lower()
            
            if choice == '1':
                name = input("Enter station name (or part of it): ").strip()
                if name:
                    search_station(name)
            elif choice == '2':
                site_id = input("Enter Station ID: ").strip()
                if site_id.isdigit():
                    get_lines_and_destinations(int(site_id))
                else:
                    print("Invalid ID. Please enter a number.")
            elif choice == 'q':
                break
            else:
                print("Invalid choice.")
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)

if __name__ == "__main__":
    main()
