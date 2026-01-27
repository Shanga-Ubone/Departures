from flask import Flask, render_template, jsonify
import requests
from datetime import datetime

app = Flask(__name__)

# --- YOUR CONFIGURATION ---
MONITORED_ROUTES = [
    {"group": "TO WORK", "id": 1555, "line": "30", "dest": "Solna", "label": None},
    {"group": "TO WORK", "id": 9531, "line": "40", "dest": "Uppsala C", "label": None},
    {"group": "TO WORK", "id": 9531, "line": "41", "dest": "Märsta", "label": None},
    {"group": "TO WORK", "id": 9189, "line": "18", "dest": "Alvik", "label": None},
    {"group": "TO WORK", "id": 9189, "line": "17", "dest": "Åkeshov", "label": None},
    {"group": "TO WORK", "id": 9189, "line": "19", "dest": "Hässelby strand", "label": None},
    {"group": "TO WORK", "id": 9001, "line": "11", "dest": "Akalla", "label": None},
    {"group": "FROM WORK", "id": 9507, "line": "41", "dest": "Södertälje centrum", "label": None},
    {"group": "FROM WORK", "id": 9507, "line": "41", "dest": "Tumba", "label": None},
    {"group": "FROM WORK", "id": 9531, "line": "30", "dest": "Sickla", "label": None},
    {"group": "FROM WORK", "id": 9302, "line": "11", "dest": "Kungsträdgården", "label": None},
    {"group": "FROM WORK", "id": 9001, "line": "19", "dest": "Hagsätra", "label": None},
    {"group": "FROM WORK", "id": 9001, "line": "18", "dest": "Farsta strand", "label": None},
    {"group": "FROM WORK", "id": 9001, "line": "17", "dest": "Skarpnäck", "label": None},
    {"group": "FROM WORK", "id": 9189, "line": "30", "dest": "Sickla", "label": None},
]

def get_departures(site_id, filters):
    base_url = f"https://transport.integration.sl.se/v1/sites/{site_id}/departures"
    headers = {"User-Agent": "SLTrafficMonitor/1.0"}

    try:
        response = requests.get(base_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        departures = data.get('departures', [])
        
        fetched_site_name = None
        if departures:
            fetched_site_name = departures[0].get('stop_area', {}).get('name')
        
        filtered_departures = []
        for dep in departures:
            line_info = dep.get('line', {})
            line_num = line_info.get('designation') if isinstance(line_info, dict) else dep.get('line_designation')
            dest = dep.get('destination', 'Unknown')
            
            for criteria in filters:
                if str(line_num) == str(criteria['line']) and criteria['dest'].lower() in dest.lower():
                    # Calculate status string here for easier frontend handling
                    sched_str = dep.get('scheduled')
                    exp_str = dep.get('expected') or sched_str
                    
                    if sched_str:
                        s_dt = datetime.fromisoformat(sched_str.replace('Z', '+00:00'))
                        e_dt = datetime.fromisoformat(exp_str.replace('Z', '+00:00'))
                        delta = (e_dt - s_dt).total_seconds() / 60
                        
                        if delta > 1: status = f"+{int(delta)} min"
                        elif delta < -1: status = f"{int(delta)} min"
                        else: status = "On Time"
                        
                        # Add formatted time for display
                        dep['display_time'] = e_dt.strftime("%H:%M")
                        dep['status_text'] = status
                        dep['line_num'] = line_num
                        filtered_departures.append(dep)
                    break 

        filtered_departures.sort(key=lambda x: x.get('expected') or x.get('scheduled'))
        return fetched_site_name, filtered_departures[:10]

    except Exception as e:
        print(f"Error: {e}")
        return None, []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    # Organize structure: Group -> Site ID -> Filters
    grouped_config = {}
    for route in MONITORED_ROUTES:
        group = route['group']
        sid = route['id']
        if group not in grouped_config: grouped_config[group] = {}
        if sid not in grouped_config[group]:
            grouped_config[group][sid] = {'label': route.get('label'), 'filters': []}
        grouped_config[group][sid]['filters'].append({'line': route['line'], 'dest': route['dest']})

    results = {}
    # Order groups: TO WORK first, then FROM WORK
    group_order = ["TO WORK", "FROM WORK"]
    for group_name in group_order:
        if group_name in grouped_config:
            results[group_name] = []
            sites = grouped_config[group_name]
            for site_id, data in sites.items():
                api_site_name, matches = get_departures(site_id, data['filters'])
                display_name = data['label'] or api_site_name or f"Site {site_id}"
                if matches:
                    results[group_name].append({
                        "station": display_name,
                        "departures": matches
                    })
    
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True, port=5000)