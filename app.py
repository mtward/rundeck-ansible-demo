import sqlite3
import json
import math
import datetime
from flask import Flask, render_template, request, g

app = Flask(__name__)

DB_PATH = '/var/cache/ansible_logs/logs.db'
PER_PAGE = 100

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        try:
            db = g._database = sqlite3.connect(DB_PATH)
            db.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return None
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/playbooks')
def api_playbooks():
    db = get_db()
    if not db:
        return {"error": "Could not connect to database"}, 500

    query = """
        SELECT 
            playbook_uuid, 
            playbook, 
            MIN(timestamp) as start_time, 
            MAX(timestamp) as end_time,
            COUNT(*) as task_count 
        FROM task_logs 
        WHERE playbook_uuid IS NOT NULL AND playbook_uuid != 'N/A'
        GROUP BY playbook_uuid 
        ORDER BY start_time DESC
    """
    
    cur = db.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    
    results = []
    for row in rows:
        results.append({
            "playbook_uuid": row["playbook_uuid"],
            "playbook": row["playbook"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "task_count": row["task_count"]
        })
        
    return {"data": results}

@app.route('/api/logs')
def api_logs():
    db = get_db()
    if not db:
        return {"error": "Could not connect to database"}, 500

    # 1. Get filter parameters
    host = request.args.get('host', '')
    playbook = request.args.get('playbook', '')
    playbook_uuid = request.args.get('playbook_uuid', '')
    module = request.args.get('module', '')
    task = request.args.get('task', '')
    status = request.args.get('status', 'ALL')
    
    # Date filters
    year = request.args.get('year', '')
    month = request.args.get('month', '')
    day = request.args.get('day', '')
    hour = request.args.get('hour', '')
    
    # Pagination
    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1
    
    offset = (page - 1) * PER_PAGE

    # 2. Build Base Query (Used for both Count and Data)
    where_clause = "WHERE 1=1"
    params = []

    if host:
        where_clause += " AND inventory_hostname LIKE ?"
        params.append(f'%{host}%')
    if playbook:
        where_clause += " AND playbook LIKE ?"
        params.append(f'%{playbook}%')
    if playbook_uuid:
        where_clause += " AND playbook_uuid = ?"
        params.append(playbook_uuid)
    if module:
        where_clause += " AND module LIKE ?"
        params.append(f'%{module}%')
    if task:
        where_clause += " AND task_name LIKE ?"
        params.append(f'%{task}%')
    if status != 'ALL':
        where_clause += " AND status = ?"
        params.append(status)

    # Date Logic
    if year:
        where_clause += " AND strftime('%Y', timestamp) = ?"
        params.append(year)
    if month:
        where_clause += " AND strftime('%m', timestamp) = ?"
        params.append(month.zfill(2))
    if day:
        where_clause += " AND strftime('%d', timestamp) = ?"
        params.append(day.zfill(2))
    if hour:
        where_clause += " AND strftime('%H', timestamp) = ?"
        params.append(hour.zfill(2))

    cur = db.cursor()

    # 3. Get Total Count (for pagination)
    count_query = f"SELECT COUNT(*) FROM task_logs {where_clause}"
    cur.execute(count_query, params)
    total_records = cur.fetchone()[0]
    total_pages = math.ceil(total_records / PER_PAGE)

    # 4. Get Data
    data_query = f"SELECT * FROM task_logs {where_clause} ORDER BY id DESC LIMIT ? OFFSET ?"
    # Append limit/offset to params list
    data_params = params + [PER_PAGE, offset]
    
    cur.execute(data_query, data_params)
    rows = cur.fetchall()

    results = []
    for row in rows:
        # Handle older rows safely
        keys = row.keys()
        pb = row["playbook"] if "playbook" in keys and row["playbook"] else "-"
        pb_uuid = row["playbook_uuid"] if "playbook_uuid" in keys and row["playbook_uuid"] else "-"
        mod = row["module"] if "module" in keys and row["module"] else "-"

        results.append({
            "id": row["id"],
            "timestamp": row["timestamp"],
            "inventory_hostname": row["inventory_hostname"],
            "playbook": pb,
            "playbook_uuid": pb_uuid,
            "module": mod,
            "task_name": row["task_name"],
            "status": row["status"],
            "result": row["result"]
        })

    return {
        "data": results,
        "page": page,
        "total_pages": total_pages,
        "total_records": total_records
    }

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
