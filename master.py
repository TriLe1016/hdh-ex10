import socket
import threading
import time
import json
import struct
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
from common import send_msg, recv_msg

# Global state
lock = threading.Lock()
condition = threading.Condition(lock)

workers = {}        # worker_id -> {cpu_cores, current_load, last_heartbeat, status, sock, addr}
tasks = {}          # task_id -> {task_id, operation, input, status, assigned_worker, created_at, started_at, completed_at, output}
task_id_counter = 1
active_policy = "FIFO"  # FIFO, ROUND_ROBIN, LEAST_LOADED
rr_index = 0
running = True

# Metrics for reporting
completed_tasks_count = 0
total_execution_time = 0.0

def get_status_payload():
    with lock:
        workers_info = {}
        for wid, w in workers.items():
            workers_info[wid] = {
                "worker_id": w["worker_id"],
                "cpu_cores": w["cpu_cores"],
                "current_load": w["current_load"],
                "last_heartbeat": w["last_heartbeat"],
                "status": w["status"],
                "addr": w["addr"]
            }
        
        # Limit tasks in response to avoid HUGE payloads
        tasks_list = list(tasks.values())
        # Sort by ID descending, show latest 100
        tasks_list.sort(key=lambda t: t["task_id"], reverse=True)
        recent_tasks = tasks_list[:100]

        payload = {
            "policy": active_policy,
            "workers": workers_info,
            "tasks": recent_tasks,
            "total_tasks": len(tasks),
            "completed_tasks": completed_tasks_count,
            "avg_execution_time": (total_execution_time / completed_tasks_count) if completed_tasks_count > 0 else 0
        }
        return payload

# HTTP Server for Client API and Web Dashboard
class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress logging to stdout to keep terminal clean
        return

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        if parsed_path.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(get_status_payload()).encode('utf-8'))
        elif parsed_path.path == '/' or parsed_path.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(get_dashboard_html().encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length).decode('utf-8')
        parsed_path = urllib.parse.urlparse(self.path)
        
        try:
            body = json.loads(post_data) if post_data else {}
        except json.JSONDecodeError:
            body = {}

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        response = {"status": "success"}

        if parsed_path.path == '/api/policy':
            global active_policy
            policy = body.get("policy", "FIFO").upper()
            if policy in ["FIFO", "ROUND_ROBIN", "LEAST_LOADED"]:
                with lock:
                    active_policy = policy
                    condition.notify_all()
                response["message"] = f"Policy changed to {policy}"
            else:
                response = {"status": "error", "message": "Invalid policy"}
                
        elif parsed_path.path == '/api/submit':
            global task_id_counter
            submitted_tasks = body.get("tasks", [])
            new_ids = []
            with lock:
                for t in submitted_tasks:
                    tid = task_id_counter
                    task_id_counter += 1
                    tasks[tid] = {
                        "task_id": tid,
                        "operation": t["operation"],
                        "input": t["input"],
                        "status": "READY",
                        "assigned_worker": None,
                        "created_at": time.time(),
                        "started_at": None,
                        "completed_at": None,
                        "output": None
                    }
                    new_ids.append(tid)
                condition.notify_all()
            response["task_ids"] = new_ids
            response["message"] = f"Submitted {len(new_ids)} tasks"

        elif parsed_path.path == '/api/kill_worker':
            worker_id = int(body.get("worker_id", 0))
            with lock:
                worker = workers.get(worker_id)
                if worker and worker["status"] == "ALIVE":
                    # Send kill message
                    send_msg(worker["sock"], {"type": "KILL"})
                    response["message"] = f"Sent kill command to worker {worker_id}"
                else:
                    response = {"status": "error", "message": "Worker not active"}
        else:
            response = {"status": "error", "message": "Endpoint not found"}

        self.wfile.write(json.dumps(response).encode('utf-8'))

def run_http_server(port):
    server = HTTPServer(('0.0.0.0', port), DashboardHandler)
    print(f"[Master] Dashboard/API Server running on port {port}")
    server.serve_forever()

# Worker TCP connection handler
def handle_worker(conn, addr):
    print(f"[Master] New connection from {addr}")
    worker_id = None
    try:
        # Expect Registration message first
        msg = recv_msg(conn)
        if not msg or msg.get("type") != "REGISTER":
            print(f"[Master] Invalid handshake from {addr}")
            conn.close()
            return

        worker_id = msg["worker_id"]
        cpu_cores = msg.get("cpu_cores", 1)

        with lock:
            # Check if this worker already exists and cleanup if needed
            if worker_id in workers:
                old_worker = workers[worker_id]
                try:
                    old_worker["sock"].close()
                except Exception:
                    pass
            
            workers[worker_id] = {
                "worker_id": worker_id,
                "cpu_cores": cpu_cores,
                "current_load": 0,
                "last_heartbeat": time.time(),
                "status": "ALIVE",
                "sock": conn,
                "addr": addr
            }
            print(f"[Master] Registered Worker {worker_id} (cores={cpu_cores})")
            send_msg(conn, {"type": "REGISTER_ACK", "status": "SUCCESS"})
            condition.notify_all()

        # Message loop
        while running:
            msg = recv_msg(conn)
            if msg is None:
                # Connection dropped
                break

            msg_type = msg.get("type")
            if msg_type == "HEARTBEAT":
                with lock:
                    if worker_id in workers:
                        workers[worker_id]["last_heartbeat"] = time.time()
                        workers[worker_id]["status"] = "ALIVE"
            
            elif msg_type == "RESULT":
                tid = msg["task_id"]
                output = msg.get("output")
                exec_time = msg.get("execution_time", 0.0)
                
                global completed_tasks_count, total_execution_time
                with lock:
                    if tid in tasks:
                        t = tasks[tid]
                        t["status"] = "COMPLETED"
                        t["output"] = output
                        t["completed_at"] = time.time()
                        completed_tasks_count += 1
                        total_execution_time += exec_time
                        
                    if worker_id in workers:
                        workers[worker_id]["current_load"] = max(0, workers[worker_id]["current_load"] - 1)
                    
                    condition.notify_all()
                print(f"[Master] Task {tid} completed by Worker {worker_id} in {exec_time:.2f}s")
                
    except Exception as e:
        print(f"[Master] Error handling worker {worker_id or addr}: {e}")
    finally:
        # Mark worker as offline
        if worker_id is not None:
            handle_worker_disconnect(worker_id)
        try:
            conn.close()
        except Exception:
            pass

def handle_worker_disconnect(worker_id):
    with lock:
        if worker_id in workers and workers[worker_id]["status"] == "ALIVE":
            print(f"[Master] Worker {worker_id} disconnected.")
            workers[worker_id]["status"] = "FAILED"
            
            # Reassign its tasks
            reassigned_count = 0
            for tid, t in tasks.items():
                if t["status"] == "RUNNING" and t["assigned_worker"] == worker_id:
                    t["status"] = "READY"
                    t["assigned_worker"] = None
                    reassigned_count += 1
            if reassigned_count > 0:
                print(f"[Master] Requeued {reassigned_count} unfinished tasks from Worker {worker_id}")
            condition.notify_all()

# Heartbeat monitor thread
def monitor_heartbeats():
    while running:
        time.sleep(1.0)
        now = time.time()
        with lock:
            for wid, w in list(workers.items()):
                if w["status"] == "ALIVE" and (now - w["last_heartbeat"]) > 6.0:
                    print(f"[Master] Heartbeat timeout for Worker {wid} (last seen {now - w['last_heartbeat']:.1f}s ago)")
                    w["status"] = "FAILED"
                    
                    # Reassign tasks
                    reassigned_count = 0
                    for tid, t in tasks.items():
                        if t["status"] == "RUNNING" and t["assigned_worker"] == wid:
                            t["status"] = "READY"
                            t["assigned_worker"] = None
                            reassigned_count += 1
                    if reassigned_count > 0:
                        print(f"[Master] Requeued {reassigned_count} unfinished tasks from Worker {wid}")
                    condition.notify_all()

# Scheduler thread
def scheduler_loop():
    global rr_index
    while running:
        task_to_assign = None
        worker_to_assign = None
        
        with condition:
            # Wait until there is a READY task and at least one ALIVE worker
            while running:
                ready_tasks = [t for t in tasks.values() if t["status"] == "READY"]
                alive_workers = [w for w in workers.values() if w["status"] == "ALIVE"]
                
                # Check scheduling policy and eligible workers
                eligible_workers = [w for w in alive_workers if w["current_load"] < w["cpu_cores"]]
                
                if ready_tasks and eligible_workers:
                    # Choose first ready task (FIFO order for tasks)
                    # Tasks are stored in insertion order in dict, let's sort by ID to be strictly FIFO
                    ready_tasks.sort(key=lambda x: x["task_id"])
                    task_to_assign = ready_tasks[0]
                    
                    # Select worker based on policy
                    if active_policy == "FIFO":
                        # Pick the first worker with available capacity
                        eligible_workers.sort(key=lambda x: x["worker_id"])
                        worker_to_assign = eligible_workers[0]
                    elif active_policy == "ROUND_ROBIN":
                        eligible_workers.sort(key=lambda x: x["worker_id"])
                        worker_to_assign = eligible_workers[rr_index % len(eligible_workers)]
                        rr_index = (rr_index + 1) % len(eligible_workers)
                    elif active_policy == "LEAST_LOADED":
                        # Pick the one with the smallest current load, break ties with ID
                        worker_to_assign = min(eligible_workers, key=lambda w: (w["current_load"], w["worker_id"]))
                    
                    if worker_to_assign:
                        # Update status inside the lock
                        task_to_assign["status"] = "RUNNING"
                        task_to_assign["assigned_worker"] = worker_to_assign["worker_id"]
                        task_to_assign["started_at"] = time.time()
                        worker_to_assign["current_load"] += 1
                        break
                
                # If nothing to schedule, wait for changes
                condition.wait(timeout=0.5)
                if not running:
                    return

        # Assign task (do network I/O outside lock)
        if task_to_assign and worker_to_assign:
            msg = {
                "type": "TASK",
                "task_id": task_to_assign["task_id"],
                "operation": task_to_assign["operation"],
                "input": task_to_assign["input"]
            }
            success = send_msg(worker_to_assign["sock"], msg)
            if not success:
                # If sending failed, revert the state
                with lock:
                    task_to_assign["status"] = "READY"
                    task_to_assign["assigned_worker"] = None
                    task_to_assign["started_at"] = None
                    worker_to_assign["current_load"] = max(0, worker_to_assign["current_load"] - 1)
                    worker_to_assign["status"] = "FAILED"
                    condition.notify_all()
                print(f"[Master] Failed to send Task {task_to_assign['task_id']} to Worker {worker_to_assign['worker_id']}. Task requeued.")

# TCP Server for Worker Connections
def run_worker_tcp_server(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(('0.0.0.0', port))
    except Exception as e:
        print(f"[Master] TCP Server failed to bind to port {port}: {e}")
        return
    server.listen(10)
    print(f"[Master] TCP Server listening for workers on port {port}")
    
    while running:
        try:
            server.settimeout(1.0)
            conn, addr = server.accept()
            t = threading.Thread(target=handle_worker, args=(conn, addr), daemon=True)
            t.start()
        except socket.timeout:
            continue
        except Exception as e:
            if running:
                print(f"[Master] TCP Server accept error: {e}")
            break

def get_dashboard_html():
    return """<!DOCTYPE html>
<html>
<head>
    <title>Distributed Scheduler Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0b0f19;
            --card-bg: rgba(20, 30, 55, 0.7);
            --border-color: rgba(255, 255, 255, 0.08);
            --primary: #4f46e5;
            --primary-glow: rgba(79, 70, 229, 0.4);
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
        }
        
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        
        body {
            background-color: var(--bg-color);
            color: var(--text-main);
            font-family: 'Outfit', sans-serif;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            overflow-x: hidden;
        }
        
        header {
            padding: 20px 40px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border-color);
            background: rgba(11, 15, 25, 0.8);
            backdrop-filter: blur(10px);
            position: sticky;
            top: 0;
            z-index: 100;
        }
        
        .logo-container {
            display: flex;
            align-items: center;
            gap: 12px;
        }
        
        .logo-icon {
            width: 32px;
            height: 32px;
            background: linear-gradient(135deg, var(--primary), #a855f7);
            border-radius: 8px;
            box-shadow: 0 0 15px var(--primary-glow);
        }
        
        h1 {
            font-size: 24px;
            font-weight: 800;
            background: linear-gradient(to right, #ffffff, #9ca3af);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .container {
            max-width: 1400px;
            width: 100%;
            margin: 0 auto;
            padding: 30px 40px;
            flex: 1;
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 30px;
        }
        
        @media (max-width: 1024px) {
            .container {
                grid-template-columns: 1fr;
            }
        }
        
        .card {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 24px;
            backdrop-filter: blur(8px);
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.4);
            margin-bottom: 30px;
        }
        
        .card-title {
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        /* Policy Selector Button Group */
        .btn-group {
            display: inline-flex;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 8px;
            padding: 4px;
            border: 1px solid var(--border-color);
        }
        
        .btn-group button {
            background: transparent;
            border: none;
            color: var(--text-muted);
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-family: inherit;
            font-size: 14px;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        
        .btn-group button.active {
            background: var(--primary);
            color: white;
            box-shadow: 0 0 10px var(--primary-glow);
        }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 16px;
            text-align: center;
        }
        
        .stat-value {
            font-size: 28px;
            font-weight: 800;
            margin-top: 8px;
            color: #ffffff;
        }
        
        .stat-label {
            font-size: 12px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        /* Workers list styling */
        .workers-container {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 20px;
        }
        
        .worker-item {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 16px;
            position: relative;
            overflow: hidden;
        }
        
        .worker-item.ALIVE {
            border-left: 4px solid var(--success);
        }
        
        .worker-item.FAILED {
            border-left: 4px solid var(--danger);
            opacity: 0.6;
        }
        
        .worker-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }
        
        .worker-id {
            font-weight: 600;
            font-size: 16px;
        }
        
        .worker-status-badge {
            font-size: 11px;
            font-weight: 600;
            padding: 4px 8px;
            border-radius: 12px;
            text-transform: uppercase;
        }
        
        .worker-status-badge.ALIVE {
            background: rgba(16, 185, 129, 0.1);
            color: var(--success);
        }
        
        .worker-status-badge.FAILED {
            background: rgba(239, 68, 68, 0.1);
            color: var(--danger);
        }
        
        .worker-metric {
            font-size: 14px;
            color: var(--text-muted);
            margin-bottom: 8px;
            display: flex;
            justify-content: space-between;
        }
        
        .worker-metric span {
            color: white;
            font-weight: 600;
        }
        
        .progress-bar-container {
            background: rgba(255, 255, 255, 0.05);
            height: 8px;
            border-radius: 4px;
            overflow: hidden;
            margin-top: 10px;
        }
        
        .progress-bar {
            background: linear-gradient(to right, var(--primary), var(--success));
            height: 100%;
            border-radius: 4px;
            transition: width 0.3s ease;
        }
        
        .kill-btn {
            background: rgba(239, 68, 68, 0.15);
            color: var(--danger);
            border: 1px solid rgba(239, 68, 68, 0.3);
            border-radius: 6px;
            padding: 4px 10px;
            font-size: 12px;
            font-weight: 600;
            cursor: pointer;
            margin-top: 12px;
            width: 100%;
            transition: all 0.3s ease;
        }
        
        .kill-btn:hover {
            background: var(--danger);
            color: white;
        }
        
        /* Tasks Table */
        .table-container {
            overflow-x: auto;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            font-size: 14px;
        }
        
        th {
            padding: 12px 16px;
            border-bottom: 2px solid var(--border-color);
            color: var(--text-muted);
            font-weight: 600;
        }
        
        td {
            padding: 12px 16px;
            border-bottom: 1px solid var(--border-color);
        }
        
        .badge {
            font-size: 11px;
            font-weight: 600;
            padding: 4px 8px;
            border-radius: 6px;
            text-transform: uppercase;
            display: inline-block;
        }
        
        .badge.READY { background: rgba(245, 158, 11, 0.1); color: var(--warning); }
        .badge.RUNNING { background: rgba(79, 70, 229, 0.1); color: #818cf8; }
        .badge.COMPLETED { background: rgba(16, 185, 129, 0.1); color: var(--success); }
        .badge.FAILED { background: rgba(239, 68, 68, 0.1); color: var(--danger); }
        
        /* Submit Form inside Sidebar */
        .form-group {
            margin-bottom: 16px;
        }
        
        label {
            display: block;
            font-size: 12px;
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 6px;
            text-transform: uppercase;
        }
        
        select, input, textarea {
            width: 100%;
            background: rgba(0, 0, 0, 0.2);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 10px 14px;
            color: white;
            font-family: inherit;
            font-size: 14px;
            outline: none;
            transition: border-color 0.3s;
        }
        
        select:focus, input:focus, textarea:focus {
            border-color: var(--primary);
        }
        
        .submit-btn {
            background: linear-gradient(135deg, var(--primary), #a855f7);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 12px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            width: 100%;
            transition: opacity 0.2s;
            box-shadow: 0 4px 15px var(--primary-glow);
        }
        
        .submit-btn:hover {
            opacity: 0.9;
        }

        .mono {
            font-family: 'JetBrains Mono', monospace;
            font-size: 13px;
        }
    </style>
</head>
<body>
    <header>
        <div class="logo-container">
            <div class="logo-icon"></div>
            <h1>Distributed Task Scheduler</h1>
        </div>
        <div class="btn-group" id="policyGroup">
            <button onclick="setPolicy('FIFO')" id="btn-FIFO">FIFO</button>
            <button onclick="setPolicy('ROUND_ROBIN')" id="btn-ROUND_ROBIN">Round Robin</button>
            <button onclick="setPolicy('LEAST_LOADED')" id="btn-LEAST_LOADED">Least Loaded</button>
        </div>
    </header>
    
    <div class="container">
        <!-- Main Area -->
        <main>
            <!-- Stats -->
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Total Tasks</div>
                    <div class="stat-value" id="stat-total">0</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Completed</div>
                    <div class="stat-value" id="stat-completed" style="color: var(--success);">0</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Avg Exec Time</div>
                    <div class="stat-value" id="stat-avg-time">0.00s</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Active Workers</div>
                    <div class="stat-value" id="stat-workers" style="color: #818cf8;">0</div>
                </div>
            </div>
            
            <!-- Workers -->
            <div class="card">
                <div class="card-title">Workers Registry</div>
                <div class="workers-container" id="workers-list">
                    <!-- Dynamic -->
                </div>
            </div>
            
            <!-- Tasks -->
            <div class="card">
                <div class="card-title">Recent Tasks (Last 100)</div>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Operation</th>
                                <th>Input</th>
                                <th>Status</th>
                                <th>Worker</th>
                                <th>Execution Time</th>
                                <th>Result / Output</th>
                            </tr>
                        </thead>
                        <tbody id="tasks-table-body">
                            <!-- Dynamic -->
                        </tbody>
                    </table>
                </div>
            </div>
        </main>
        
        <!-- Sidebar -->
        <aside>
            <div class="card">
                <div class="card-title">Submit New Task</div>
                <div class="form-group">
                    <label>Task Type</label>
                    <select id="task-type" onchange="updateInputPlaceholder()">
                        <option value="prime_count">Prime Counting (Count primes <= N)</option>
                        <option value="matrix_mult">Matrix Multiplication (NxN Matrix Multiplication)</option>
                        <option value="monte_carlo_pi">Monte Carlo Pi Estimation (N Samples)</option>
                        <option value="word_count">Word Count (Text string count)</option>
                    </select>
                </div>
                
                <div class="form-group">
                    <label>Input Value</label>
                    <input type="text" id="task-input" value="50000" placeholder="e.g. 50000">
                </div>
                
                <button class="submit-btn" onclick="submitTask()">Assign Task</button>
            </div>

            <div class="card">
                <div class="card-title">Experiment Controls</div>
                <p style="font-size: 14px; color: var(--text-muted); margin-bottom: 16px; line-height: 1.5;">
                    Trigger system-wide automated experiments and review outcomes directly in stdout reports.
                </p>
                <div style="display: flex; flex-direction: column; gap: 10px;">
                    <button class="submit-btn" style="background: #1e293b; box-shadow: none;" onclick="quickBatch(10)">Submit Batch of 10 Tasks</button>
                    <button class="submit-btn" style="background: #1e293b; box-shadow: none;" onclick="quickBatch(100)">Submit Batch of 100 Tasks</button>
                </div>
            </div>
        </aside>
    </div>
    
    <script>
        function updateInputPlaceholder() {
            const type = document.getElementById("task-type").value;
            const input = document.getElementById("task-input");
            if (type === "prime_count") {
                input.value = "50000";
                input.placeholder = "Number limit N (e.g. 50000)";
            } else if (type === "matrix_mult") {
                input.value = "100";
                input.placeholder = "Matrix size N (e.g. 100)";
            } else if (type === "monte_carlo_pi") {
                input.value = "1000000";
                input.placeholder = "Samples N (e.g. 1000000)";
            } else if (type === "word_count") {
                input.value = "Distributed operating systems require coordinate scheduling of multiple resources to execute jobs effectively.";
                input.placeholder = "Text to count";
            }
        }

        async function fetchStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                
                // Update policy active state
                document.querySelectorAll('#policyGroup button').forEach(b => b.classList.remove('active'));
                const activeBtn = document.getElementById(`btn-${data.policy}`);
                if (activeBtn) activeBtn.classList.add('active');
                
                // Update stats
                document.getElementById('stat-total').textContent = data.total_tasks;
                document.getElementById('stat-completed').textContent = data.completed_tasks;
                document.getElementById('stat-avg-time').textContent = data.avg_execution_time.toFixed(2) + 's';
                
                let activeWorkers = 0;
                for (let k in data.workers) {
                    if (data.workers[k].status === 'ALIVE') activeWorkers++;
                }
                document.getElementById('stat-workers').textContent = activeWorkers;
                
                // Update Workers List
                const workersList = document.getElementById('workers-list');
                workersList.innerHTML = '';
                
                for (let id in data.workers) {
                    const w = data.workers[id];
                    const loadPercent = Math.min(100, (w.current_load / w.cpu_cores) * 100);
                    const workerCard = document.createElement('div');
                    workerCard.className = `worker-item ${w.status}`;
                    workerCard.innerHTML = `
                        <div class="worker-header">
                            <span class="worker-id">Worker #${w.worker_id}</span>
                            <span class="worker-status-badge ${w.status}">${w.status}</span>
                        </div>
                        <div class="worker-metric">Address: <span>${w.addr[0]}:${w.addr[1]}</span></div>
                        <div class="worker-metric">CPU Cores: <span>${w.cpu_cores}</span></div>
                        <div class="worker-metric">Load: <span>${w.current_load} / ${w.cpu_cores} (${loadPercent.toFixed(0)}%)</span></div>
                        <div class="progress-bar-container">
                            <div class="progress-bar" style="width: ${loadPercent}%"></div>
                        </div>
                        ${w.status === 'ALIVE' ? `<button class="kill-btn" onclick="killWorker(${w.worker_id})">Kill Worker</button>` : ''}
                    `;
                    workersList.appendChild(workerCard);
                }
                
                // Update Tasks Table
                const tbody = document.getElementById('tasks-table-body');
                tbody.innerHTML = '';
                
                data.tasks.forEach(t => {
                    let execTime = '-';
                    if (t.started_at && t.completed_at) {
                        execTime = (t.completed_at - t.started_at).toFixed(2) + 's';
                    } else if (t.started_at) {
                        execTime = ((Date.now() / 1000) - t.started_at).toFixed(1) + 's (running)';
                    }
                    
                    let outVal = '-';
                    if (t.output !== null) {
                        outVal = typeof t.output === 'object' ? JSON.stringify(t.output) : t.output;
                    }
                    
                    const tr = document.createElement('tr');
                    tr.innerHTML = `
                        <td class="mono">#${t.task_id}</td>
                        <td><span class="mono">${t.operation}</span></td>
                        <td style="max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"><span class="mono">${t.input}</span></td>
                        <td><span class="badge ${t.status}">${t.status}</span></td>
                        <td>${t.assigned_worker !== null ? `Worker #${t.assigned_worker}` : '-'}</td>
                        <td>${execTime}</td>
                        <td style="max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${outVal}"><span class="mono">${outVal}</span></td>
                    `;
                    tbody.appendChild(tr);
                });
            } catch (err) {
                console.error("Error fetching status:", err);
            }
        }
        
        async function setPolicy(policy) {
            await fetch('/api/policy', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ policy })
            });
            fetchStatus();
        }
        
        async function submitTask() {
            const operation = document.getElementById("task-type").value;
            let val = document.getElementById("task-input").value;
            // Parse inputs if they look like numbers
            let input = val;
            if (!isNaN(val) && val.trim() !== '') {
                input = parseInt(val, 10);
            }
            
            await fetch('/api/submit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    tasks: [{ operation, input }]
                })
            });
            fetchStatus();
        }

        async function quickBatch(count) {
            const tasks = [];
            const ops = ["prime_count", "matrix_mult", "monte_carlo_pi", "word_count"];
            const txt = "Distributed operating systems require coordinate scheduling of multiple resources to execute jobs effectively.";
            for (let i = 0; i < count; i++) {
                const op = ops[i % ops.length];
                let input = 10000;
                if (op === "matrix_mult") input = 80;
                if (op === "monte_carlo_pi") input = 100000;
                if (op === "word_count") input = txt;
                tasks.push({ operation: op, input });
            }
            await fetch('/api/submit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tasks })
            });
            fetchStatus();
        }
        
        async function killWorker(workerId) {
            if (!confirm(`Are you sure you want to simulate a dirty kill on Worker #${workerId}?`)) return;
            await fetch('/api/kill_worker', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ worker_id: workerId })
            });
            fetchStatus();
        }
        
        // Auto refresh
        setInterval(fetchStatus, 1000);
        window.onload = () => {
            updateInputPlaceholder();
            fetchStatus();
        };
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    import sys
    tcp_port = 5000
    http_port = 8000
    if len(sys.argv) > 1:
        tcp_port = int(sys.argv[1])
    if len(sys.argv) > 2:
        http_port = int(sys.argv[2])

    print("[Master] Starting Distributed Task Scheduler Master...")
    
    # Threads
    t_tcp = threading.Thread(target=run_worker_tcp_server, args=(tcp_port,), daemon=True)
    t_http = threading.Thread(target=run_http_server, args=(http_port,), daemon=True)
    t_sched = threading.Thread(target=scheduler_loop, daemon=True)
    t_hb = threading.Thread(target=monitor_heartbeats, daemon=True)
    
    t_tcp.start()
    t_http.start()
    t_sched.start()
    t_hb.start()
    
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("[Master] Shutting down...")
        running = False
