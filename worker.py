import socket
import threading
import time
import sys
import argparse
import random
from common import send_msg, recv_msg

# Computation Tasks
def prime_count(n):
    if n < 2:
        return 0
    sieve = [True] * (n + 1)
    sieve[0] = sieve[1] = False
    for i in range(2, int(n**0.5) + 1):
        if sieve[i]:
            for j in range(i*i, n + 1, i):
                sieve[j] = False
    return sum(sieve)

def matrix_mult(n):
    # n x n matrix multiplication
    A = [[random.random() for _ in range(n)] for _ in range(n)]
    B = [[random.random() for _ in range(n)] for _ in range(n)]
    C = [[0.0 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        for j in range(n):
            s = 0.0
            for k in range(n):
                s += A[i][k] * B[k][j]
            C[i][j] = s
    return sum(C[0])  # return sum of first row to keep data small

def monte_carlo_pi(samples):
    count = 0
    for _ in range(samples):
        x = random.random()
        y = random.random()
        if x*x + y*y <= 1.0:
            count += 1
    return (4.0 * count) / samples

def word_count(text):
    words = text.split()
    freq = {}
    for w in words:
        w_clean = "".join(c for c in w if c.isalnum()).lower()
        if w_clean:
            freq[w_clean] = freq.get(w_clean, 0) + 1
    sorted_freq = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return sorted_freq[:20]

def execute_task(task_id, op, val, sock, worker_id):
    """Runs a task and sends results back to the master."""
    print(f"[Worker {worker_id}] Starting task {task_id}: {op}({val})")
    start_time = time.time()
    
    try:
        if op == "prime_count":
            result = prime_count(int(val))
        elif op == "matrix_mult":
            result = matrix_mult(int(val))
        elif op == "monte_carlo_pi":
            result = monte_carlo_pi(int(val))
        elif op == "word_count":
            result = word_count(str(val))
        else:
            raise ValueError(f"Unknown operation: {op}")
        
        elapsed = time.time() - start_time
        print(f"[Worker {worker_id}] Task {task_id} completed in {elapsed:.3f}s")
        
        # Send result back
        result_msg = {
            "type": "RESULT",
            "worker_id": worker_id,
            "task_id": task_id,
            "status": "COMPLETED",
            "output": result,
            "execution_time": elapsed
        }
        send_msg(sock, result_msg)
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[Worker {worker_id}] Task {task_id} failed: {e}")
        error_msg = {
            "type": "RESULT",
            "worker_id": worker_id,
            "task_id": task_id,
            "status": "FAILED",
            "output": str(e),
            "execution_time": elapsed
        }
        send_msg(sock, error_msg)

def heartbeat_loop(sock, worker_id, stop_event):
    """Sends periodic heartbeats to the master."""
    while not stop_event.is_set():
        heartbeat = {
            "type": "HEARTBEAT",
            "worker_id": worker_id
        }
        if not send_msg(sock, heartbeat):
            print(f"[Worker {worker_id}] Failed to send heartbeat. Master connection might be lost.")
            break
        time.sleep(2.0)

def main():
    parser = argparse.ArgumentParser(description="Distributed Task Scheduler Worker Node")
    parser.add_argument("--id", type=int, required=True, help="Unique worker ID")
    parser.add_argument("--cores", type=int, default=2, help="Number of CPU cores/capacity")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Master TCP server host")
    parser.add_argument("--port", type=int, default=5000, help="Master TCP server port")
    args = parser.parse_args()

    worker_id = args.id
    cpu_cores = args.cores
    master_host = args.host
    master_port = args.port

    print(f"[Worker {worker_id}] Connecting to Master at {master_host}:{master_port}...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((master_host, master_port))
    except Exception as e:
        print(f"[Worker {worker_id}] Connection failed: {e}")
        sys.exit(1)

    # Register worker
    reg_msg = {
        "type": "REGISTER",
        "worker_id": worker_id,
        "cpu_cores": cpu_cores
    }
    if not send_msg(sock, reg_msg):
        print(f"[Worker {worker_id}] Failed to send registration message.")
        sock.close()
        sys.exit(1)

    ack = recv_msg(sock)
    if not ack or ack.get("type") != "REGISTER_ACK" or ack.get("status") != "SUCCESS":
        print(f"[Worker {worker_id}] Registration rejected by Master.")
        sock.close()
        sys.exit(1)

    print(f"[Worker {worker_id}] Successfully registered with Master.")

    # Start heartbeat thread
    stop_event = threading.Event()
    t_hb = threading.Thread(target=heartbeat_loop, args=(sock, worker_id, stop_event), daemon=True)
    t_hb.start()

    # Receive and execute tasks
    try:
        while True:
            msg = recv_msg(sock)
            if msg is None:
                print(f"[Worker {worker_id}] Disconnected from Master.")
                break

            msg_type = msg.get("type")
            if msg_type == "TASK":
                task_id = msg["task_id"]
                op = msg["operation"]
                val = msg["input"]
                
                # Run task in a separate thread for concurrency
                t = threading.Thread(
                    target=execute_task, 
                    args=(task_id, op, val, sock, worker_id), 
                    daemon=True
                )
                t.start()
            elif msg_type == "KILL":
                print(f"[Worker {worker_id}] Received kill command from Master. Crashing now!")
                break
    except KeyboardInterrupt:
        print(f"[Worker {worker_id}] Shutting down...")
    finally:
        stop_event.set()
        sock.close()
        print(f"[Worker {worker_id}] Stopped.")

if __name__ == "__main__":
    main()
