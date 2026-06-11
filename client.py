import urllib.request
import json
import argparse
import sys

def post_json(url, payload):
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            url, 
            data=data, 
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req) as res:
            return json.loads(res.read().decode('utf-8'))
    except Exception as e:
        print(f"Error making POST request to {url}: {e}")
        return None

def get_json(url):
    try:
        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req) as res:
            return json.loads(res.read().decode('utf-8'))
    except Exception as e:
        print(f"Error making GET request to {url}: {e}")
        return None

def display_status(status):
    if not status:
        return
    print("\n================ SYSTEM STATUS ================")
    print(f"Scheduling Policy: {status['policy']}")
    print(f"Total Tasks:       {status['total_tasks']}")
    print(f"Completed Tasks:   {status['completed_tasks']}")
    print(f"Avg Exec Time:     {status['avg_execution_time']:.2f}s")
    
    print("\n--- Worker Registry ---")
    workers = status['workers']
    if not workers:
        print("No workers registered.")
    for wid, w in workers.items():
        print(f"Worker #{w['worker_id']}: status={w['status']}, cores={w['cpu_cores']}, load={w['current_load']}/{w['cpu_cores']}, addr={w['addr'][0]}:{w['addr'][1]}")
        
    print("\n--- Recent Tasks (up to 10) ---")
    tasks = status['tasks']
    if not tasks:
        print("No tasks in the system.")
    for t in tasks[:10]:
        exec_time = "-"
        if t['started_at'] and t['completed_at']:
            exec_time = f"{t['completed_at'] - t['started_at']:.2f}s"
        elif t['started_at']:
            exec_time = "running"
        
        output_str = str(t['output'])
        if len(output_str) > 30:
            output_str = output_str[:27] + "..."
            
        print(f"Task #{t['task_id']}: operation={t['operation']}, input={t['input']}, status={t['status']}, worker={t['assigned_worker']}, time={exec_time}, output={output_str}")
    print("===============================================\n")

def main():
    parser = argparse.ArgumentParser(description="Distributed Scheduler Client CLI")
    parser.add_argument("--url", type=str, default="http://127.0.0.1:8000", help="Master HTTP Server URL")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # status command
    subparsers.add_parser("status", help="Get system status")
    
    # policy command
    policy_parser = subparsers.add_parser("policy", help="Change scheduler policy")
    policy_parser.add_argument("policy", choices=["FIFO", "ROUND_ROBIN", "LEAST_LOADED"], help="Scheduling policy")
    
    # submit command
    submit_parser = subparsers.add_parser("submit", help="Submit a task")
    submit_parser.add_argument("--type", choices=["prime_count", "matrix_mult", "monte_carlo_pi", "word_count"], required=True, help="Task type")
    submit_parser.add_argument("--input", required=True, help="Input data (integer for math tasks, string for word count)")
    
    # submit-batch command
    batch_parser = subparsers.add_parser("submit-batch", help="Submit a batch of mixed tasks")
    batch_parser.add_argument("--count", type=int, default=10, help="Number of tasks in batch")
    
    # kill-worker command
    kill_parser = subparsers.add_parser("kill-worker", help="Simulate worker failure")
    kill_parser.add_argument("--id", type=int, required=True, help="Worker ID to terminate")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(0)
        
    base_url = args.url.rstrip('/')
    
    if args.command == "status":
        status = get_json(f"{base_url}/api/status")
        display_status(status)
        
    elif args.command == "policy":
        res = post_json(f"{base_url}/api/policy", {"policy": args.policy})
        if res:
            print(res.get("message", "Success"))
            
    elif args.command == "submit":
        # Parse numeric input if appropriate
        val = args.input
        if val.isdigit():
            val = int(val)
        payload = {
            "tasks": [{
                "operation": args.type,
                "input": val
            }]
        }
        res = post_json(f"{base_url}/api/submit", payload)
        if res:
            print(res.get("message", "Success"))
            print(f"Task ID(s): {res.get('task_ids')}")
            
    elif args.command == "submit-batch":
        ops = ["prime_count", "matrix_mult", "monte_carlo_pi", "word_count"]
        txt = "Distributed operating systems require coordinate scheduling of multiple resources to execute jobs effectively."
        tasks = []
        for i in range(args.count):
            op = ops[i % len(ops)]
            if op == "prime_count":
                input_val = 50000
            elif op == "matrix_mult":
                input_val = 80
            elif op == "monte_carlo_pi":
                input_val = 200000
            else:
                input_val = txt
                
            tasks.append({
                "operation": op,
                "input": input_val
            })
        payload = {"tasks": tasks}
        res = post_json(f"{base_url}/api/submit", payload)
        if res:
            print(res.get("message", "Success"))
            
    elif args.command == "kill-worker":
        res = post_json(f"{base_url}/api/kill_worker", {"worker_id": args.id})
        if res:
            print(res.get("message", "Success"))

if __name__ == "__main__":
    main()
