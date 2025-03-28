'''from datetime import datetime, timedelta, timezone
import json
import sys, subprocess
from google.cloud import logging


def get_logs(HOST_NAME):
    # Get current time and time 1 minute ago
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=1)
    
    # Format the times to the correct format for the query
    START_TIME = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    END_TIME = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    QUERY = """jsonPayload.app_name: "pnp" 
                severity>="INFO"
                "SelfTest"
            """

    FILTER = f"""
    jsonPayload.hostname = "{HOST_NAME}" AND timestamp >= "{START_TIME}" AND timestamp <= "{END_TIME}"
    AND {QUERY}
    """
    
    # Run the gcloud command to fetch logs
    result = subprocess.run(["/usr/local/bin/gcloud", "logging", "read", FILTER, "--project", "osaro-logging", "--format", "json"], stdout=subprocess.PIPE, text=True)
    logs = json.loads(result.stdout)

    # Sort the logs by timestamp
    sorted_logs = sorted(logs, key=lambda x: x['timestamp'])
    return sorted_logs

HOST_NAME = "cpg-ech02-7"
all_logs = get_logs(HOST_NAME)
if all_logs:
    print(all_logs)
else:
    print("nothing")'''


from datetime import datetime, timedelta, timezone
import json
import time
import subprocess

def fetch_logs(HOST_NAME):
    """Fetch logs within the last minute."""
    # Get current time and time 1 minute ago
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=1)
    
    # Format the times to the correct format for the query
    START_TIME = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    END_TIME = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    QUERY = """jsonPayload.app_name: "pnp" 
                severity>="INFO"
                "SelfTest"
            """

    FILTER = f"""
    jsonPayload.hostname = "{HOST_NAME}" AND timestamp >= "{START_TIME}" AND timestamp <= "{END_TIME}"
    AND {QUERY}
    """
    
    # Run the gcloud command to fetch logs
    result = subprocess.run(
        ["/usr/local/bin/gcloud", "logging", "read", FILTER, "--project", "osaro-logging", "--format", "json"],
        stdout=subprocess.PIPE,
        text=True
    )
    
    # Parse logs from JSON output
    try:
        logs = json.loads(result.stdout)
    except json.JSONDecodeError:
        logs = []
    
    return logs

def extract_action_id(logs):
    """Extract self.action_id from logs with 'Marking action completed'."""
    action_ids = []
    for log in logs:
        if log.get("jsonPayload", {}).get("fields", {}).get("message") == "Marking action completed":
            action_id = log.get("jsonPayload", {}).get("fields", {}).get("self.action_id")
    return action_id

def monitor_logs(HOST_NAME):
    """Keep searching logs every 30 seconds until 'self.action_id' is found."""
    while True:
        print(f"Checking logs at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}...")
        logs = fetch_logs(HOST_NAME)
        action_ids = extract_action_id(logs)
        
        if action_ids:
            print("Found self.action_id(s):", action_ids)
            break
        else:
            print("No relevant logs found. Retrying...")
            time.sleep(30)

# Host name to monitor
HOST_NAME = "cpg-ech02-7"
monitor_logs(HOST_NAME)
