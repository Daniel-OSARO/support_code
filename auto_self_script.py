from datetime import datetime, timedelta, timezone
import json
import sys, subprocess
from google.cloud import logging

'''def get_logs(START_TIME, END_TIME, HOST_NAME):

    QUERY = """jsonPayload.app_name = "osaro-pnp"
                "sent print payload to label printer" OR "issuing Coupang WMS UpdatePrintOrderStatus request"
            """

    FILTER = f"""
    jsonPayload.hostname = "{HOST_NAME}" AND timestamp >= "{START_TIME}" AND timestamp <= "{END_TIME}"
    AND {QUERY}
    """
    
    # result = subprocess.run(["/Users/zijianjiang/Desktop/google-cloud-sdk/bin/gcloud", "logging", "read", FILTER, "--project", "osaro-logging", "--format", "json"], stdout=subprocess.PIPE, text=True)
    result = subprocess.run(["/usr/local/bin/gcloud", "logging", "read", FILTER, "--project", "osaro-logging", "--format", "json"], stdout=subprocess.PIPE, text=True)
    logs = json.loads(result.stdout)
    sorted_logs = sorted(logs, key=lambda x: x['timestamp'])
    return(sorted_logs)

START_TIME = "2024-10-30T14:00:00"
END_TIME = "2024-10-30T19:30:00"
HOST_NAME = "cpg-ech02-7"
all_logs = get_logs(START_TIME=START_TIME, END_TIME=END_TIME, HOST_NAME=HOST_NAME)'''


def get_logs(HOST_NAME):
    # Get current time and time 1 minute ago
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=1)
    
    # Format the times to the correct format for the query
    START_TIME = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    END_TIME = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    QUERY = """jsonPayload.app_name: "pnp" 
                severity>="INFO"
                "SelfTestStatusResponse"
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
    print("nothing")