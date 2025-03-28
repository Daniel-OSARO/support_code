import paramiko
import time
import re
import requests
import os
import json
import subprocess
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from tkinter import Tk, Text, END

# 서버 정보
servers = [
    {"id": 1, "host": "192.168.111.11", "username": "admin", "password": os.getenv("SSH_PASSWORD")},
    {"id": 2, "host": "192.168.111.12", "username": "admin", "password": os.getenv("SSH_PASSWORD")},
    {"id": 3, "host": "192.168.111.13", "username": "admin", "password": os.getenv("SSH_PASSWORD")},
    {"id": 4, "host": "192.168.111.14", "username": "admin", "password": os.getenv("SSH_PASSWORD")},
    {"id": 5, "host": "192.168.111.15", "username": "admin", "password": os.getenv("SSH_PASSWORD")},
    {"id": 6, "host": "192.168.111.16", "username": "admin", "password": os.getenv("SSH_PASSWORD")},
    {"id": 7, "host": "192.168.111.17", "username": "admin", "password": os.getenv("SSH_PASSWORD")},
]

# SSH 연결 및 명령 실행 함수
def execute_ssh_command(host, username, password, command):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username=username, password=password, timeout=10)

        stdin, stdout, stderr = client.exec_command(command)
        output = stdout.read().decode().strip()
        client.close()
        return output
    except Exception as e:
        return f"Error connecting to {host}: {str(e)}"

# GUI 창에 결과 표시
def display_results_in_window(results):
    window = Tk()
    window.title("Command Results")
    text_box = Text(window, wrap='word', height=30, width=100)
    text_box.insert(END, results)
    text_box.pack(expand=True, fill='both')
    window.mainloop()

# 메뉴 표시 및 선택
def show_menu():
    print("\n=== Menu ===")
    print("1. Version Check")
    print("2. Self Test")
    print("3. Salt-update")
    print("0. Exit")
    choice = input("Select an option: ")
    return choice

# 테스트할 서버 선택
def select_tests():
    print("\nAvailable Servers:")
    for server in servers:
        print(f"Cell{server['id']}")
    selected = input("Enter cell numbers to test (e.g., 1457): ")
    return [int(x) for x in selected if x.isdigit()]

# 병렬로 명령 실행 및 실시간 결과 처리
def run_tests_parallel(server_ids, command):
    results = {}

    def handle_result(future, server_id):
        try:
            output = future.result()
            results[server_id] = output
            # 실시간으로 결과 출력
            print(f"[Server {server_id}] Output: {output}")
        except Exception as e:
            results[server_id] = f"Error: {str(e)}"
            print(f"[Server {server_id}] Error: {str(e)}")

    with ThreadPoolExecutor(max_workers=len(server_ids)) as executor:
        futures = {
            server['id']: executor.submit(
                execute_ssh_command, server['host'], server['username'], server['password'], command
            )
            for server in servers if server['id'] in server_ids
        }
        for server_id, future in futures.items():
            future.add_done_callback(lambda f, s=server_id: handle_result(f, s))

    # 모든 결과 반환
    return results

# 결과 처리
def process_results(results):
    ready_cells = []
    not_ready_cells = []

    for server_id, output in results.items():
        print(f"\n[Server {server_id}]")
        print(f"Output: {output}")
        # if "pnp" in output:
        if "first_command" in output:
            print("Result: Good to start")
            ready_cells.append(server_id)
        else:
            print("Result: Check again")
            not_ready_cells.append(server_id)

    return ready_cells, not_ready_cells

# HTTP 응답 처리
def response_request(command):
    if command.status_code == 200:
        return summarize_results(command.json())
    else:
        print(f"Retrying request after delay...")
        time.sleep(30)
        return response_request(requests.get(command.url))

# 결과 요약
def summarize_results(data):
    summary = "===== Summarized Results =====\n"
    summary += "C1ST1125\n"

    # Suction Summary
    suction_data = data.get("suctionCheck", [])
    summary += f"- Suction {len(suction_data)}/{len(suction_data)}\n"
    for suction in suction_data:
        end_effector = suction["endEffector"]
        unsealed = suction["unsealedKpa"]
        sealed = suction["sealedKpa"]
        summary += f"  * {end_effector.split('_')[-1]} : {unsealed:.2f} / {sealed:.2f}\n"

    # Camera Validation Summary
    calibration_data = data.get("calibrationCheck", [])
    summary += f"- Camera validation {len(calibration_data)}/{len(calibration_data)}\n"

    # Force Compression Summary
    force_data = data.get("forceCompressionCheck", [])
    summary += f"- Force compression {len(force_data)}/{len(force_data)}\n"

    # Robot Check Summary
    robot_check = data.get("robotCheck", {})
    robot_status = "OK" if robot_check.get("status") == "SUCCESS" else "FAIL"
    summary += f"- Robot check {robot_status}\n"

    # Brightness Check Summary
    brightness_data = data.get("brightnessCheck", [])
    brightness_fail = [
        brightness["cameraId"] for brightness in brightness_data
        if brightness["status"] != "SUCCESS"
    ]
    summary += f"- Brightness check {len(brightness_data) - len(brightness_fail)}/{len(brightness_data)}"
    if brightness_fail:
        summary += f" ({', '.join(brightness_fail)})\n"
    else:
        summary += "\n"

    return summary

import json
import subprocess
from datetime import datetime, timedelta, timezone
import time

def fetch_logs(host_name, start_time=None, end_time=None):
    """
    Fetch logs from Google Cloud within the specified time range.
    
    Args:
        host_name (str): The hostname to filter logs.
        start_time (datetime, optional): Start time for the log query.
        end_time (datetime, optional): End time for the log query.
        
    Returns:
        list: List of log entries parsed from the JSON response.
    """
    if not start_time or not end_time:
        # Default to logs within the last minute
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=1)

    # Format the times for the query
    start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    # Adjusted query with additional filters
    query = f"""
    jsonPayload.hostname="{host_name}" 
    jsonPayload.app_name: "pnp" 
    severity>="INFO"
    "SelfTest"
    -"pnp_config"
    -"MetadataMap"
    """

    filter_expr = f"""
    timestamp >= "{start_time_str}" AND timestamp <= "{end_time_str}"
    AND {query}
    """

    # Run the gcloud command to fetch logs
    result = subprocess.run(
        ["/usr/local/bin/gcloud", "logging", "read", filter_expr, "--project", "osaro-logging", "--format", "json"],
        stdout=subprocess.PIPE,
        text=True
    )

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []

def google_log_monitor(host_name, ready_cells, max_duration=360, poll_interval=10):
    """
    Monitor Google Cloud logs for the specified cells and actions.

    Args:
        host_name (str): The hostname for filtering logs.
        ready_cells (list): List of ready cells to monitor.
        max_duration (int): Maximum duration to monitor in seconds.
        poll_interval (int): Interval between log fetches in seconds.

    Returns:
        dict: Mapping of `action_id` to their corresponding cell and status.
    """
    print("\nStarting Google log monitoring...")
    print(f"Monitoring for cells: {ready_cells}")
    print(f"Duration: {max_duration}s, Interval: {poll_interval}s")

    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(seconds=max_duration)
    action_status = {cell: {"action_id": None, "status": "pending"} for cell in ready_cells}

    while datetime.now(timezone.utc) < end_time:
        logs = fetch_logs(host_name)
        for entry in logs:
            fields = entry.get("jsonPayload", {})
            message = fields.get("message", "")
            cell_number = fields.get("cell_number", "unknown")
            action_id = fields.get("action_id")

            if cell_number in ready_cells:
                if "Marking action executing" in message and action_id:
                    if action_status[cell_number]["action_id"] is None:
                        action_status[cell_number]["action_id"] = action_id
                        action_status[cell_number]["status"] = "executing"
                        print(f"[Cell {cell_number}] Action {action_id} started.")

                elif "Marking action completed" in message and action_id:
                    if action_status[cell_number]["action_id"] == action_id:
                        action_status[cell_number]["status"] = "completed"
                        print(f"[Cell {cell_number}] Action {action_id} completed.")

        # Check if all cells have completed or failed
        if all(data["status"] != "pending" for data in action_status.values()):
            break

        time.sleep(poll_interval)

    # Mark any remaining actions as failed
    for cell, data in action_status.items():
        if data["status"] == "executing":
            data["status"] = "failed"
            print(f"[Cell {cell}] Action {data['action_id']} failed due to timeout.")

    print("\nGoogle log monitoring complete.")
    return action_status



# 메인 함수
def main():
    while True:
        choice = show_menu()

        if choice == "0":
            print("Exiting program...")
            break
        elif choice == "1":
            print("\nVersion Check is not yet implemented.")
        elif choice == "2":
            selected_servers = select_tests()
            # first_command = 'docker restart pnp'
            first_command = 'echo "first_command"'
            print("\nRunning first command: pnp restart")
            first_results = run_tests_parallel(selected_servers, first_command)
            print(f"first results : {first_results}") # debug
            time.sleep(3)
            ready_cells, not_ready_cells = process_results(first_results)
            
            # Proceed to Next Mission
            print(f"\nOnly Cell {', '.join(map(str, ready_cells))} is ready to test.")
            next_mission = input("Proceed to the next mission? (y/n): ").strip().lower()
            if ready_cells:
                if next_mission == "y":
                    # second_command = "curl -i -X POST 'http://localhost:51061/v1/pnp/self-test'"
                    second_command = 'echo "second command"'
                    second_results = run_tests_parallel(ready_cells, second_command)
                    print("\nRunning second command on ready cells...")
                    # print(f"second_results : {second_results}")

                    # Done 1. Check if the return value of run_tests_parallel come or not

                    # 2. Google log - It needs to be work separately and modulized
                    # 2-0. Searching only within ready_cells. Need to modify and filter dynamically based on the ready_cells
                    # 2-1. While printing the message in fiedls in jsonPayload in real-time or search for every 10s?,
                        # 2-1-1. If the message is "Marking action executing", extract self.action_id in fiedls in jsonPayload once
                        # 2-1-2. If the message is "Marking action completed" showed, check the response cell number and store the cell number with corresponding self.action_id'
                            # 2-1-2-1. If "Marking action completed" not showing during 6m, treat it as fail
                    # 2-2. If the all ready_cells get "Marking action completed" or fail, finish Step 2.

                    # 3. In MBP, print to current termianl "do curl with cell{ready_cell} and {self.action_id}"
                        # 3-1. The ready_cell and self.action_id should be corresponded, so the number of printing command should be same as the number of ready_cell and self.action_id

                    # 4. Do summary and make CxST{today}.txt file and combined summary file


                    # decode_second_results = str(second_results)
                    # # Extract multiple action IDs and process each
                    # matches = re.findall(r'"actionId":\s*"([a-f0-9\-]+)"', decode_second_results)
                    # if matches:
                    #     for action_id in matches:
                    #         url = f"curl -i -X GET 'http://localhost:51061/v1/pnp/self-test/{action_id}'"
                    #         print(f"Action ID found: {action_id}, Processing URL: {url}")
                    #         third_results = run_tests_parallel(ready_cells, url)
                    #         print("Waiting for self-testing to complete...")
                    #         ########################
                    #         # Better to check with log
                    #         time.sleep(300)
                    #         ########################
                    #         final = response_request(third_results)
                    #         ########################
                    #         # Display & Save results 
                    #         display_results_in_window(final)
                    #         ########################
                    # else:
                    #     print("No actionID found in response.")
        else:
            print("Invalid choice. Please select again.")

if __name__ == "__main__":
    main()