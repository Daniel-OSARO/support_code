import paramiko
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time

# ANSI escape codes for colors
COLORS = [
    "\033[91m",        # Red
    "\033[38;5;208m", # Orange (256-color)
    "\033[93m",        # Yellow
    "\033[92m",        # Green
    "\033[94m",        # Blue
    "\033[38;5;54m",  # Indigo (256-color)
    "\033[95m",        # Violet
]
RESET_COLOR = "\033[0m" 

# export SUDO_PASSWORD="muchscarydontuse"

def execute_ssh_command(host, username, password, command, sudo_password=None, server_color=RESET_COLOR):
    """Executes an SSH command and returns output, error, and exit status."""
    client = None
    original_command_for_logging = command # Store original command for logging if it's modified
    try:
        # print(f"{server_color}[DEBUG] Connecting to {host}...{RESET_COLOR}")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username=username, password=password, timeout=10)

        log_command = original_command_for_logging
        if "sudo" in command and sudo_password:
            # Ensure sudo command structure is robust
            # For logging, show the command without the echoed password
            log_command = command.replace('sudo ', '', 1) # Command part after 'sudo '
            if "echo" in original_command_for_logging and "| sudo -S -p ''" in original_command_for_logging: # Check if it was already modified
                 log_command = original_command_for_logging.split("| sudo -S -p '' ", 1)[1]

            command = f"echo '{sudo_password}' | sudo -S -p '' {command.replace('sudo ', '', 1)}"

        # print(f"{server_color}[DEBUG] Executing command on {host}: {log_command}{RESET_COLOR}")
        stdin, stdout, stderr = client.exec_command(command, get_pty=True) # get_pty can sometimes be needed for sudo

        # Reading stdout and stderr before checking exit status
        output = stdout.read().decode(errors='ignore').strip()
        error = stderr.read().decode(errors='ignore').strip()
        exit_status = stdout.channel.recv_exit_status() # Get exit status

        if error: # If there is any content in stderr, log it as an ERROR
            print(f"{server_color}[ERROR] Error from {host}: {error}{RESET_COLOR}")

        return output, error, exit_status

    except Exception as e:
        print(f"{server_color}[ERROR] SSH execution failed on {host}: {e}{RESET_COLOR}")
        return None, str(e), -1 # Return a consistent tuple format on failure
    finally:
        if client:
            client.close()


def get_target_servers(selection, servers):
    if selection == "0":
        return servers
    return [server for server in servers if str(server['id']) in selection]


def process_service(server, service, sudo_password, docker_restart_only=False):
    try:
        server_color = server.get("color", RESET_COLOR) # Get server color, default to reset
        if not docker_restart_only:  # Run Salt update first
            print(f"{server_color}[DEBUG] Processing salt update {service} on Cell{server['id']}{RESET_COLOR}")
            # Ensure the command uses sudo correctly if needed
            salt_command = f"sudo salt-update {service} koji/cpg-ech02-1-7_onsite"
            salt_output, salt_error, salt_exit_status = execute_ssh_command(
                server['host'], server['username'], server['password'], salt_command, sudo_password, server_color=server_color
            )

            # Check exit status first for success (0 means success)
            if salt_exit_status == 0 and not salt_error:
                failed_matches = re.findall(r'Failed:\s+(\d+)', salt_output)
                failed_count = sum(int(match) for match in failed_matches) if failed_matches else 0
                if failed_count == 0:
                    print(f"{server_color}✅ Salt update Success: Cell{server['id']}, Salt Update {service}{RESET_COLOR}")
                else:
                    print(f"{server_color}⚠️ Salt update Warning: Cell{server['id']}, Salt Update {service} - Exit status 0 but output shows {failed_count} failures. Output: {salt_output[:100]}...{RESET_COLOR}")
            else:
                error_details = salt_error if salt_error else salt_output
                print(f"{server_color}❌ Salt update Failed: Cell{server['id']}, Salt Update {service} (Exit: {salt_exit_status}). Error: {error_details[:150]}...{RESET_COLOR}")
                return # Stop processing this service for this server if salt update failed

        # Always restart Docker for the service (only if salt update succeeded or was skipped)
        print(f"{server_color}[DEBUG] Processing docker restart {service} on Cell{server['id']}{RESET_COLOR}")
        # Docker restart might not need sudo depending on setup, but let's assume it might
        restart_command = f"sudo docker restart {service}"
        restart_output, restart_error, restart_exit_status = execute_ssh_command(
            server['host'], server['username'], server['password'], restart_command, sudo_password, server_color=server_color
        )

        # Check exit status for docker restart
        if restart_exit_status == 0 and not restart_error:
            # Check if the output contains the service name as a confirmation
            if service in restart_output:
                print(f"{server_color}✅ Docker restart Success: Cell{server['id']}, Docker Restart {service}{RESET_COLOR}")
            else:
                print(f"{server_color}🤔 Docker restart Status Uncertain: Cell{server['id']}, Docker Restart {service}. Exit status 0, but confirmation string not found. Output: {restart_output[:100]}...{RESET_COLOR}")
        else:
            error_details = restart_error if restart_error else restart_output
            print(f"{server_color}❌ Docker restart Failed: Cell{server['id']}, Docker Restart {service} (Exit: {restart_exit_status}). Error: {error_details[:150]}...{RESET_COLOR}")

    except Exception as e:
        server_color = server.get("color", RESET_COLOR) # Ensure color is available for exception logging
        print(f"{server_color}[ERROR] Error processing {service} on {server['host']}: {e}{RESET_COLOR}")


def process_services_for_server(server, services, sudo_password, docker_restart_only):
    try:
        for service in services:
            process_service(server, service, sudo_password, docker_restart_only)
    except Exception as e:
        server_color = server.get("color", RESET_COLOR) # Ensure color is available for exception logging
        print(f"{server_color}[ERROR] Error processing services on {server['host']}: {e}{RESET_COLOR}")


def execute_pnp_reset(server):
    """Executes the PNP reset curl command on a given server."""
    server_color = server.get("color", RESET_COLOR) # Get server color
    try:
        print(f"{server_color}[DEBUG] Processing PNP reset on Cell{server['id']}{RESET_COLOR}")
        reset_command = "curl -i -X POST 'http://localhost:80/v1/pnp/reset'"
        output, error, exit_status = execute_ssh_command(server['host'], server['username'], server['password'], reset_command, server_color=server_color) # Modified to get all parts

        # Check the exit status and output for success criteria
        if exit_status == 0 and not error and output and "HTTP/1.1 200 OK" in output:
            print(f"{server_color}✅ PNP Reset Success: Cell{server['id']}{RESET_COLOR}")
        elif exit_status == 0 and not error and output:
            print(f"{server_color}⚠️ PNP Reset Warning: Cell{server['id']}, HTTP 200 OK not found. Output: {output[:100]}...{RESET_COLOR}")
        else:
            error_details = error if error else output
            print(f"{server_color}❌ PNP Reset Failed: Cell{server['id']} (Exit: {exit_status}). Error: {error_details[:150]}...{RESET_COLOR}")

    except Exception as e:
        print(f"{server_color}[ERROR] Error executing PNP reset on {server['host']}: {e}{RESET_COLOR}")


def run_pnp_reset_parallel(target_servers, max_workers=2):
    """Runs the PNP reset command in parallel on the target servers."""
    # Limit concurrency for PNP reset as well, maybe less restrictive than salt
    with ThreadPoolExecutor(max_workers=max_workers) as executor: # Example: limit PNP reset concurrency too
        futures = [
            executor.submit(execute_pnp_reset, server)
            for server in target_servers
        ]

        for future in as_completed(futures):
            try:
                future.result() # Wait for completion and handle potential exceptions
            except Exception as e:
                # Exceptions from execute_pnp_reset are caught internally,
                # but this catches potential ThreadPoolExecutor issues.
                print(f"[ERROR] A task for PNP reset failed: {e}")


def run_service_parallel(target_servers, services, sudo_password, docker_restart_only=False, max_workers=2):
    # Limit the number of concurrent workers to 3
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_services_for_server, server, services, sudo_password, docker_restart_only)
            for server in target_servers
        ]

        for future in as_completed(futures):
            future.result()  # Handle exceptions properly


def main():
    sudo_password = os.getenv("SUDO_PASSWORD")
    if not sudo_password:
        print("[ERROR] SUDO_PASSWORD environment variable is not set.")
        print("Please run the following command:")
        print('export SUDO_PASSWORD="your_password"')
        return

    # --- User-configurable max_workers ---
    MAX_WORKERS_SALT_UPDATE = 2
    MAX_WORKERS_DOCKER_RESTART = 4
    MAX_WORKERS_PNP_RESET = 4
    # ------------------------------------

    servers = [
        {"id": i, "host": f"192.168.111.1{i}", "username": "admin", "password": os.getenv("SSH_PASSWORD"), "color": COLORS[(i-1) % len(COLORS)]}
        for i in range(1, 8)
    ]
    
    SERVICE_GROUPS = {
        "1": ["pnp"],
        "2": ["pnp", "vidarr"],
        "3": ["pnp", "vidarr", "mimir"],
        "4": ["vidarr"],
        "5": ["mimir"],
        "6": ["garmr"],
        "7": ["mimir", "garmr"],
        "8": ["pnp", "mimir", "garmr"],
        "9": ["pnp", "vidarr", "mimir", "garmr"]
    }
    
    while True:
        print("\n==========================")
        print("      MAIN MENU")
        print("==========================")
        print("1. Salt update & Docker restart")
        print("2. Docker restart only")
        print("3. Reset PNP")
        print("Q. Exit")
        main_choice = input("Select an option: ")
        
        if main_choice == "q":
            break
        
        if main_choice in ["1", "2"]:
            print("\n------ Select Service Group ------")
            for key, value in SERVICE_GROUPS.items():
                print(f"{key}. {' '.join(value)}")
            print("Q. --Back--")
            
            while True:
                sub_choice = input("Select a service group: ")
                if sub_choice == "q":
                    break
                
                services = SERVICE_GROUPS.get(sub_choice)
                if not services:
                    print("[ERROR] Invalid service group selection.")
                    continue
                
                selected = input("Enter cell numbers to send command (e.g., 12347, 0 for all): ")
                target_servers_for_service = get_target_servers(selected, servers)
                if not target_servers_for_service:
                    print("[ERROR] No target servers selected or invalid input.")
                    continue
                
                if main_choice == "1":
                    run_service_parallel(target_servers_for_service, services, sudo_password, max_workers=MAX_WORKERS_SALT_UPDATE)
                elif main_choice == "2":
                    run_service_parallel(target_servers_for_service, services, sudo_password, docker_restart_only=True, max_workers=MAX_WORKERS_DOCKER_RESTART)
                break
        
        elif main_choice == "3":
            print("\n------ Select Target Cells for PNP Reset ------")
            selected = input("Enter cell numbers to reset PNP (e.g., 12347, 0 for all, q to go back): ")
            if selected == 'q':
                continue

            target_servers_for_reset = get_target_servers(selected, servers)
            if not target_servers_for_reset:
                print("[ERROR] No target servers selected or invalid input.")
            else:
                print(f"Executing PNP reset on cells: {[s['id'] for s in target_servers_for_reset]}")
                run_pnp_reset_parallel(target_servers_for_reset, max_workers=MAX_WORKERS_PNP_RESET)
        
        else:
            print("[ERROR] Invalid option selected.")

if __name__ == "__main__":
    main()
