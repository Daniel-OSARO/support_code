import paramiko
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time

# export SUDO_PASSWORD="muchscarydontuse"

def execute_ssh_command(host, username, password, command, sudo_password=None):
    """Executes an SSH command and returns output, error, and exit status."""
    client = None
    try:
        # print(f"[DEBUG] Connecting to {host}...")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username=username, password=password, timeout=10)

        # print(f"[DEBUG] Executing command on {host}: {command}")
        if "sudo" in command and sudo_password:
            # Ensure sudo command structure is robust
            command = f"echo '{sudo_password}' | sudo -S -p '' {command.replace('sudo ', '', 1)}" # More reliable sudo injection

        stdin, stdout, stderr = client.exec_command(command, get_pty=True) # get_pty can sometimes be needed for sudo

        # Reading stdout and stderr before checking exit status
        output = stdout.read().decode(errors='ignore').strip()
        error = stderr.read().decode(errors='ignore').strip()
        exit_status = stdout.channel.recv_exit_status() # Get exit status

        # print(f"[DEBUG] Output from {host}: {output}")
        # print(f"[DEBUG] Error from {host}: {error}")
        # print(f"[DEBUG] Exit status from {host}: {exit_status}")

        return output, error, exit_status

    except Exception as e:
        print(f"[ERROR] SSH execution failed on {host}: {e}")
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
        if not docker_restart_only:  # Run Salt update first
            print(f"[DEBUG] Processing salt update {service} on Cell{server['id']}")
            # Ensure the command uses sudo correctly if needed
            salt_command = f"sudo salt-update {service} koji/cpg-ech02-1-7_onsite"
            salt_output, salt_error, salt_exit_status = execute_ssh_command(
                server['host'], server['username'], server['password'], salt_command, sudo_password
            )

            # Check exit status first for success (0 means success)
            if salt_exit_status == 0:
                # Optionally check output for 'Failed: 0' for extra confirmation
                failed_matches = re.findall(r'Failed:\s+(\d+)', salt_output)
                failed_count = sum(int(match) for match in failed_matches) if failed_matches else 0
                if failed_count == 0:
                    print(f"✅ Salt update Success: Cell{server['id']}, Salt Update {service}")
                else:
                    # Exit status was 0, but output indicates failure? Log this inconsistency.
                    print(f"⚠️ Salt update Warning: Cell{server['id']}, Salt Update {service} - Exit status 0 but output shows {failed_count} failures. Output: {salt_output[:100]}...")
            else:
                # Non-zero exit status indicates failure
                error_details = salt_error if salt_error else salt_output # Show stderr if available, otherwise stdout
                print(f"❌ Salt update Failed: Cell{server['id']}, Salt Update {service} (Exit: {salt_exit_status}). Error: {error_details[:150]}...")
                return # Stop processing this service for this server if salt update failed

        # Always restart Docker for the service (only if salt update succeeded or was skipped)
        print(f"[DEBUG] Processing docker restart {service} on Cell{server['id']}")
        # Docker restart might not need sudo depending on setup, but let's assume it might
        restart_command = f"sudo docker restart {service}"
        restart_output, restart_error, restart_exit_status = execute_ssh_command(
            server['host'], server['username'], server['password'], restart_command, sudo_password
        )

        # Check exit status for docker restart
        if restart_exit_status == 0:
             # Check if the output contains the service name as a confirmation
             # This might be less reliable than checking the exit status
            if service in restart_output or service in restart_error: # Sometimes confirmation is in stderr
                 print(f"✅ Docker restart Success: Cell{server['id']}, Docker Restart {service}")
            else:
                 # Command succeeded (exit status 0) but output confirmation missing
                 print(f"🤔 Docker restart Status Uncertain: Cell{server['id']}, Docker Restart {service}. Exit status 0, but confirmation string not found. Output: {restart_output[:100]}...")
        else:
            error_details = restart_error if restart_error else restart_output
            print(f"❌ Docker restart Failed: Cell{server['id']}, Docker Restart {service} (Exit: {restart_exit_status}). Error: {error_details[:150]}...")

    except Exception as e:
        print(f"[ERROR] Error processing {service} on {server['host']}: {e}")


def process_services_for_server(server, services, sudo_password, docker_restart_only):
    try:
        for service in services:
            process_service(server, service, sudo_password, docker_restart_only)
    except Exception as e:
        print(f"[ERROR] Error processing services on {server['host']}: {e}")


def execute_pnp_reset(server):
    """Executes the PNP reset curl command on a given server."""
    try:
        print(f"[DEBUG] Processing PNP reset on Cell{server['id']}")
        reset_command = "curl -i -X POST 'http://localhost:80/v1/pnp/reset'"
        output = execute_ssh_command(server['host'], server['username'], server['password'], reset_command)

        if output and "HTTP/1.1 200 OK" in output: # Check for a successful HTTP response
            print(f"✅ PNP Reset Success: Cell{server['id']}")
        elif output:
            print(f"❌ PNP Reset Failed: Cell{server['id']}, Output: {output[:100]}...") # Print partial output on failure
        else:
            print(f"❌ PNP Reset Failed: Cell{server['id']} (No output or connection error)")

    except Exception as e:
        print(f"[ERROR] Error executing PNP reset on {server['host']}: {e}")


def run_pnp_reset_parallel(target_servers):
    """Runs the PNP reset command in parallel on the target servers."""
    # Limit concurrency for PNP reset as well, maybe less restrictive than salt
    with ThreadPoolExecutor(max_workers=2) as executor: # Example: limit PNP reset concurrency too
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


def run_service_parallel(target_servers, services, sudo_password, docker_restart_only=False):
    # Limit the number of concurrent workers to 2
    with ThreadPoolExecutor(max_workers=2) as executor:
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

    servers = [
        {"id": i, "host": f"192.168.111.1{i}", "username": "admin", "password": os.getenv("SSH_PASSWORD")}
        for i in range(1, 8)
    ]
    
    SERVICE_GROUPS = {
        "1": ["pnp"],
        "2": ["pnp", "vidarr"],
        "3": ["pnp", "vidarr", "mimir"],
        "4": ["vidarr"],
        "5": ["mimir"],
        "6": ["garmr"],
        "7": ["mimir", "garmr"]
    }
    
    while True:
        print("\n==========================")
        print("      MAIN MENU")
        print("==========================")
        print("1. Salt update & Docker restart")
        print("2. Docker restart only")
        print("3. Reset PNP")
        print("9. Exit")
        main_choice = input("Select an option: ")
        
        if main_choice == "9":
            break
        
        if main_choice in ["1", "2"]:
            print("\n------ Select Service Group ------")
            for key, value in SERVICE_GROUPS.items():
                print(f"{key}. {' '.join(value)}")
            print("9. --Back--")
            
            while True:
                sub_choice = input("Select a service group: ")
                if sub_choice == "9":
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
                    run_service_parallel(target_servers_for_service, services, sudo_password)
                elif main_choice == "2":
                    run_service_parallel(target_servers_for_service, services, sudo_password, docker_restart_only=True)
                break
        
        elif main_choice == "3":
            print("\n------ Select Target Cells for PNP Reset ------")
            selected = input("Enter cell numbers to reset PNP (e.g., 12347, 0 for all, 9 to go back): ")
            if selected == '9':
                continue

            target_servers_for_reset = get_target_servers(selected, servers)
            if not target_servers_for_reset:
                print("[ERROR] No target servers selected or invalid input.")
            else:
                print(f"Executing PNP reset on cells: {[s['id'] for s in target_servers_for_reset]}")
                run_pnp_reset_parallel(target_servers_for_reset)
        
        else:
            print("[ERROR] Invalid option selected.")

if __name__ == "__main__":
    main()
