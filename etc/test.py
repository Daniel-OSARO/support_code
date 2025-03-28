import paramiko
import os
from concurrent.futures import ThreadPoolExecutor

def execute_ssh_command(host, username, password, command, sudo_password=None):
    """SSH로 명령어 실행 함수"""
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username=username, password=password, timeout=10)

        if sudo_password:
            command = f'echo {sudo_password} | sudo -S {command}'

        stdin, stdout, stderr = client.exec_command(command)
        output = stdout.read().decode().strip()
        client.close()
        return output
    except Exception as e:
        return f"Error connecting to {host}: {str(e)}"

def get_target_servers(selection, servers):
    """사용자가 선택한 서버 필터링"""
    if selection == "0":
        return servers
    return [server for server in servers if str(server['id']) in selection]

def run_commands_on_servers(target_servers, command, sudo_password=None):
    """병렬로 명령 실행"""
    results = {}
    with ThreadPoolExecutor(max_workers=len(target_servers)) as executor:
        futures = {
            server['id']: executor.submit(
                execute_ssh_command, server['host'], server['username'], server['password'], command, sudo_password
            )
            for server in target_servers
        }
        for server_id, future in futures.items():
            results[server_id] = future.result()
    return results

MENU_TITLES = {
    "1": "Salt update",
    "2": "Test"
}

SALT_UPDATE_MENU = {
    "1": "Salt update pnp",
    "2": "Salt update pnp vidarr",
    "3": "Salt update pnp vidarr mimir",
    "4": "Salt update vidarr",
    "5": "Salt update mimir"
}

TEST_MENU = {
    "1": "Echo Test1",
    "2": "Echo Test2"
}

SALT_UPDATE_COMMANDS = {
    "1": "sudo salt-update pnp koji/cpg-ech02-1-7_onsite && docker restart pnp",
    "2": "sudo salt-update pnp koji/cpg-ech02-1-7_onsite && sudo salt-update vidarr koji/cpg-ech02-1-7_onsite && docker restart pnp vidarr mimir",
    "3": "sudo salt-update pnp koji/cpg-ech02-1-7_onsite && sudo salt-update vidarr koji/cpg-ech02-1-7_onsite && sudo salt-update mimir koji/cpg-ech02-1-7_onsite && docker restart pnp vidarr mimir",
    "4": "sudo salt-update vidarr koji/cpg-ech02-1-7_onsite && docker restart pnp vidarr mimir",
    "5": "sudo salt-update mimir koji/cpg-ech02-1-7_onsite && docker restart pnp vidarr mimir"
}

TEST_COMMANDS = {
    "1": "echo test1",
    "2": "echo test2"
}

def display_menu(options):
    for key, value in options.items():
        print(f"{key}. {value}")
    print("9. Back")

def main():
    servers = [
        {"id": i, "host": f"192.168.111.1{i}", "username": "admin", "password": os.getenv("SSH_PASSWORD")}
        for i in range(1, 8)
    ]
    
    while True:
        print("\n==========================")
        print("      MAIN MENU")
        print("==========================")
        for key, title in MENU_TITLES.items():
            print(f"{key}. {title}")
        print("9. Exit")
        main_choice = input("Select an option: ")
        
        if main_choice == "9":
            break
        
        if main_choice == "1":
            sub_commands = SALT_UPDATE_COMMANDS
            print("\n------ Salt update ------")
            display_menu(SALT_UPDATE_MENU)
        elif main_choice == "2":
            sub_commands = TEST_COMMANDS
            print("\n------ Test ------")
            display_menu(TEST_MENU)
        else:
            print("Invalid selection, try again.")
            continue
        
        while True:
            sub_choice = input("Select a command: ")
            if sub_choice == "9":
                break
            
            command = sub_commands.get(sub_choice)
            if not command:
                print("Invalid selection, try again.")
                continue
            
            selected = input("Enter cell numbers to send command (e.g., 12347, 0 for all): ")
            target_servers = get_target_servers(selected, servers)
            
            sudo_password = None
            if main_choice == "1":
                sudo_password = input("Enter sudo password: ")
            
            results = run_commands_on_servers(target_servers, command, sudo_password)
            for server_id, output in results.items():
                print(f"\n[Server {server_id}] Output:\n{output}\n")

if __name__ == "__main__":
    main()
