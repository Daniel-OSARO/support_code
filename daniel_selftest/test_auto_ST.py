import os
import subprocess
import iterm2
import asyncio

########## User part ##########
# Show the menu option
def show_menu():
    print("\n=== Menu ===")
    print("1. Version Check")
    print("2. Self Test")
    print("3. Salt-update")
    print("0. Exit")
    choice = input("Select an option: ")
    return choice

def main():
    """Main menu loop."""
    ensure_iterm2_is_running()

    while True:
        choice = show_menu()

        if choice == "0":
            print("Exiting program...")
            break
        elif choice == "1":
            print("Starting Version Check...")
            version_check()
        elif choice == "2":
            cell_nums = input("Enter cell numbers to test (e.g., 1457): ")
            try:
                selected_cells = [int(num) for num in cell_nums]  # Convert input to a list of integers
                print(f"Selected cells: {selected_cells}")
                iterm2.run_until_complete(lambda connection: setup_panes(connection, selected_cells), retry=True)
            except ValueError:
                print("Invalid input. Please enter a valid sequence of cell numbers (e.g., 1457).")
        elif choice == "3":
            print("Salt-update is not yet implemented.")
        else:
            print("Invalid option. Please try again.")



########## Control iterm2 ##########
def ensure_iterm2_is_running():
    """Check if iTerm2 is running; if not, launch it."""
    try:
        # Check if iTerm2 is running using AppleScript
        output = subprocess.check_output(["osascript", "-e", 'application "iTerm" is running'])
        if output.strip() == b"false":
            raise subprocess.CalledProcessError(1, "iTerm2 is not running")
    except subprocess.CalledProcessError:
        # If not running, launch iTerm2
        print("iTerm2 is not running. Launching iTerm2...")
        subprocess.Popen(["open", "-a", "iTerm"])

def version_check():
    """Version Check: Run a command in the current terminal session."""
    # Path to the script
    script_path = os.path.expanduser("~/Downloads/getdevinfo.sh")

    # Verify that the file exists
    if not os.path.isfile(script_path):
        print(f"Error: {script_path} not found.")
        return
    if not os.access(script_path, os.X_OK):
        print(f"Error: {script_path} is not executable.")
        return

    # Run the script directly in the current terminal
    try:
        subprocess.run(["bash", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: Command execution failed with return code {e.returncode}")


async def setup_panes(connection, selected_cells):
    """
    Split panes first, then execute user-controlled commands in parallel for each pane.
    :param connection: iTerm2 connection object.
    :param selected_cells: List of selected cell numbers.
    """
    global global_panes  # Declare the global dictionary to store pane references
    global_panes = {}  # Initialize the global pane dictionary

    app = await iterm2.async_get_app(connection)

    # Get the current window or create one if it doesn't exist
    windows = app.windows
    if not windows:
        print("No open iTerm2 window found. Creating a new one.")
        window = await app.async_create_window()  # Create a new window
    else:
        window = windows[0]  # Use the first window

    # Get the current tab and create a new one if needed
    current_tab = window.current_tab
    tab = await window.async_create_tab()  # Create a new tab

    # Get the current session in the tab
    current_session = tab.current_session

    # Start with the first session as Pane 1
    sessions = [current_session]
    global_panes[f"Pane {selected_cells[0]}"] = current_session  # Assign a name to the first pane

    # Split panes for the selected cells (up to 4 vertically, then horizontally)
    for i, cell in enumerate(selected_cells[1:], 1):
        if i < 4:
            session = await sessions[-1].async_split_pane(vertical=True)  # Split vertically
        else:
            session = await sessions[i % 4].async_split_pane(vertical=False)  # Split horizontally after 4 cells

        # Assign the session to the global panes dictionary
        global_panes[f"Pane {cell}"] = session
        sessions.append(session)  # Add to the session list

    print(f"Completed splitting panes for cells: {selected_cells}")

    # Allow user to input commands and execute them concurrently in all the panes
    async def run_commands_in_pane(session, commands):
        """
        Send a list of commands to the session in sequence.
        :param session: The session to send the commands to.
        :param commands: A list of commands to send to the pane.
        """
        for command in commands:
            await session.async_send_text(command + '\n')
            print(f"Sent command: {command}")

    # Ask the user for commands to run in each pane
    user_commands = {}  # Store user-defined commands for each pane

    for i, session in enumerate(sessions):
        pane_name = f"Pane {selected_cells[i]}"
        print(f"Enter commands for {pane_name} (separate by commas):")
        commands_input = input(f"Commands for {pane_name}: ").split(",")
        user_commands[pane_name] = [command.strip() for command in commands_input]

    # Run all commands in parallel
    tasks = []
    for i, session in enumerate(sessions):
        pane_name = f"Pane {selected_cells[i]}"
        commands = user_commands.get(pane_name, [])
        task = asyncio.create_task(run_commands_in_pane(session, commands))
        tasks.append(task)

    # Wait for all commands to finish executing in parallel
    await asyncio.gather(*tasks)

    print(f"Completed setup and command execution for cells: {selected_cells}")


async def iterm2_connection(connection):
    app = await iterm2.async_get_app(connection)

    # Get the current window, or create a new one if none exists
    windows = app.windows
    if windows:
        window = windows[0]  # Use the first window if it exists
    else:
        window = await app.async_create_window()  # Otherwise, create a new window

    # Create a new tab in the window
    tab = await window.async_create_tab()

    # Split panes
    pane1 = tab.current_session
    pane2 = await pane1.async_split_pane(vertical=True)
    pane3 = await pane2.async_split_pane(vertical=True)
    pane4 = await pane3.async_split_pane(vertical=True)
    pane5 = await pane1.async_split_pane(vertical=False)
    pane6 = await pane2.async_split_pane(vertical=False)
    pane7 = await pane3.async_split_pane(vertical=False)
    pane8 = await pane4.async_split_pane(vertical=False)

    # Send commands to each pane
    await pane1.async_send_text("ssh admin@192.168.111.11\n")
    await pane2.async_send_text("ssh admin@192.168.111.12\n")
    await pane3.async_send_text("ssh admin@192.168.111.13\n")
    await pane4.async_send_text("ssh admin@192.168.111.14\n")
    await pane5.async_send_text("ssh admin@192.168.111.15\n")
    await pane6.async_send_text("ssh admin@192.168.111.16\n")
    await pane7.async_send_text("ssh admin@192.168.111.17\n")
    await pane8.async_send_text("check_version\n")

def run_iterm2():
    # Ensure iTerm2 is running
    ensure_iterm2_is_running()

    # Connect to iTerm2 and run the script
    iterm2.run_until_complete(iterm2_connection, retry=True)

if __name__ == "__main__":
    main()