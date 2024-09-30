'''#!/usr/bin/env python3
import subprocess, time, tkinter as tk, socket


def check_internet_connection():
    try:
        # Attempt to connect to a well-known website
        socket.create_connection(("www.google.com", 80))
        return True
    except OSError:
        pass
    return False


def wait_for_internet_connection(timeout=60):
    start_time = time.time()
    while True:
        if check_internet_connection():
            print("Internet connection detected.")
            break
        elapsed_time = time.time() - start_time
        if elapsed_time >= timeout:
            print("Timeout reached. Unable to establish internet connection.")
            break
        print("Waiting for internet connection...")
        time.sleep(5)  # Wait for 5 seconds before rechecking


wait_for_internet_connection()

# Template for ffplay command
template = (
    "ffplay -rtsp_transport tcp 'rtsp://admin:osaro51423@192.168.111.1%d:8554/"
    "cam/realmonitor?channel=%d&subtype=1' -left %d -top %d -x %d -y %d -noborder"
)

# Create tkinter root object to get screen dimensions
root = tk.Tk()
root.withdraw()  # Hide the tkinter window immediately
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# Get the target cells input
target_cell_str = input("Target cell: ")
target_cell_int = [int(digit) for digit in target_cell_str]

# Define the number of columns
columns = len(target_cell_int) + 1

# Calculate cell width and height based on columns
cell_width = int(screen_width / columns)
cell_height = int(cell_width / 704 * 480)  # Maintain the aspect ratio
print(f"Cell dimensions: {cell_width}x{cell_height}")

# Initialize index counter for positioning
n = 0

# Loop through each target cell and camera channel to create displays
for k_cell in target_cell_int:
    row = n // columns  # Initialize row for each new cell
    for k_cam in range(1, 4):  # Assuming 3 channels for each cell
        column = n % columns  # Update column position based on n
        cmdline = template % (k_cell, k_cam, column * cell_width, row * cell_height, cell_width, cell_height)
        subprocess.Popen(cmdline, shell=True)
        n += 1  # Increment counter for each camera

    # Move to the next row after finishing the cameras for the current cell
    n = (row + 1) * columns  # Force n to the start of the next row


# Keep the script running until interrupted
while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        exit()
'''

#!/usr/bin/env python3
import subprocess, time, tkinter as tk, socket


def check_internet_connection():
    try:
        # Attempt to connect to a well-known website
        socket.create_connection(("www.google.com", 80))
        return True
    except OSError:
        pass
    return False


def wait_for_internet_connection(timeout=60):
    start_time = time.time()
    while True:
        if check_internet_connection():
            print("Internet connection detected.")
            break
        elapsed_time = time.time() - start_time
        if elapsed_time >= timeout:
            print("Timeout reached. Unable to establish internet connection.")
            break
        print("Waiting for internet connection...")
        time.sleep(5)  # Wait for 5 seconds before rechecking


wait_for_internet_connection()

# Template for ffplay command
template = (
    "ffplay -rtsp_transport tcp 'rtsp://admin:osaro51423@192.168.111.1%d:8554/"
    "cam/realmonitor?channel=%d&subtype=1' -left %d -top %d -x %d -y %d -noborder"
)

# Create tkinter root object to get screen dimensions
root = tk.Tk()
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
root.destroy()

# Get the target cells input
target_cell_str = input("Target cell: ")
target_cell_int = [int(digit) for digit in target_cell_str]

# Define the number of columns
# columns = int(input("Enter the number of columns: "))
columns = len(target_cell_int)+1

# Calculate cell width and height based on columns
cell_width = int(screen_width / columns)
cell_height = int(cell_width / 704 * 480)  # Maintain the aspect ratio
print(f"Cell dimensions: {cell_width}x{cell_height}")

# Initialize index counter for positioning
n = 0

# Loop through each target cell and camera channel to create displays
for k_cell in target_cell_int:
    row = n // columns  # Initialize row for each new cell
    for k_cam in range(1, 4):  # Assuming 3 channels for each cell
        column = n % columns  # Update column position based on n
        cmdline = template % (k_cell, k_cam, column * cell_width, row * cell_height, cell_width, cell_height)
        subprocess.Popen(cmdline, shell=True)
        n += 1  # Increment counter for each camera

    # Move to the next row after finishing the cameras for the current cell
    n = (row + 1) * columns  # Force n to the start of the next row


# Keep the script running until interrupted
while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        exit()