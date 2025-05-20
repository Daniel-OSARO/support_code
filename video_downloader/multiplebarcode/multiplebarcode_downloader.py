import csv
import os
import time
import subprocess
from datetime import datetime, timedelta, timezone
import requests
from requests.auth import HTTPDigestAuth
from tqdm import tqdm
import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import structlog
import colorama
from colorama import Fore, Back, Style
from google.cloud import logging as cloud_logging
import json
import re

# Initialize colorama
colorama.init()

# Configure structlog
logger = structlog.get_logger()

def create_folder(folder_name):
    """Create a folder if it doesn't exist."""
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name

def get_nvr_address(worker_id):
    """Get NVR address based on worker ID."""
    mapping = {
        "rw_136_robotagent1": "192.168.111.11:8010",
        "rw_136_robotagent2": "192.168.111.12:8010",
        "rw_136_robotagent3": "192.168.111.13:8010",
        "rw_136_robotagent4": "192.168.111.14:8010",
        "rw_136_robotagent5": "192.168.111.15:8010",
        "rw_136_robotagent6": "192.168.111.16:8010",
        "rw_136_robotagent7": "192.168.111.17:8010"
    }
    return mapping.get(worker_id)

def get_hostname_from_worker_id(worker_id):
    """Extracts the Cloud Logging hostname based on the worker ID."""
    if worker_id and worker_id.startswith("rw_136_robotagent"):
        try:
            agent_number = int(worker_id[-1])
            return f"cpg-ech02-{agent_number}"
        except ValueError:
            print_error(f"Could not parse agent number from worker ID: {worker_id}")
            return None
    print_warning(f"Could not determine hostname for worker ID: {worker_id}")
    return None

def build_log_query_filter(start_time_str, end_time_str, hostnames, cell_numbers):
    """Builds the filter string for the Cloud Logging API."""
    # Base query parts
    base_query = """
    jsonPayload.app_name="osaro-pnp"
    severity>="INFO"
    --jsonPayload.fields.err:"MultipleValidBarcodes"
    jsonPayload.fields.message:"barcode scan sender canceled, barcode unavailable for mass estimation"
    """
    
    # Add hostname and timestamp filters
    hostname_filters = [f'jsonPayload.hostname="{hostname}"' for hostname in hostnames]
    hostname_query = " OR ".join(hostname_filters)
    
    filter_parts = [
        f'timestamp >= "{start_time_str}"',
        f'timestamp <= "{end_time_str}"',
        f"({hostname_query})",
        base_query
    ]

    return " AND ".join(filter_parts)

def fetch_timestamps_from_logs(project_id, filter_str):
    """Queries Cloud Logging and extracts timestamps."""
    client = cloud_logging.Client(project=project_id)
    logger.info(f"Executing Cloud Logging query with filter:\n{filter_str}")
    
    timestamps = {}
    try:
        entries = client.list_entries(filter_=filter_str, order_by=cloud_logging.DESCENDING)
        
        for entry in entries:
            # logger.info(f"Raw entry payload: {entry.payload}") # Log the raw payload for debugging - Commented out as the issue is identified
            try:
                # Extract timestamp from the log entry
                timestamp_str = entry.timestamp.isoformat()
                if not timestamp_str.endswith('Z') and '+' not in timestamp_str:
                    timestamp_str += 'Z'
                timestamp_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                
                # Use timestamp as key
                # Accessing hostname and fields directly from entry.payload as jsonPayload key is not present
                timestamps[timestamp_dt] = {
                    'hostname': entry.payload['hostname'],
                    'message': entry.payload['fields'].get('message', '')
                }
                logger.info(f"Found timestamp: {timestamp_dt} for hostname: {entry.payload['hostname']}")

            except (KeyError, TypeError) as e:
                logger.warning(f"Could not parse timestamp or expected fields from log entry: {e} - Payload: {entry.payload}")
                continue
                
    except Exception as e:
        print_error(f"Error querying Cloud Logging: {e}")
        return None

    return timestamps

def convert_video(input_file, output_folder):
    """Convert DAV file to MP4 format."""
    output_file = os.path.join(output_folder, os.path.basename(input_file).replace(".dav", ".mp4"))
    command = ["ffmpeg", "-y", "-i", input_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", output_file]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(output_file):
        print(f"Conversion complete: {output_file}")
    else:
        print(f"Conversion failed for {input_file}")

def get_video(nvrname, start_time, end_time, vid_name, target_folder_path, channel=1, failed_downloads_file=None, downloaded_log_file=None):
    """Downloads a video segment to the specified target folder."""
    if not nvrname:
        print_error("Invalid NVR address provided to get_video. Skipping download.")
        return
    
    try:
        create_folder(target_folder_path)
    except OSError as e:
        print_error(f"Could not create target folder {target_folder_path}: {e}. Skipping download for {vid_name}")
        return

    url = f"http://{nvrname}/cgi-bin/loadfile.cgi?action=startLoad&channel={channel}&startTime={start_time}&endTime={end_time}"
    username = "admin"
    password = "osaro51423"
    
    print_info(f"Downloading video: {vid_name} to {target_folder_path}")
    
    try:
        with requests.get(url, auth=HTTPDigestAuth(username, password), stream=True, timeout=60) as response:
            if response.status_code == 200:
                file_size = int(response.headers.get('content-length', 0))
                progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True, desc=vid_name, leave=False)
                
                file_path = os.path.join(target_folder_path, f"{vid_name}.dav")
                
                with open(file_path, 'wb') as out_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            progress_bar.update(len(chunk))
                            out_file.write(chunk)
                progress_bar.close()
                
                if file_size != 0 and progress_bar.n != file_size:
                    print_warning(f"Download of {vid_name} may be incomplete. Expected {file_size} bytes, got {progress_bar.n} bytes. File saved to: {file_path}")
                else:
                    print_success(f"Download complete: {file_path}")
                
                if downloaded_log_file:
                    try:
                        with open(downloaded_log_file, 'a') as log_f:
                            log_f.write(f"{vid_name}\n")
                    except IOError as e:
                        print_error(f"Could not write to downloaded log file {downloaded_log_file}: {e}")
            else:
                print_error(f"Error {response.status_code} downloading {vid_name}: {response.text}")
                if failed_downloads_file:
                    with open(failed_downloads_file, 'a') as f:
                        f.write(f"{vid_name}\n")
    except requests.exceptions.Timeout:
        print_error(f"Timeout occurred while downloading {vid_name} from {nvrname}")
        if failed_downloads_file:
            with open(failed_downloads_file, 'a') as f:
                f.write(f"{vid_name}\n")
    except requests.exceptions.RequestException as e:
        print_error(f"An error occurred during download request for {vid_name}: {e}")
        if failed_downloads_file:
            with open(failed_downloads_file, 'a') as f:
                f.write(f"{vid_name}\n")
    except Exception as e:
        print_error(f"An unexpected error occurred during download/saving of {vid_name}: {e}")
        if failed_downloads_file:
            with open(failed_downloads_file, 'a') as f:
                f.write(f"{vid_name}\n")

def print_success(message):
    logger.info(f"{Fore.GREEN}{message}{Style.RESET_ALL}")

def print_error(message):
    logger.error(f"{Fore.RED}{message}{Style.RESET_ALL}")

def print_warning(message):
    logger.warning(f"{Fore.YELLOW}{message}{Style.RESET_ALL}")

def print_info(message):
    logger.info(f"{Fore.CYAN}{message}{Style.RESET_ALL}")

def get_dav_folders():
    """Get list of folders containing DAV files."""
    current_dir = os.path.dirname(__file__)
    folders = []
    for item in os.listdir(current_dir):
        if os.path.isdir(os.path.join(current_dir, item)):
            if any(f.endswith('.dav') for f in os.listdir(os.path.join(current_dir, item))):
                folders.append(item)
    return folders

def select_dav_folder():
    """Let user select a folder containing DAV files."""
    dav_folders = get_dav_folders()
    if not dav_folders:
        print_error("No folders containing DAV files found in the current directory.")
        return None
    
    print("\nAvailable folders with DAV files:")
    for i, folder in enumerate(dav_folders, 1):
        print(f"{Fore.GREEN}{i}. {folder}{Style.RESET_ALL}")
    
    while True:
        try:
            choice = input(f"\n{Fore.CYAN}Enter the number of the folder (or 'm' for manual input, 'q' to quit): {Style.RESET_ALL}").strip()
            if choice.lower() == 'q':
                return None
            elif choice.lower() == 'm':
                manual_input = input(f"{Fore.CYAN}Enter folder name manually: {Style.RESET_ALL}").strip()
                if os.path.exists(os.path.join(os.path.dirname(__file__), manual_input)):
                    return manual_input
                else:
                    print_error("Folder does not exist. Please try again.")
                    continue
            choice = int(choice)
            if 1 <= choice <= len(dav_folders):
                return dav_folders[choice - 1]
            else:
                print_error("Invalid selection. Please try again.")
        except ValueError:
            print_error("Please enter a valid number.")

def menu1_download_dav():
    """Menu option 1: Download DAV files based on logs."""
    # --- User Configuration Start ---
    # Set the date and time range for searching here (YYYY-MM-DD HH:MM:SS UTC format)
    DEFAULT_START_DATE_STR = "2025-05-02 23:00:00"  # Example: "2025-05-17 23:00:00"
    DEFAULT_END_DATE_STR = "2025-05-03 19:00:00"    # Example: "2025-05-18 19:00:00"
    # --- User Configuration End ---

    try:
        # Convert the date-time strings to datetime objects and make them UTC aware
        start_time = datetime.strptime(DEFAULT_START_DATE_STR, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        end_time = datetime.strptime(DEFAULT_END_DATE_STR, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

        if end_time <= start_time:
            print_error("Configured end date/time is before or the same as the start date/time. Please check the values.")
            return
    except ValueError:
        print_error("Invalid date-time format. Please use YYYY-MM-DD HH:MM:SS format in UTC.")
        return
    
    # Get cell numbers from user
    while True:
        try:
            cell_input_str = input(f"\n{Fore.CYAN}Enter cell numbers (e.g., 1,3,5 or 1356 for cells 1, 3, 5, 6): {Style.RESET_ALL}").strip()
            if not cell_input_str: 
                print_error("Cell numbers cannot be empty. Please try again.")
                continue

            if ',' in cell_input_str:
                cell_numbers = [int(x.strip()) for x in cell_input_str.split(',')]
            else:
                if not cell_input_str.isdigit():
                    print_error("Invalid input. For continuous digits, ensure all characters are numbers (1-7).")
                    continue
                cell_numbers = [int(char) for char in cell_input_str]
            
            if not all(1 <= x <= 7 for x in cell_numbers):
                print_error("Cell numbers must be between 1 and 7.")
                continue
            break
        except ValueError:
            print_error("Invalid input. Please use comma-separated numbers (e.g., 1,3,5) or a sequence of digits (e.g., 1356). Ensure all numbers are between 1 and 7.")
    
    # Get channel number from user
    while True:
        try:
            channel = input(f"\n{Fore.CYAN}Select channel (1-4, default: 1): {Style.RESET_ALL}").strip()
            if not channel:
                channel = 1
                break
            channel = int(channel)
            if 1 <= channel <= 4:
                break
            else:
                print_error("Invalid channel number")
        except ValueError:
            print_error("Invalid channel number")
    
    # Create folder name based on time range and cell numbers
    # Use the start date and time for the folder name, and add _dav suffix
    folder_name = f"{start_time.strftime('%Y%m%d_%H%M%S')}_C{','.join(map(str, cell_numbers))}_ch{channel}_dav"
    failed_downloads_file = f"{folder_name}_failed_downloads.txt"
    downloaded_log_file = f"{folder_name}_downloaded.txt"
    
    # Clear previous files if they exist
    if os.path.exists(failed_downloads_file):
        os.remove(failed_downloads_file)
    if os.path.exists(downloaded_log_file):
        os.remove(downloaded_log_file)
    
    print_info(f"\nCreating folder: {folder_name}")
    print_info(f"Selected channel: {channel}")
    
    # Get hostnames for selected cells
    hostnames = [f"cpg-ech02-{cell}" for cell in cell_numbers]
    
    # Build and execute log query
    # Create time strings for log query (already in UTC)
    log_start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    log_end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    log_filter = build_log_query_filter(log_start_time_str, log_end_time_str, hostnames, cell_numbers)
    timestamps = fetch_timestamps_from_logs("osaro-logging", log_filter)
    
    if not timestamps:
        print_error("No timestamps found in logs for the given criteria.")
        return
    
    print_info(f"Found {len(timestamps)} timestamps in logs.")
    
    # Download videos
    download_tasks = []
    with ThreadPoolExecutor(max_workers=4) as download_executor:
        for timestamp_dt, data in timestamps.items():
            hostname = data['hostname']
            cell_number = int(hostname.split('-')[-1])
            
            # Calculate video time range
            video_start = timestamp_dt - timedelta(seconds=15)
            video_end = timestamp_dt + timedelta(seconds=30)
            
            start_time_str = video_start.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = video_end.strftime('%Y-%m-%d %H:%M:%S')
            
            # Generate video name
            vid_name = f"{video_start.strftime('%Y%m%d_%H%M%S')}_C{cell_number}_ch{channel}"
            
            # Get NVR address
            worker_id = f"rw_136_robotagent{cell_number}"
            nvr_address = get_nvr_address(worker_id)
            
            if not nvr_address:
                print_error(f"Could not determine NVR address for cell {cell_number}")
                continue
            
            print_info(f"Queueing download for {vid_name}")
            future = download_executor.submit(
                get_video, nvr_address, start_time_str, end_time_str,
                vid_name, folder_name, channel, failed_downloads_file, downloaded_log_file
            )
            download_tasks.append(future)
        
        # Wait for all downloads to complete
        for future in download_tasks:
            future.result()
    
    print_success("\nDownload process completed!")

def menu2_convert_to_mp4():
    """Menu option 2: Convert DAV files to MP4."""
    dav_folder = select_dav_folder()
    if not dav_folder:
        return
    
    dav_folder_path = os.path.join(os.path.dirname(__file__), dav_folder)
    
    # Ensure base_folder_name correctly removes _dav and any channel info before adding _mp4
    base_folder_name = dav_folder
    if base_folder_name.endswith("_dav"):
        base_folder_name = base_folder_name[:-4] # Remove '_dav'
    
    # Check if folder name contains "_ch" followed by a number, and preserve it for mp4 folder
    ch_match = re.search(r'_ch\d+', base_folder_name)
    if ch_match:
        channel_part = ch_match.group(0)
        # Remove channel part from base to avoid duplication if it was part of original dav_folder name structure
        base_name_no_channel = base_folder_name.replace(channel_part, '') 
        mp4_folder = f"{base_name_no_channel}{channel_part}_mp4"
    else:
        mp4_folder = f"{base_folder_name}_mp4"

    mp4_folder_path = os.path.join(os.path.dirname(__file__), mp4_folder)
    create_folder(mp4_folder_path)
    
    print_info(f"\nConverting DAV files from folder: {dav_folder}")
    print_info(f"Creating MP4 folder: {mp4_folder}")
    
    dav_files = [f for f in os.listdir(dav_folder_path) if f.endswith('.dav')]
    
    if not dav_files:
        print_error(f"No DAV files found in {dav_folder}")
        return
    
    print_info(f"Found {len(dav_files)} DAV files to convert")
    
    with ProcessPoolExecutor(max_workers=3) as convert_executor:
        for dav_file in dav_files:
            input_file = os.path.join(dav_folder_path, dav_file)
            print_info(f"Converting {dav_file}...")
            convert_executor.submit(convert_video, input_file, mp4_folder_path)
    
    print_success("\nConversion process completed!")

def print_menu():
    """Print the main menu."""
    print(f"\n{Fore.CYAN}{'='*50}")
    print(f"{Fore.YELLOW}=== Multiple Barcode Video Downloader Menu ===")
    print(f"{Fore.CYAN}{'='*50}")
    print(f"{Fore.GREEN}1. Download DAV files from logs")
    print(f"{Fore.BLUE}c. Convert DAV files to MP4")
    print(f"{Fore.RED}Enter 'q' to exit")
    print(f"{Fore.CYAN}{'='*50}{Style.RESET_ALL}")

def main():
    """Main function."""
    while True:
        print_menu()
        choice = input(f"{Fore.CYAN}Select menu (1, c or 'q'): {Style.RESET_ALL}").strip().lower()
        
        if choice == 'q':
            print_info("Exiting program...")
            break
        elif choice == "1":
            menu1_download_dav()
        elif choice == "c":
            menu2_convert_to_mp4()
        else:
            print_error("Invalid selection. Please try again.")

if __name__ == '__main__':
    main()
