# This script downloads video clips from an NVR based on CSV data.
# It supports downloading videos for "induction error" and "item lost" scenarios,
# and also provides functionality to convert downloaded DAV files to MP4 format.
#
# Usage:
# 1. Obtain a CSV file from Grafana's Item Stuck Barcode Info.
# 2. Establish an SSH tunnel to the NVR (e.g., `ssh -L 8005:192.168.8.10:80 -A cx` where x is the cell number).
# 3. Place this script and the CSV file in the same directory.
# 4. Run the script: `python3 held_videos.py` and follow the menu prompts.

import csv
import os
import time
from datetime import datetime, timedelta, timezone
import requests
from requests.auth import HTTPDigestAuth
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import re
import argparse
import structlog
import colorama
from colorama import Fore, Back, Style
import json
from google.cloud import logging as cloud_logging
import subprocess

# Initialize colorama
colorama.init()

# Configure structlog
logger = structlog.get_logger()

# --- Print Helper Functions ---
def print_success(message):
    logger.info(f"{Fore.GREEN}{message}{Style.RESET_ALL}")

def print_error(message):
    logger.error(f"{Fore.RED}{message}{Style.RESET_ALL}")

def print_warning(message):
    logger.warning(f"{Fore.YELLOW}{message}{Style.RESET_ALL}")

def print_info(message):
    logger.info(f"{Fore.CYAN}{message}{Style.RESET_ALL}")

# --- End Print Helper Functions ---

# --- Utility Functions ---
def get_csv_files():
    current_dir = os.path.dirname(__file__)
    return [f for f in os.listdir(current_dir) if f.endswith('.csv')]

def get_dav_folders():
    current_dir = os.path.dirname(__file__)
    folders = []
    for item in os.listdir(current_dir):
        if os.path.isdir(os.path.join(current_dir, item)):
            # Check if folder contains .dav files
            if any(f.endswith('.dav') for f in os.listdir(os.path.join(current_dir, item))):
                folders.append(item)
    return folders

def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name

def unique_filename(base_filename):
    """
    Returns a unique filename by appending a counter if the file already exists.
    """
    counter = 1
    filename, file_extension = os.path.splitext(base_filename)
    new_filename = base_filename
    while os.path.exists(new_filename):
        new_filename = f"{filename}_({counter}){file_extension}"
        counter += 1
    return new_filename

def create_date_folder(start_time_str):
    """
    Extracts the date from a string in the format 'YYYY-MM-DD HH:MM:SS'
    and creates a folder named after the date if it doesn't exist.
    """
    date_str = start_time_str.split(" ")[0]
    if not os.path.exists(date_str):
        os.makedirs(date_str)
    return date_str

def get_nvr_address_from_cell(cell_num_str):
    """
    Determines the NVR address based on the provided cell number.
    Assumes cell_num_str is '1' through '7' and maps to 192.168.111.11 through 192.168.111.17.
    """
    try:
        cell_number = int(cell_num_str)
        if 1 <= cell_number <= 7:
            # Map cell 1 to 11, cell 2 to 12, etc.
            last_octet = 10 + cell_number
            return f"192.168.111.{last_octet}:8010"
        else:
            print_error(f"Invalid cell number: {cell_num_str}. Please enter 1-7.")
            return None
    except ValueError:
        print_error(f"Invalid cell number format: {cell_num_str}. Please enter a digit.")
        return None

def get_hostname_from_cell_number(cell_num_str):
    """
    Extracts the Cloud Logging hostname based on the cell number.
    """
    try:
        cell_number = int(cell_num_str)
        if 1 <= cell_number <= 7:
            return f"cpg-ech02-{cell_number}"
        else:
            print_error(f"Invalid cell number: {cell_num_str}. Please enter 1-7.")
            return None
    except ValueError:
        print_error(f"Invalid cell number format: {cell_num_str}. Please enter a digit.")
        return None

def get_video(nvrname, start_time, end_time, vid_name, target_folder_path, channel=1, failed_downloads_file=None, downloaded_log_file=None):
    """Downloads a video segment to the specified target folder.
    Logs successful downloads to downloaded_log_file if provided.
    """
    if not nvrname:
        print_error("Invalid NVR address provided to get_video. Skipping download.")
        if failed_downloads_file:
            try:
                with open(failed_downloads_file, 'a') as f: f.write(f"{vid_name}\n")
            except IOError as ioe:
                 print_error(f"Could not write to failed downloads file {failed_downloads_file}: {ioe}")
        return False
    
    try:
        create_folder(target_folder_path)
    except OSError as e:
        print_error(f"Could not create target folder {target_folder_path}: {e}. Skipping download for {vid_name}")
        if failed_downloads_file:
            try:
                with open(failed_downloads_file, 'a') as f: f.write(f"{vid_name}\n")
            except IOError as ioe:
                 print_error(f"Could not write to failed downloads file {failed_downloads_file}: {ioe}")
        return False

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
                    print_warning(f"Download incomplete for {vid_name}. Expected {file_size}, got {progress_bar.n}")
                    if failed_downloads_file:
                         try:
                             with open(failed_downloads_file, 'a') as f: f.write(f"{vid_name}\n")
                         except IOError as ioe:
                             print_error(f"Could not write to failed downloads file {failed_downloads_file}: {ioe}")
                    return False
                else:
                    print_success(f"Download complete: {file_path}")
                    return True
    except requests.exceptions.Timeout:
        print_error(f"Timeout occurred while downloading {vid_name} from {nvrname}")
        if failed_downloads_file:
            try:
                with open(failed_downloads_file, 'a') as f: f.write(f"{vid_name}\n")
            except IOError as ioe:
                print_error(f"Could not write to failed downloads file {failed_downloads_file}: {ioe}")
        return False
    except requests.exceptions.RequestException as e:
        print_error(f"An error occurred during download request for {vid_name}: {e}")
        if failed_downloads_file:
            try:
                with open(failed_downloads_file, 'a') as f: f.write(f"{vid_name}\n")
            except IOError as ioe:
                print_error(f"Could not write to failed downloads file {failed_downloads_file}: {ioe}")
        return False
    except Exception as e:
        print_error(f"An unexpected error occurred during download/saving of {vid_name}: {e}")
        if failed_downloads_file:
            try:
                with open(failed_downloads_file, 'a') as f: f.write(f"{vid_name}\n")
            except IOError as ioe:
                 print_error(f"Could not write to failed downloads file {failed_downloads_file}: {ioe}")
        return False
    return False

# --- Helper function to count lines in a file safely ---
def count_lines_in_file(filepath):
    """Counts the number of lines in a file, returning 0 if file doesn't exist."""
    try:
        with open(filepath, 'r') as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0
# --- End helper function ---

def build_log_query_filter(start_time_str, end_time_str, hostname, bagger_error_codes=None):
    """Builds the filter string for the Cloud Logging API for bagger error logs."""
    # Fixed query parts
    base_query = """
    jsonPayload.app_name="osaro-pnp"
    severity="WARNING"
    jsonPayload.fields.message="bagger error while waiting for ready"
    """
    # Add hostname and timestamp filters
    filter_parts = [
        f'timestamp >= "{start_time_str}"',
        f'timestamp <= "{end_time_str}"',
        f'jsonPayload.hostname="{hostname}"',
        base_query
    ]

    # Add bagger error code filter if provided
    if bagger_error_codes:
        # Create OR conditions for multiple error codes
        error_filters = []
        for code in bagger_error_codes:
            error_filters.append(f'jsonPayload.fields.error:"BaggerErrorCode {{ code: {code} }}"')
        error_query = " OR ".join(error_filters)
        filter_parts.append(f"({error_query})")

    return " AND ".join(filter_parts)

def fetch_timestamps_from_logs(project_id, filter_str):
    """Queries Cloud Logging and extracts relevant timestamps and hostnames."""
    client = cloud_logging.Client(project=project_id)
    print_info(f"Executing Cloud Logging query with filter:\n{filter_str}")
    
    log_entries_info = [] # List of (timestamp_dt, hostname) tuples
    try:
        entries = client.list_entries(filter_=filter_str, order_by=cloud_logging.DESCENDING) 
        
        for entry in entries:
            try:
                # Convert the LogEntry to its API representation (a dict) for consistent access
                api_repr = entry.to_api_repr()

                # The actual structured log data can be in 'jsonPayload' or 'protoPayload'
                payload_content = api_repr.get('jsonPayload')
                if not payload_content:
                    payload_content = api_repr.get('protoPayload') # This will be a dict if it was StructEntry

                if not payload_content:
                    print_warning(f"Could not retrieve any structured payload for log entry: {entry.log_name} with insertId: {entry.insert_id}")
                    continue

                # Now access hostname and fields from payload_content
                hostname = payload_content.get('hostname')
                # If hostname is not directly in payload_content, try resource labels (as before)
                if not hostname and api_repr.get('resource') and api_repr['resource'].get('labels'):
                    hostname = api_repr['resource']['labels'].get('instance_id')
                    if not hostname:
                         hostname = api_repr['resource']['labels'].get('pod_name') or api_repr['resource']['labels'].get('container_name')
                
                timestamp_str = entry.timestamp.isoformat()
                if not timestamp_str.endswith('Z') and '+' not in timestamp_str:
                     timestamp_str += 'Z'
                timestamp_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

                # Extract bagger error code from fields within the payload_content
                bagger_error_match = re.search(r'code: (\d+)', payload_content.get('fields', {}).get('error', ''))
                bagger_code = bagger_error_match.group(1) if bagger_error_match else "unknown"
                
                if hostname and timestamp_dt:
                    log_entries_info.append((timestamp_dt, hostname, bagger_code))
                    print_info(f"Found log entry: Host={hostname}, Time={timestamp_dt}, BaggerCode={bagger_code}")

            except Exception as e:
                print_warning(f"Could not parse log entry: {e} - Insert ID: {getattr(entry, 'insert_id', 'N/A')}")
                continue
                
    except Exception as e:
        print_error(f"Error querying Cloud Logging: {e}")
        return None

    return log_entries_info

def select_csv_file():
    csv_files = get_csv_files()
    if not csv_files:
        print_error("No CSV files found.")
        return None
    print("\nAvailable CSV files:")
    for i, file in enumerate(csv_files, 1):
        print(f"{i}. {file}")
    while True:
        try:
            choice = input(f"\n{Fore.CYAN}Select CSV file number (m: manual input, q: quit): {Style.RESET_ALL}").strip()
            if choice.lower() == 'q':
                return None
            elif choice.lower() == 'm':
                manual_input = input(f"{Fore.CYAN}Enter CSV filename manually: {Style.RESET_ALL}").strip()
                if os.path.exists(os.path.join(os.path.dirname(__file__), manual_input)) and manual_input.endswith('.csv'):
                    return manual_input
                else:
                    print_error("File does not exist or is not a CSV file.")
                    continue
            choice = int(choice)
            if 1 <= choice <= len(csv_files):
                return csv_files[choice - 1]
            else:
                print_error("Invalid selection. Please try again.")
        except ValueError:
            print_error("Please enter a valid number.")

def select_dav_folder():
    dav_folders = get_dav_folders()
    if not dav_folders:
        print_error("No folders containing DAV files found.")
        return None
    print("\nAvailable folders with DAV files:")
    for i, folder in enumerate(dav_folders, 1):
        print(f"{i}. {folder}")
    while True:
        try:
            choice = input(f"{Fore.CYAN}Select folder number (m: manual input, q: quit): {Style.RESET_ALL}").strip()
            if choice.lower() == 'q':
                return None
            elif choice.lower() == 'm':
                manual_input = input(f"{Fore.CYAN}Enter folder name manually: {Style.RESET_ALL}").strip()
                if os.path.exists(os.path.join(os.path.dirname(__file__), manual_input)):
                    return manual_input
                else:
                    print_error("Folder does not exist.")
                    continue
            choice = int(choice)
            if 1 <= choice <= len(dav_folders):
                return dav_folders[choice - 1]
            else:
                print_error("Invalid selection. Please try again.")
        except ValueError:
            print_error("Please enter a valid number.")

# --- Menu 1: induction_error ---
def menu1_induction_error():
    csv_file = select_csv_file()
    if not csv_file:
        return
    
    cell_num_input = input(f"{Fore.CYAN}Enter cell number (e.g., 1): {Style.RESET_ALL}").strip()
    cell_str = f"C{cell_num_input}"
    nvr = get_nvr_address_from_cell(cell_num_input)
    if not nvr:
        return

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
                print_error("Invalid channel number.")
        except ValueError:
            print_error("Please enter a number.")
    
    folder_name = f"induction_error_{os.path.splitext(os.path.basename(csv_file))[0]}_dav"
    csv_file_path = os.path.join(os.path.dirname(__file__), csv_file)
    failed_downloads_file = os.path.join(os.path.dirname(__file__), f"{folder_name}_failed_downloads.txt")
    if os.path.exists(failed_downloads_file):
        os.remove(failed_downloads_file)

    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        with ThreadPoolExecutor(max_workers=4) as download_executor:
            tasks = []
            for row in reader:
                induction_time_str = row["induction_error_time"]
                start_timestamp = datetime.fromisoformat(induction_time_str.replace("Z", "+00:00")) - timedelta(seconds=3)
                end_timestamp = start_timestamp + timedelta(seconds=30)
                start_time_str = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                vid_name = f"{start_time_str.replace(' ', '_')}_{cell_str}_{channel}"
                print_info(f"Processing: {vid_name}")
                tasks.append(download_executor.submit(get_video, nvr, start_time_str, end_time_str, vid_name, folder_name, channel, failed_downloads_file))
            for t in tasks:
                t.result()
    print_success("Induction error download complete!")

# --- Menu 2: item_lost ---
def menu2_item_lost():
    csv_file = select_csv_file()
    if not csv_file:
        return
    
    cell_num_input = input(f"\n{Fore.CYAN}Enter cell number (e.g., 1): {Style.RESET_ALL}").strip()
    cell_str = f"C{cell_num_input}"
    nvr = get_nvr_address_from_cell(cell_num_input)
    if not nvr:
        return

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
                print_error("Invalid channel number.")
        except ValueError:
            print_error("Please enter a number.")
    
    folder_name = f"item_lost_{os.path.splitext(os.path.basename(csv_file))[0]}_dav"
    csv_file_path = os.path.join(os.path.dirname(__file__), csv_file)
    failed_downloads_file = os.path.join(os.path.dirname(__file__), f"{folder_name}_failed_downloads.txt")
    if os.path.exists(failed_downloads_file):
        os.remove(failed_downloads_file)

    with open(csv_file_path, newline='') as csvfile:
        reader = list(csv.DictReader(csvfile, delimiter=','))
        if not reader:
            print_error("CSV for item_lost requires at least 1 row.")
            return
        with ThreadPoolExecutor(max_workers=4) as download_executor:
            tasks = []
            for row in reader:
                timestamp_col_name = None
                for col_name in ['timestamp', 'held_start_time', next(iter(row), None)]:
                    if col_name and col_name in row:
                        timestamp_col_name = col_name
                        break
                if not timestamp_col_name:
                    print_error(f"Skipping row due to missing timestamp column: {row}")
                    continue
                ts_str = row[timestamp_col_name]
                try:
                    ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except Exception as e:
                    print_error(f"Skipping row due to invalid timestamp format '{ts_str}': {e}")
                    continue
                start_timestamp = ts_dt - timedelta(seconds=50)
                end_timestamp = ts_dt + timedelta(seconds=0)
                start_time_str = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                row_time_for_name = ts_dt.strftime('%Y-%m-%d_%H-%M-%S')
                vid_name = f"{row_time_for_name}_{cell_str}_{channel}"
                print_info(f"Processing: {vid_name}")
                tasks.append(download_executor.submit(get_video, nvr, start_time_str, end_time_str, vid_name, folder_name, channel, failed_downloads_file))
            for t in tasks:
                t.result()
    print_success("Item lost download complete!")

# --- Menu 3: bagger_video ---
def menu3_bagger_video():
    csv_file = select_csv_file()
    if not csv_file:
        return
    
    cell_num_input = input(f"{Fore.CYAN}Enter cell number (e.g., 1): {Style.RESET_ALL}").strip()
    cell_str = f"C{cell_num_input}"
    nvr = get_nvr_address_from_cell(cell_num_input)
    if not nvr:
        return

    while True:
        try:
            channel = input(f"\n{Fore.CYAN}Select channel (1-4, default: 3): {Style.RESET_ALL}").strip()
            if not channel:
                channel = 3
                break
            channel = int(channel)
            if 1 <= channel <= 4:
                break
            else:
                print_error("Invalid channel number.")
        except ValueError:
            print_error("Please enter a number.")

    bagger_code_input = input(f"{Fore.CYAN}Enter bagger_code(s) (comma separated, press Enter for all): {Style.RESET_ALL}").strip()
    if bagger_code_input:
        bagger_code_filter = [code.strip() for code in bagger_code_input.split(",") if code.strip()]
    else:
        bagger_code_filter = None

    folder_name = f"bagger_video_{os.path.splitext(os.path.basename(csv_file))[0]}_dav"
    csv_file_path = os.path.join(os.path.dirname(__file__), csv_file)
    failed_downloads_file = os.path.join(os.path.dirname(__file__), f"{folder_name}_failed_downloads.txt")
    if os.path.exists(failed_downloads_file):
        os.remove(failed_downloads_file)

    with open(csv_file_path, newline='') as csvfile:
        reader = list(csv.DictReader(csvfile, delimiter=','))
        if not reader:
            print_error("CSV for bagger video requires at least 1 row.")
            return
        with ThreadPoolExecutor(max_workers=4) as download_executor:
            tasks = []
            for row in reader:
                row_bagger_code = row.get("bagger_code", "")
                if bagger_code_filter and row_bagger_code not in bagger_code_filter:
                    continue
                utc_time_str = row["utc_time"]
                try:
                    utc_dt = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
                except Exception as e:
                    print_error(f"Skipping row due to invalid utc_time format '{utc_time_str}': {e}")
                    continue
                start_timestamp = utc_dt - timedelta(seconds=10)
                end_timestamp = utc_dt + timedelta(seconds=20)
                start_time_str = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                vid_name = f"{cell_str}_{utc_time_str}_ErrorCode_{row_bagger_code}"
                print_info(f"Processing: {vid_name}")
                tasks.append(download_executor.submit(get_video, nvr, start_time_str, end_time_str, vid_name, folder_name, channel, failed_downloads_file))
            for t in tasks:
                t.result()
    print_success("Bagger video download complete!")

# --- Menu C: Convert DAV to MP4 ---
def menu_convert_to_mp4():
    dav_folder = select_dav_folder()
    if not dav_folder:
        return
    dav_folder_path = os.path.join(os.path.dirname(__file__), dav_folder)
    
    if dav_folder.endswith("_dav"):
        mp4_folder = dav_folder.replace("_dav", "_mp4")
    else:
        ch_match = re.search(r'_ch\d+', dav_folder)
        if ch_match:
            channel_part = ch_match.group(0)
            base_name = dav_folder.replace(channel_part, '')
            mp4_folder = f"{base_name}{channel_part}_mp4"
        else:
            mp4_folder = f"{dav_folder}_mp4"
            
    mp4_folder_path = os.path.join(os.path.dirname(__file__), mp4_folder)
    create_folder(mp4_folder_path)
    
    dav_files = [f for f in os.listdir(dav_folder_path) if f.endswith('.dav')]
    if not dav_files:
        print_error(f"No DAV files found in {dav_folder}.")
        return
    
    print_info(f"Starting conversion of {len(dav_files)} DAV files...")
    with ProcessPoolExecutor(max_workers=3) as convert_executor:
        for dav_file in dav_files:
            input_file = os.path.join(dav_folder_path, dav_file)
            print_info(f"Converting: {dav_file}")
            convert_executor.submit(convert_video, input_file, mp4_folder_path)
    print_success("MP4 conversion complete!")

# --- New Menu 4: Download videos from Google Logs (Bagger Error) ---
def menu4_download_from_logs_bagger_error():
    project_id = "osaro-logging" # Fixed project ID

    # 1. Get Time Range from User
    print("\nTime range selection:")
    print("a. Automatic daily time range (previous day 23:00 to current day 19:30)")
    print("b. Manual time input")
    
    while True:
        time_choice = input(f"{Fore.CYAN}Select time input method (a/b): {Style.RESET_ALL}").strip().lower()
        if time_choice == 'a':
            # Automatic daily time range
            while True:
                date_input = input(f"{Fore.CYAN}Enter date (YYYY-MM-DD, e.g., 2025-06-25): {Style.RESET_ALL}").strip()
                try:
                    # Parse the date
                    date_obj = datetime.strptime(date_input, '%Y-%m-%d').date()
                    
                    # Calculate time range: previous day 23:00 to current day 19:30
                    start_dt_utc = datetime(date_obj.year, date_obj.month, date_obj.day, 23, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)
                    end_dt_utc = datetime(date_obj.year, date_obj.month, date_obj.day, 19, 30, 0, tzinfo=timezone.utc)
                    
                    print_info(f"Automatic time range: {start_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} to {end_dt_utc.strftime('%Y-%m-%d %H:%M:%S')}")
                    break
                except ValueError:
                    print_error("Invalid date format. Please use YYYY-MM-DD.")
            break
        elif time_choice == 'b':
            # Manual time input
            while True:
                start_time_input = input(f"{Fore.CYAN}Enter start time (YYYY-MM-DD HH:MM:SS, e.g., 2025-06-24 04:00:00): {Style.RESET_ALL}").strip()
                end_time_input = input(f"{Fore.CYAN}Enter end time (YYYY-MM-DD HH:MM:SS, e.g., 2025-06-24 08:00:00): {Style.RESET_ALL}").strip()
                try:
                    start_dt_utc = datetime.strptime(start_time_input, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    end_dt_utc = datetime.strptime(end_time_input, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    if start_dt_utc >= end_dt_utc:
                        print_error("Start time must be before end time.")
                        continue
                    break
                except ValueError:
                    print_error("Invalid date/time format. Please use YYYY-MM-DD HH:MM:SS.")
            break
        else:
            print_error("Invalid choice. Please enter 'a' or 'b'.")

    start_log_time_str = start_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_log_time_str = end_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    # 2. Get Cell Number from User
    while True:
        cell_num_input = input(f"{Fore.CYAN}Enter cell number (1-7, e.g., 5): {Style.RESET_ALL}").strip()
        hostname = get_hostname_from_cell_number(cell_num_input)
        if hostname:
            break
        
    # 3. Get Channel from User
    while True:
        try:
            channel = input(f"{Fore.CYAN}Select channel (1-4, default: 1): {Style.RESET_ALL}").strip()
            if not channel:
                channel = 1
                break
            channel = int(channel)
            if 1 <= channel <= 4:
                break
            else:
                print_error("Invalid channel number.")
        except ValueError:
            print_error("Please enter a number.")

    # 4. Get Optional Bagger Error Code(s)
    bagger_error_codes = None
    while True:
        choice = input(f"{Fore.CYAN}Do you want to filter by bagger error code? (yes/no): {Style.RESET_ALL}").strip().lower()
        if choice == 'yes':
            code_input = input(f"{Fore.CYAN}Enter bagger error code(s) (comma separated, e.g., 52,47): {Style.RESET_ALL}").strip()
            try:
                # Parse multiple codes
                bagger_error_codes = [int(code.strip()) for code in code_input.split(",") if code.strip()]
                if not bagger_error_codes:
                    print_error("No valid error codes entered.")
                    continue
                print_info(f"Filtering for error codes: {bagger_error_codes}")
                break
            except ValueError:
                print_error("Invalid error code format. Please enter numbers separated by commas.")
        elif choice == 'no':
            break
        else:
            print_error("Invalid input. Please enter 'yes' or 'no'.")

    # Construct the log query filter
    log_filter = build_log_query_filter(start_log_time_str, end_log_time_str, hostname, bagger_error_codes)

    # Fetch timestamps from logs
    log_entries = fetch_timestamps_from_logs(project_id, log_filter)

    if not log_entries:
        print_warning("No matching log entries found for the given criteria.")
        return

    # Determine target folder name
    base_dir = os.path.dirname(__file__)
    folder_suffix = f"ch{channel}"
    
    # Add start date and time to folder name for better organization
    start_date_time_str = start_dt_utc.strftime('%Y%m%d_%H%M%S')
    
    if bagger_error_codes:
        # Create separate folders for each error code
        error_code_folders = {}
        for code in bagger_error_codes:
            folder_name = f"bagger_error_{code}_{hostname.replace('cpg-ech02-', 'cell')}_{folder_suffix}_{start_date_time_str}_dav"
            target_folder_path = os.path.join(base_dir, folder_name)
            create_folder(target_folder_path)
            error_code_folders[code] = target_folder_path
    else:
        # Single folder for all errors
        folder_name = f"bagger_error_all_{hostname.replace('cpg-ech02-', 'cell')}_{folder_suffix}_{start_date_time_str}_dav"
        target_folder_path = os.path.join(base_dir, folder_name)
        create_folder(target_folder_path)
        error_code_folders = {None: target_folder_path}

    failed_downloads_file = os.path.join(base_dir, f"bagger_error_failed_downloads_{start_date_time_str}.txt")
    if os.path.exists(failed_downloads_file):
        os.remove(failed_downloads_file)
    
    download_tasks = []
    successful_downloads_count = 0

    print_info(f"Found {len(log_entries)} log entries. Initiating video downloads...")
    with ThreadPoolExecutor(max_workers=4) as download_executor:
        for timestamp_dt, hostname_from_log, bagger_code_from_log in log_entries:
            # Video time range: -10s to +10s from log timestamp
            video_start_dt = timestamp_dt - timedelta(seconds=10)
            video_end_dt = timestamp_dt + timedelta(seconds=10)

            video_start_time_str = video_start_dt.strftime('%Y-%m-%d %H:%M:%S')
            video_end_time_str = video_end_dt.strftime('%Y-%m-%d %H:%M:%S')

            # Use the cell number from user input for NVR address lookup
            nvr_address = get_nvr_address_from_cell(cell_num_input) 
            if not nvr_address:
                print_error(f"Could not determine NVR address for cell {cell_num_input}. Skipping video for {timestamp_dt}.")
                continue

            # Determine target folder based on bagger code
            if bagger_error_codes:
                # Find the matching error code folder
                target_folder_path = None
                for code in bagger_error_codes:
                    if str(code) == bagger_code_from_log:
                        target_folder_path = error_code_folders[code]
                        break
                if not target_folder_path:
                    print_warning(f"No matching folder for bagger code {bagger_code_from_log}. Skipping video for {timestamp_dt}.")
                    continue
            else:
                # Use the single folder for all errors
                target_folder_path = error_code_folders[None]

            # Construct video name: hostname_timestamp_baggercode_chX
            # Ensure hostname_from_log is clean for filename
            clean_hostname = hostname_from_log.replace('cpg-ech02-', 'cell')
            vid_name = f"{clean_hostname}_{timestamp_dt.strftime('%Y%m%d_%H%M%S')}_ErrorCode_{bagger_code_from_log}_ch{channel}".strip("_")
            
            print_info(f"Queueing download for {vid_name} from {nvr_address} (Time: {video_start_time_str} to {video_end_time_str})")
            future = download_executor.submit(get_video, nvr_address, video_start_time_str, video_end_time_str, 
                                            vid_name, target_folder_path, channel, failed_downloads_file)
            download_tasks.append(future)

        for future in download_tasks:
            try:
                if future.result() is True:
                    successful_downloads_count += 1
            except Exception as e:
                print_error(f"An error occurred during video download: {e}")

    # Final Summary
    total_logs_found = len(log_entries)
    download_failures = count_lines_in_file(failed_downloads_file)
    
    print_success("\nGoogle Logs-based download process completed!")
    print_info(f"\n{'='*20} Summary {'='*20}")
    print_info(f"Total log entries found: {total_logs_found}")
    print_success(f"Successful downloads: {successful_downloads_count}")
    print_error(f"Download failures: {download_failures}")
    if (successful_downloads_count + download_failures) != total_logs_found:
        print_warning("Note: Discrepancy between logs found and downloads attempted/failed. Some entries might have been skipped due to NVR address issues or other errors before download.")
    print_info(f"{'='*50}")

def convert_video(input_file, output_folder):
    output_file = os.path.join(output_folder, os.path.basename(input_file).replace(".dav", ".mp4"))
    command = ["ffmpeg", "-y", "-i", input_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", output_file]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(output_file):
        print(f"Conversion complete: {output_file}")
    else:
        print(f"Conversion failed: {input_file}")

# --- Main Loop ---
def main():
    while True:
        print(f"\n{Fore.CYAN}{'='*50}")
        print(f"{Fore.YELLOW}=== Item Stuck Video Menu ===")
        print(f"{Fore.CYAN}{'='*50}")
        print(f"{Fore.GREEN}1. Download induction_error videos")
        print(f"{Fore.MAGENTA}2. Download item_lost videos")
        print(f"{Fore.YELLOW}3. Download bagger videos")
        print(f"{Fore.BLUE}4. Download videos from Google Logs (Bagger Error)")
        print(f"{Fore.BLUE}c. Convert DAV to MP4")
        print(f"{Fore.RED}q. Exit")
        print(f"{Fore.CYAN}{'='*50}{Style.RESET_ALL}")

        choice = input(f"{Fore.CYAN}Select menu (1, 2, 3, 4, c, q): {Style.RESET_ALL}").strip().lower()
        if choice == 'q':
            print_info("Exiting program!")
            break
        elif choice == "1":
            menu1_induction_error()
        elif choice == "2":
            menu2_item_lost()
        elif choice == "3":
            menu3_bagger_video()
        elif choice == "4":
            menu4_download_from_logs_bagger_error()
        elif choice == "c":
            menu_convert_to_mp4()
        else:
            print_error("Invalid selection.")

if __name__ == '__main__':
    main()
