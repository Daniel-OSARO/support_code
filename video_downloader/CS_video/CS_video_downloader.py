# Parallel video downloader
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
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name

def get_nvr_address(worker_id):
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
            # Assuming a direct mapping like cpg-ech02-1, cpg-ech02-2, etc.
            # Adjust the prefix if needed (e.g., "cpg-ech02-")
            return f"cpg-ech02-{agent_number}"
        except ValueError:
            print_error(f"Could not parse agent number from worker ID: {worker_id}")
            return None
    print_warning(f"Could not determine hostname for worker ID: {worker_id}")
    return None

def get_log_time_range(date_obj):
    """Calculates the UTC start and end time strings for log querying based on the shipped date.
    Returns an extended range: previous day 23:00 UTC to the *next* day 19:30 UTC.
    """
    # Query logs from the previous day 23:00 UTC to the *next* day 19:30 UTC
    # date_obj is assumed to be a date object (e.g., from 4/10/25)
    start_time_dt = datetime(date_obj.year, date_obj.month, date_obj.day, 23, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)
    end_time_dt = datetime(date_obj.year, date_obj.month, date_obj.day, 19, 30, 0, tzinfo=timezone.utc) + timedelta(days=1)

    # Format as ISO 8601 strings with 'Z' for UTC (using timezone aware objects)
    start_time_str = start_time_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_str = end_time_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return start_time_str, end_time_str

def build_log_query_filter(start_time_str, end_time_str, hostname, invoice_numbers):
    """Builds the filter string for the Cloud Logging API."""
    # Base query parts from print_time_analysis.ipynb and user description
    base_query = """
    jsonPayload.app_name="osaro-pnp"
    severity>="INFO"
    jsonPayload.fields.message="issuing Coupang WMS SubmitOutbound request"
    """
    # Add hostname and timestamp filters
    filter_parts = [
        f'timestamp >= "{start_time_str}"',
        f'timestamp <= "{end_time_str}"',
        f'jsonPayload.hostname="{hostname}"',
        base_query
    ]

    # Add invoice number filter if provided
    if invoice_numbers:
        # Create OR conditions for multiple invoice numbers
        invoice_filters = [f'jsonPayload.fields.request:"{inv}"' for inv in invoice_numbers]
        invoice_query = " OR ".join(invoice_filters)
        filter_parts.append(f"({invoice_query})")

    return " AND ".join(filter_parts)

def fetch_timestamps_from_logs(project_id, filter_str):
    """Queries Cloud Logging and extracts invoice timestamps."""
    client = cloud_logging.Client(project=project_id)
    logger.info(f"Executing Cloud Logging query with filter:\n{filter_str}")
    
    invoice_timestamps = {}
    try:
        # Use list_entries for potentially large result sets
        entries = client.list_entries(filter_=filter_str, order_by=cloud_logging.DESCENDING) 
        
        for entry in entries:
            # Extract invoice number from the request payload
            try:
                # Assuming the request field is a JSON string
                request_payload = json.loads(entry.payload['fields']['request'])
                invoice_number = request_payload.get("invoiceNumber")
                
                if invoice_number and invoice_number not in invoice_timestamps:
                    # Parse timestamp string to datetime object (ensure UTC)
                    timestamp_str = entry.timestamp.isoformat()
                    if not timestamp_str.endswith('Z') and '+' not in timestamp_str:
                         timestamp_str += 'Z' # Assume UTC if no timezone info
                    timestamp_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    
                    invoice_timestamps[invoice_number] = timestamp_dt
                    logger.info(f"Found timestamp for {invoice_number}: {timestamp_dt}")

            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.warning(f"Could not parse invoice/timestamp from log entry: {e} - Entry: {entry.payload}")
                continue
                
    except Exception as e:
        print_error(f"Error querying Cloud Logging: {e}")
        return None # Indicate failure

    return invoice_timestamps

def convert_video(input_file, output_folder):
    output_file = os.path.join(output_folder, os.path.basename(input_file).replace(".dav", ".mp4"))
    command = ["ffmpeg", "-y", "-i", input_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", output_file]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(output_file):
        print(f"Conversion complete: {output_file}")
    else:
        print(f"Conversion failed for {input_file}")

def get_video(nvrname, start_time, end_time, vid_name, target_folder_path, channel=1, failed_downloads_file=None, downloaded_log_file=None):
    """Downloads a video segment to the specified target folder.
    Logs successful downloads to downloaded_log_file if provided.
    """
    if not nvrname:
        print_error("Invalid NVR address provided to get_video. Skipping download.")
        # Optionally write to failed log here if invoice number is available/passed
        return
    
    # Ensure the target directory exists before downloading
    try:
        create_folder(target_folder_path)
    except OSError as e:
        print_error(f"Could not create target folder {target_folder_path}: {e}. Skipping download for {vid_name}")
        # Log failure if possible
        if failed_downloads_file:
            try:
                invoice_number = vid_name.split('_')[0] 
                with open(failed_downloads_file, 'a') as f: f.write(f"{invoice_number}\n")
            except IndexError:
                 print_warning(f"Could not extract invoice number from {vid_name} for failure log.")
        return

    url = f"http://{nvrname}/cgi-bin/loadfile.cgi?action=startLoad&channel={channel}&startTime={start_time}&endTime={end_time}"
    username = "admin"
    password = "osaro51423"
    
    # Use print_info consistently
    print_info(f"Downloading video: {vid_name} to {target_folder_path}")
    
    try:
        # Timeout added for robustness
        with requests.get(url, auth=HTTPDigestAuth(username, password), stream=True, timeout=60) as response: 
            if response.status_code == 200:
                file_size = int(response.headers.get('content-length', 0))
                progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True, desc=vid_name, leave=False)
                
                # File path uses the target_folder_path directly
                file_path = os.path.join(target_folder_path, f"{vid_name}.dav")
                
                with open(file_path, 'wb') as out_file:
                    for chunk in response.iter_content(chunk_size=8192): # Slightly larger chunk size
                        if chunk: # filter out keep-alive new chunks
                            progress_bar.update(len(chunk))
                            out_file.write(chunk)
                progress_bar.close()
                if file_size != 0 and progress_bar.n != file_size:
                    print_warning(f"Download incomplete for {vid_name}. Expected {file_size}, got {progress_bar.n}")
                    if failed_downloads_file:
                         invoice_number = vid_name.split('_')[0]
                         with open(failed_downloads_file, 'a') as f: f.write(f"{invoice_number}\n")
                else:
                    print_success(f"Download complete: {file_path}") # Use success print
                    if downloaded_log_file:
                        try:
                            invoice_number = vid_name.split('_')[0] 
                            with open(downloaded_log_file, 'a') as log_f:
                                log_f.write(f"{invoice_number}\n")
                        except IndexError:
                            print_warning(f"Could not extract invoice number from {vid_name} for downloaded log.")
                        except IOError as e:
                             print_error(f"Could not write to downloaded log file {downloaded_log_file}: {e}")
            else:
                print_error(f"Error {response.status_code} downloading {vid_name}: {response.text}")
                if failed_downloads_file:
                    invoice_number = vid_name.split('_')[0]
                    with open(failed_downloads_file, 'a') as f:
                        f.write(f"{invoice_number}\n")
    except requests.exceptions.Timeout:
        print_error(f"Timeout occurred while downloading {vid_name} from {nvrname}")
        if failed_downloads_file:
            invoice_number = vid_name.split('_')[0]
            with open(failed_downloads_file, 'a') as f: f.write(f"{invoice_number}\n")
    except requests.exceptions.RequestException as e:
        print_error(f"An error occurred during download request for {vid_name}: {e}")
        if failed_downloads_file:
            invoice_number = vid_name.split('_')[0]
            with open(failed_downloads_file, 'a') as f: f.write(f"{invoice_number}\n")
    except Exception as e:
        print_error(f"An unexpected error occurred during download/saving of {vid_name}: {e}")
        if failed_downloads_file:
            try:
                invoice_number = vid_name.split('_')[0] 
                with open(failed_downloads_file, 'a') as f: f.write(f"{invoice_number}\n")
            except IndexError:
                 print_warning(f"Could not extract invoice number from {vid_name} for unexpected error log.")

# --- Helper function to count lines in a file safely ---
def count_lines_in_file(filepath):
    """Counts the number of lines in a file, returning 0 if file doesn't exist."""
    try:
        with open(filepath, 'r') as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0
    except Exception as e:
        print_error(f"Error counting lines in {filepath}: {e}")
        return 0 # Indicate error or inability to count
# --- End helper function ---

def get_csv_files():
    current_dir = os.path.dirname(__file__)
    csv_files = [f for f in os.listdir(current_dir) if f.endswith('.csv')]
    return csv_files

def get_dav_folders():
    current_dir = os.path.dirname(__file__)
    folders = []
    for item in os.listdir(current_dir):
        if os.path.isdir(os.path.join(current_dir, item)):
            # Check if folder contains .dav files
            if any(f.endswith('.dav') for f in os.listdir(os.path.join(current_dir, item))):
                folders.append(item)
    return folders

def print_menu():
    print(f"\n{Fore.CYAN}{'='*50}")
    print(f"{Fore.YELLOW}=== Video Downloader Menu ===")
    print(f"{Fore.CYAN}{'='*50}")
    print(f"{Fore.GREEN}1. Download DAV files from CSV (using shippedat)")
    print(f"{Fore.MAGENTA}2. Download DAV files from CSV (using BigQuery for timestamp)")
    print(f"{Fore.BLUE}c. Convert DAV files to MP4")
    print(f"{Fore.RED}Enter 'q' to exit")
    print(f"{Fore.CYAN}{'='*50}{Style.RESET_ALL}")

def print_success(message):
    logger.info(f"{Fore.GREEN}{message}{Style.RESET_ALL}")

def print_error(message):
    logger.error(f"{Fore.RED}{message}{Style.RESET_ALL}")

def print_warning(message):
    logger.warning(f"{Fore.YELLOW}{message}{Style.RESET_ALL}")

def print_info(message):
    logger.info(f"{Fore.CYAN}{message}{Style.RESET_ALL}")

def select_csv_file():
    csv_files = get_csv_files()
    if not csv_files:
        print_error("No CSV files found in the current directory.")
        return None
    
    print("\nAvailable CSV files:")
    for i, file in enumerate(csv_files, 1):
        print(f"{Fore.GREEN}{i}. {file}{Style.RESET_ALL}")
    
    while True:
        try:
            choice = input(f"\n{Fore.CYAN}Enter the number of the CSV file (or 'm' for manual input, 'q' to quit): {Style.RESET_ALL}").strip()
            if choice.lower() == 'q':
                return None
            elif choice.lower() == 'm':
                manual_input = input(f"{Fore.CYAN}Enter CSV file name manually: {Style.RESET_ALL}").strip()
                if os.path.exists(os.path.join(os.path.dirname(__file__), manual_input)) and manual_input.endswith('.csv'):
                    return manual_input
                else:
                    print_error("CSV file does not exist or is not a CSV file. Please try again.")
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
    csv_file = select_csv_file()
    if not csv_file:
        return
    
    # Channel selection
    while True:
        try:
            channel = input(f"\n{Fore.CYAN}Select channel (1-4, default: 1)\n(Recommendation) Missing: 2 and 3, Wrong: 1 : {Style.RESET_ALL}").strip()
            if not channel:  # If empty input, use default
                channel = 1
                break
            channel = int(channel)
            if 1 <= channel <= 4:
                break
            else:
                print_error("Invalid channel number")
        except ValueError:
            print_error("Invalid channel number")
    
    csv_file_path = os.path.join(os.path.dirname(__file__), csv_file)
    folder_name = f"{os.path.splitext(os.path.basename(csv_file))[0]}_ch{channel}_dav"
    failed_downloads_file = f"{folder_name}_failed_downloads.txt"
    
    # Clear previous failed downloads file if exists
    if os.path.exists(failed_downloads_file):
        os.remove(failed_downloads_file)
    
    print_info(f"\nProcessing CSV file: {csv_file}")
    print_info(f"Creating folder: {folder_name}")
    print_info(f"Selected channel: {channel}")
    
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        
        download_tasks = []
        with ThreadPoolExecutor(max_workers=4) as download_executor:
            for row in reader:
                shippedat_str = row.get("shippedat")
                worker_id = row.get("pack_workerid", "")
                invoice_number = row.get("invoicenumber", "")
                
                if not shippedat_str:
                    print_error(f"Error: No shippedat time found in row")
                    exit(1)
                
                try:
                    # Try first format: %m/%d/%Y %H:%M:%S
                    shippedat_obj = datetime.strptime(shippedat_str, '%m/%d/%Y %H:%M:%S')
                except ValueError:
                    try:
                        # Try second format: %Y.%m.%d %H:%M:%S
                        shippedat_obj = datetime.strptime(shippedat_str, '%Y.%m.%d %H:%M:%S')
                    except ValueError:
                        print_error(f"Error: Invalid shippedat format: {shippedat_str}")
                        exit(1)
                
                shippedat_obj = shippedat_obj - timedelta(hours=9)
                start_time = shippedat_obj - timedelta(seconds=30)
                end_time = shippedat_obj + timedelta(seconds=20)
                
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
                
                short_shippedat = shippedat_obj.strftime('%Y%m%d_%H%M%S')
                cell_info = f"cell{worker_id[-1]}" if worker_id.startswith("rw_136_robotagent") else ""
                vid_name = f"{invoice_number}_{short_shippedat}_{cell_info}_ch{channel}".strip("_")
                
                nvr_address = get_nvr_address(worker_id)
                if not nvr_address:
                    print_error(f"Error: Unknown NVR address for {worker_id}")
                    exit(1)
                
                print_info(f"Processing {shippedat_str} -> {start_time_str} to {end_time_str} on {nvr_address}")
                
                future = download_executor.submit(get_video, nvr_address, start_time_str, end_time_str, 
                                                vid_name, folder_name, channel, failed_downloads_file)
                download_tasks.append(future)
            
            for future in download_tasks:
                future.result()
    
    print_success("\nDownload process completed!")

def menu2_download_dav_using_logs():
    csv_file = select_csv_file()
    if not csv_file:
        return

    # Channel selection (same as menu 1)
    while True:
        try:
            channel = input(f"\n{Fore.CYAN}Select channel (1-4, default: 1)\n(Recommendation) Missing: 2 and 3, Wrong: 1 : {Style.RESET_ALL}").strip()
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

    csv_file_path = os.path.join(os.path.dirname(__file__), csv_file)
    folder_name = f"{os.path.splitext(os.path.basename(csv_file))[0]}_log_time_ch{channel}" # Distinguish folder
    failed_downloads_file = f"{folder_name}_failed_downloads.txt"
    unfound_timestamps_file = f"{folder_name}_unfound_timestamps.txt" # File for unfound invoices
    project_id = "osaro-logging" # Assuming this project ID
    base_dir = os.path.dirname(csv_file_path) # Directory containing the CSV
    csv_base_name = os.path.splitext(os.path.basename(csv_file))[0]
    # --- Add downloaded log file --- 
    downloaded_log_file = os.path.join(base_dir, f"{csv_base_name}_downloaded.txt")
    # --- End add downloaded log file ---

    if os.path.exists(failed_downloads_file):
        os.remove(failed_downloads_file)
    if os.path.exists(unfound_timestamps_file):
        os.remove(unfound_timestamps_file) # Clear previous unfound file
    # --- Clear downloaded log file --- 
    if os.path.exists(downloaded_log_file):
        os.remove(downloaded_log_file)
    # --- End clear downloaded log file ---

    print_info(f"\nProcessing CSV file: {csv_file}")
    print_info(f"Creating folder: {folder_name}")
    print_info(f"Selected channel: {channel}")
    print_info("Will use BigQuery Logs to find precise timestamps.")

    # Step 1: Read CSV and group requests by hostname, collecting dates and invoices
    requests_by_hostname = {} # Key: hostname, Value: {'invoices': [], 'dates': []}
    all_rows_data = [] # Keep original row data for later use
    single_target_folder = None # Holds path if user opts for single folder

    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        
        # --- Check for return_reason_group column --- 
        fieldnames_lower = [name.lower() for name in reader.fieldnames or []]
        reason_column_name = None
        if 'return_reason_group' in fieldnames_lower:
            # Find the original case-sensitive column name
            original_index = fieldnames_lower.index('return_reason_group')
            reason_column_name = reader.fieldnames[original_index]
            print_info(f"Found column: '{reason_column_name}'. Files will be downloaded into subfolders.")
        else:
            print_warning("Column 'return_reason_group' not found in CSV.")
            while True:
                choice = input(f"{Fore.YELLOW}Download all videos into a single folder '{csv_base_name}_all'? (yes/no): {Style.RESET_ALL}").strip().lower()
                if choice == 'yes':
                    single_target_folder = os.path.join(base_dir, f"{csv_base_name}_all")
                    print_info(f"Will download all files to: {single_target_folder}")
                    create_folder(single_target_folder) # Create the single folder immediately
                    break
                elif choice == 'no':
                    print_error("Aborting script as requested.")
                    return # Exit the menu function
                else:
                    print_error("Invalid input. Please enter 'yes' or 'no'.")
        # --- End Check --- 
                    
        for row in reader:
            shippedat_str = row.get("shippedat")
            worker_id = row.get("pack_workerid", "")
            invoice_number = row.get("invoicenumber", "")

            if not shippedat_str or not worker_id or not invoice_number:
                print_error(f"Skipping row due to missing data: {row}")
                continue

            # Try parsing various date formats, extracting only the date part
            shipped_date_obj = None
            possible_date_formats = ['%m/%d/%y', '%m/%d/%Y', '%Y.%m.%d', '%Y-%m-%d']
            date_part = shippedat_str.split(' ')[0] # Get the date part before parsing
            
            for fmt in possible_date_formats:
                try:
                    shipped_date_obj = datetime.strptime(date_part, fmt).date()
                    break # Stop trying formats if one succeeds
                except ValueError:
                    continue # Try the next format

            if shipped_date_obj is None:
                print_error(f"Error: Could not parse shippedat date format: {shippedat_str}")
                continue # Skip this row if no format matches

            hostname = get_hostname_from_worker_id(worker_id)
            if not hostname:
                print_error(f"Skipping row for worker {worker_id} - could not determine hostname.")
                continue

            # --- Grouping Logic (remains by hostname) ---
            if hostname not in requests_by_hostname:
                requests_by_hostname[hostname] = {'invoices': [], 'dates': []}
            requests_by_hostname[hostname]['invoices'].append(invoice_number)
            requests_by_hostname[hostname]['dates'].append(shipped_date_obj)
            
            # --- Store return reason (if column exists) --- 
            return_reason = None
            if reason_column_name:
                return_reason = row.get(reason_column_name, None)
            
            # Store necessary info for download later 
            all_rows_data.append({
                'invoice_number': invoice_number,
                'worker_id': worker_id,
                'shipped_date': shipped_date_obj,
                'return_reason_group': return_reason, # Store the reason
                'original_row': row, 
                'csv_filename': csv_file 
            })


    # Step 2: Query logs for each HOSTNAME group using min/max dates from that group
    all_invoice_timestamps = {}
    print_info(f"Found {len(requests_by_hostname)} hostnames to query based on CSV data.")

    for hostname, data in requests_by_hostname.items():
        invoice_list = data['invoices']
        date_list = data['dates']

        if not date_list:
            print_warning(f"No valid dates found for hostname {hostname}, skipping query.")
            continue
        
        # Find min and max dates within this hostname group
        min_date = min(date_list)
        max_date = max(date_list)

        # Calculate the overall time range based on min/max dates for this hostname
        # Range: (min_date - 1 day @ 23:00 UTC) to (max_date + 1 day @ 19:30 UTC)
        start_dt = datetime(min_date.year, min_date.month, min_date.day, 23, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)
        end_dt = datetime(max_date.year, max_date.month, max_date.day, 19, 30, 0, tzinfo=timezone.utc) + timedelta(days=1)
        start_log_time = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_log_time = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        print_info(f"Querying logs for {hostname} (Date range: {min_date} to {max_date}) "
                   f"from {start_log_time} to {end_log_time} for {len(invoice_list)} invoices...")
        
        # Build filter for the current group using the calculated overall time range
        log_filter = build_log_query_filter(start_log_time, end_log_time, hostname, invoice_list)
        
        # Fetch timestamps for this group
        timestamps = fetch_timestamps_from_logs(project_id, log_filter)
        
        if timestamps:
            all_invoice_timestamps.update(timestamps)
        else:
            print_warning(f"Could not retrieve any timestamps for hostname group: {hostname} within the searched range.")
            # Note: Individual invoice errors are handled later in the download step

    print_info(f"Finished querying logs. Found potential timestamps for {len(all_invoice_timestamps)} unique invoices across all hosts.")

    # Step 3: Prepare and execute downloads using found timestamps
    download_tasks = []
    processed_invoices = set() # Track invoices for which downloads are initiated
    unfound_invoices = [] # List to store invoices without timestamps

    with ThreadPoolExecutor(max_workers=4) as download_executor:
        for row_data in all_rows_data:
            invoice_number = row_data['invoice_number']
            worker_id = row_data['worker_id']

            if invoice_number in processed_invoices:
                 continue # Already handled this invoice (could happen with duplicates in CSV)

            timestamp_dt = all_invoice_timestamps.get(invoice_number)

            if timestamp_dt:
                # --- Determine Target Download Folder --- 
                target_folder_path = None
                if single_target_folder:
                    target_folder_path = single_target_folder
                else:
                    reason = row_data.get('return_reason_group')
                    reason_lower = reason.lower() if reason else '' # Handle None safely
                    
                    subfolder_name = ""
                    if reason_lower == 'missing':
                        subfolder_name = "Missing"
                    elif reason_lower == 'wrong':
                        subfolder_name = "Wrong"
                    else: 
                        subfolder_name = "Others" # Catches empty strings, None, and other values
                    
                    # Include CSV file name in the subfolder
                    prefix = csv_base_name
                    subfolder_name = f"{prefix}_{subfolder_name}"
                    
                    target_folder_path = os.path.join(base_dir, subfolder_name)
                # --- End Determine Target Folder ---
                    
                # Calculate download start/end times (existing logic)
                start_time = timestamp_dt - timedelta(seconds=30)
                end_time = timestamp_dt + timedelta(seconds=20)

                # Format for the download URL (original format required by NVR)
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

                # Generate video name (similar to menu 1, but using accurate time and no folder name)
                short_timestamp = timestamp_dt.strftime('%Y%m%d_%H%M%S')
                cell_info = f"cell{worker_id[-1]}" if worker_id.startswith("rw_136_robotagent") else ""
                vid_name = f"{invoice_number}_{short_timestamp}_{cell_info}_ch{channel}".strip("_")

                nvr_address = get_nvr_address(worker_id)
                if not nvr_address:
                    print_error(f"Error: Unknown NVR address for {worker_id}. Skipping invoice {invoice_number}")
                    if failed_downloads_file:
                         with open(failed_downloads_file, 'a') as f: f.write(f"{invoice_number}\n")
                    continue

                print_info(f"Queueing download for {invoice_number} ({vid_name}) to {target_folder_path} using log timestamp {timestamp_dt}")
                # Pass the determined target_folder_path and downloaded_log_file to get_video
                future = download_executor.submit(get_video, nvr_address, start_time_str, end_time_str,
                                                vid_name, target_folder_path, channel, 
                                                failed_downloads_file, downloaded_log_file)
                download_tasks.append(future)
                processed_invoices.add(invoice_number)

            else:
                print_warning(f"No timestamp found in logs for invoice {invoice_number}. Skipping download.")
                unfound_invoices.append(invoice_number) # Add to unfound list

        # Wait for all downloads to complete
        for future in download_tasks:
            future.result()

    # Step 4: Write unfound invoices to a file
    if unfound_invoices:
        print_warning(f"\n{len(unfound_invoices)} invoices could not find a timestamp in the logs.")
        try:
            with open(unfound_timestamps_file, 'w') as f:
                for inv in sorted(list(set(unfound_invoices))): # Write unique sorted list
                    f.write(f"{inv}\n")
            print_info(f"List of unfound invoices saved to: {unfound_timestamps_file}")
        except IOError as e:
            print_error(f"Could not write unfound invoices file: {e}")
    else:
        print_info("\nAll invoices processed had corresponding timestamps found in logs (or skipped due to other errors).")

    print_success("\nLog-based download process completed!")

    # --- Final Summary --- 
    print_info(f"\n{'='*20} Summary {'='*20}")
    total_csv_rows = len(all_rows_data)
    unique_invoices_in_csv = len(set(item['invoice_number'] for item in all_rows_data))
    
    successful_downloads = count_lines_in_file(downloaded_log_file)
    timestamps_not_found = count_lines_in_file(unfound_timestamps_file)
    download_failures = count_lines_in_file(failed_downloads_file)
    
    print_info(f"Total rows processed from CSV: {total_csv_rows}")
    print_info(f"Unique invoice numbers in CSV: {unique_invoices_in_csv}")
    print_success(f"Successful downloads: {successful_downloads}")
    print_warning(f"Timestamps not found in logs: {timestamps_not_found}")
    print_error(f"Download failures (NVR/Network issues): {download_failures}")
    
    # Check if counts match unique invoices
    total_accounted = successful_downloads + timestamps_not_found + download_failures
    if total_accounted == unique_invoices_in_csv:
        print_info("Counts match unique invoices processed.")
    else:
        print_warning(f"Discrepancy detected: Unique invoices ({unique_invoices_in_csv}) vs Accounted ({total_accounted})")
        print_warning("This might happen if errors occurred before the download stage (e.g., invalid worker ID) or due to unexpected issues.")
    print_info(f"{'='*50}")
    # --- End Final Summary ---

def menu2_convert_to_mp4():
    dav_folder = select_dav_folder()
    if not dav_folder:
        return
    
    dav_folder_path = os.path.join(os.path.dirname(__file__), dav_folder)
    
    if dav_folder.endswith("_dav"):
        base_folder_name = dav_folder[:-4] # Remove '_dav'
        mp4_folder = f"{base_folder_name}_mp4"
    else:
        # Check if folder name contains "_ch" followed by a number
        ch_match = re.search(r'_ch\d+', dav_folder)
        if ch_match:
            # Keep the channel info in the mp4 folder name
            channel_part = ch_match.group(0)
            base_name = dav_folder.replace(channel_part, '')
            mp4_folder = f"{base_name}{channel_part}_mp4"
        else:
            mp4_folder = f"{dav_folder}_mp4"

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

def main():
    while True:
        print_menu()
        choice = input(f"{Fore.CYAN}Select menu (1-2, c or 'q'): {Style.RESET_ALL}").strip().lower()
        
        if choice == 'q':
            print_info("Exiting program...")
            break
        elif choice == "1":
            menu1_download_dav()
        elif choice == "2":
            menu2_download_dav_using_logs()
        elif choice == "c":
            menu2_convert_to_mp4()
        else:
            print_error("Invalid selection. Please try again.")

if __name__ == '__main__':
    main()
