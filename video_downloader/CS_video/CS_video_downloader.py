# Parallel video downloader
import csv
import os
import time
import subprocess
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPDigestAuth
from tqdm import tqdm
import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import structlog
import colorama
from colorama import Fore, Back, Style

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

def convert_video(input_file, output_folder):
    output_file = os.path.join(output_folder, os.path.basename(input_file).replace(".dav", ".mp4"))
    command = ["ffmpeg", "-y", "-i", input_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", output_file]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(output_file):
        print(f"Conversion complete: {output_file}")
    else:
        print(f"Conversion failed for {input_file}")

def get_video(nvrname, start_time, end_time, vid_name, folder_name, channel=2, failed_downloads_file=None):
    if not nvrname:
        print("Invalid NVR address. Skipping download.")
        return
    
    url = f"http://{nvrname}/cgi-bin/loadfile.cgi?action=startLoad&channel={channel}&startTime={start_time}&endTime={end_time}"
    username = "admin"
    password = "osaro51423"
    
    print(f"Downloading video: {vid_name}")
    
    try:
        with requests.get(url, auth=HTTPDigestAuth(username, password), stream=True) as response:
            if response.status_code == 200:
                file_size = int(response.headers.get('content-length', 0))
                progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True)
                
                create_folder(folder_name)
                file_path = os.path.join(folder_name, f"{vid_name}.dav")
                
                with open(file_path, 'wb') as out_file:
                    for chunk in response.iter_content(chunk_size=4096):
                        progress_bar.update(len(chunk))
                        out_file.write(chunk)
                progress_bar.close()
                print("Download complete")
            else:
                print(f"Error {response.status_code}: {response.text}")
                if failed_downloads_file:
                    invoice_number = vid_name.split('_')[0]  # Extract invoice number from vid_name
                    with open(failed_downloads_file, 'a') as f:
                        f.write(f"{invoice_number}\n")
    except Exception as e:
        print(f"An error occurred: {e}")
        if failed_downloads_file:
            invoice_number = vid_name.split('_')[0]  # Extract invoice number from vid_name
            with open(failed_downloads_file, 'a') as f:
                f.write(f"{invoice_number}\n")

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
    print(f"{Fore.GREEN}1. Download DAV files from CSV")
    print(f"{Fore.BLUE}2. Convert DAV files to MP4")
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
            channel = input(f"\n{Fore.CYAN}Select channel (1-4, default: 1): {Style.RESET_ALL}").strip()
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
    folder_name = f"{os.path.splitext(os.path.basename(csv_file))[0]}"
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
                start_time = shippedat_obj - timedelta(seconds=25)
                end_time = shippedat_obj + timedelta(seconds=15)
                
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
                
                short_shippedat = shippedat_obj.strftime('%Y%m%d_%H%M%S')
                cell_info = f"cell{worker_id[-1]}" if worker_id.startswith("rw_136_robotagent") else ""
                vid_name = f"{invoice_number}_{os.path.splitext(os.path.basename(csv_file))[0]}_{short_shippedat}_{cell_info}_ch{channel}".strip("_")
                
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

def menu2_convert_to_mp4():
    dav_folder = select_dav_folder()
    if not dav_folder:
        return
    
    dav_folder_path = os.path.join(os.path.dirname(__file__), dav_folder)
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
        choice = input(f"{Fore.CYAN}Select menu (1-2 or 'q'): {Style.RESET_ALL}").strip()
        
        if choice.lower() == 'q':
            print_info("Exiting program...")
            break
        elif choice == "1":
            menu1_download_dav()
        elif choice == "2":
            menu2_convert_to_mp4()
        else:
            print_error("Invalid selection. Please try again.")

if __name__ == '__main__':
    main()
