'''
How to use
1. Locate the CSV file in the same place as the script. The CSV file should have only one sheet. 
    Also need to install ffmpeg for video conversion by 'brew install ffmpeg'
2. Run 'python3 CS_video_downloader.py --csv_file "csv file name". Then it'll automatically download the videos in the same folder from -30s to +10s of the shippedat time.
** There are some options, 
    --pre_seconds : For setting the start time before shippedat time
    --post_seconds : For setting the end time after shippedat time
    --channel : For setting the amcrest channel number. Default is 1
3. Currently, the script is set to download videos in parallel. If you want to download videos sequentially, please run the commented code at the bottom of the script.
(It might take some time to download all the videos and converting those, so please be patient)
'''

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

def convert_video(input_file):
    output_file = input_file.replace(".dav", ".mp4")
    command = ["ffmpeg", "-y", "-i", input_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", output_file]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(output_file):
        print(f"Conversion complete: {output_file}")
        os.remove(input_file)  # Remove the original .dav file
    else:
        print(f"Conversion failed for {input_file}")

def get_video(nvrname, start_time, end_time, vid_name, folder_name, channel=2):
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
                
                return file_path  # Return file path for parallel conversion
            else:
                print("Error", response.status_code)
    except Exception as e:
        print(f"An error occurred: {e}")
    return None

def main():
    parser = argparse.ArgumentParser(description="Download video clips based on shippedat time from a CSV file.")
    parser.add_argument("--csv_file", required=True, help="Path to the CSV file")
    parser.add_argument("--pre_seconds", type=int, default=25, help="Seconds before shippedat time to start the video")
    parser.add_argument("--post_seconds", type=int, default=15, help="Seconds after shippedat time to end the video")
    parser.add_argument("--channel", type=int, default=1, help="NVR channel number (default: 1)")
    args = parser.parse_args()
    
    csv_file_path = args.csv_file
    pre_seconds = args.pre_seconds
    post_seconds = args.post_seconds
    channel = args.channel
    
    folder_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    create_folder(folder_name)
    
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        
        download_tasks = []
        with ThreadPoolExecutor(max_workers=3) as download_executor, ProcessPoolExecutor(max_workers=3) as convert_executor:
            for row in reader:
                shippedat_str = row.get("shippedat")
                worker_id = row.get("pack_workerid", "")
                invoice_number = row.get("invoicenumber", "")
                
                if not shippedat_str:
                    continue
                
                shippedat_obj = datetime.strptime(shippedat_str, '%m/%d/%y %H:%M:%S') - timedelta(hours=9)  # Convert KST to UTC
                start_time = shippedat_obj - timedelta(seconds=pre_seconds)
                end_time = shippedat_obj + timedelta(seconds=post_seconds)
                
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
                
                short_shippedat = shippedat_obj.strftime('%Y%m%d_%H%M%S')
                cell_info = f"cell{worker_id[-1]}" if worker_id.startswith("rw_136_robotagent") else ""
                vid_name = f"{invoice_number}_{folder_name}_{short_shippedat}_{cell_info}_ch{channel}".strip("_")
                
                nvr_address = get_nvr_address(worker_id)
                if not nvr_address:
                    print(f"Skipping {shippedat_str}: Unknown NVR address for {worker_id}")
                    continue
                
                print(f"Processing {shippedat_str} -> {start_time_str} to {end_time_str} on {nvr_address}")
                
                future = download_executor.submit(get_video, nvr_address, start_time_str, end_time_str, vid_name, folder_name, channel)
                download_tasks.append(future)
            
            for future in download_tasks:
                file_path = future.result()
                if file_path:
                    convert_executor.submit(convert_video, file_path)

if __name__ == '__main__':
    main()


# Please run the below code if you want to download videos sequentially or have another issue
'''
# Sequential video downloader
import csv
import os
import time
import subprocess
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPDigestAuth
from tqdm import tqdm
import argparse

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

def convert_video(input_file):
    output_file = input_file.replace(".dav", ".mp4")
    command = ["ffmpeg", "-y", "-i", input_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", output_file]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(output_file):
        print(f"Conversion complete: {output_file}")
        os.remove(input_file)  # Remove the original .dav file
    else:
        print(f"Conversion failed for {input_file}")

def get_video(nvrname, start_time, end_time, vid_name, folder_name, channel=2):
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
                
                # Convert to MP4
                convert_video(file_path)
            else:
                print("Error", response.status_code)
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    parser = argparse.ArgumentParser(description="Download video clips based on shippedat time from a CSV file.")
    parser.add_argument("--csv_file", required=True, help="Path to the CSV file")
    parser.add_argument("--pre_seconds", type=int, default=25, help="Seconds before shippedat time to start the video")
    parser.add_argument("--post_seconds", type=int, default=15, help="Seconds after shippedat time to end the video")
    parser.add_argument("--channel", type=int, default=2, help="NVR channel number (default: 2)")
    args = parser.parse_args()
    
    csv_file_path = args.csv_file
    pre_seconds = args.pre_seconds
    post_seconds = args.post_seconds
    channel = args.channel
    
    folder_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    create_folder(folder_name)
    
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        
        for row in reader:
            shippedat_str = row.get("shippedat")
            worker_id = row.get("pack_workerid", "")
            invoice_number = row.get("invoicenumber", "")
            
            if not shippedat_str:
                continue
            
            shippedat_obj = datetime.strptime(shippedat_str, '%m/%d/%y %H:%M:%S') - timedelta(hours=9)  # Convert KST to UTC
            start_time = shippedat_obj - timedelta(seconds=pre_seconds)
            end_time = shippedat_obj + timedelta(seconds=post_seconds)
            
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
            
            short_shippedat = shippedat_obj.strftime('%Y%m%d_%H%M%S')
            cell_info = f"cell{worker_id[-1]}" if worker_id.startswith("rw_136_robotagent") else ""
            vid_name = f"{invoice_number}_{folder_name}_{short_shippedat}_{cell_info}_ch{channel}".strip("_")
            
            nvr_address = get_nvr_address(worker_id)
            if not nvr_address:
                print(f"Skipping {shippedat_str}: Unknown NVR address for {worker_id}")
                continue
            
            print(f"Processing {shippedat_str} -> {start_time_str} to {end_time_str} on {nvr_address}")
            
            get_video(nvr_address, start_time_str, end_time_str, vid_name, folder_name, channel)

if __name__ == '__main__':
    main()
'''