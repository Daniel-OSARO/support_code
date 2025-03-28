"""
1. grafana에서 csv파일 받기
2. 해당 python file과 csv파일 같은 폴더에 두기
3. python3 bagger_video_downloader.py --csv_file "csv파일 이름" --pre_seconds 5 --post_seconds 15 --cell "cell1" --nvr 192.168.111l.1x:8010 (필요시)--channel 2 --bagger_code "47", "52"
    예시 : python3 bagger_video_downloader.py --csv_file "c1_bagger_0224.csv" --pre_seconds 5 --post_seconds 15 --cell "cell1" --nvr 192.168.111.11:8010 --bagger_code "47", "52"
"""

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

def unique_filename(base_filename):
    counter = 1
    filename, file_extension = os.path.splitext(base_filename)
    new_filename = base_filename
    while os.path.exists(new_filename):
        new_filename = f"{filename}_({counter}){file_extension}"
        counter += 1
    return new_filename

def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name

def convert_video(input_file):
    output_file = input_file.replace(".dav", ".mp4")
    command = ["ffmpeg", "-y", "-i", input_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", output_file]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(output_file):
        print(f"Conversion complete: {output_file}")
        os.remove(input_file)  # Remove the original .dav file
    else:
        print(f"Conversion failed for {input_file}")

def get_video(nvrname, start_time, end_time, vid_name, folder_name, channel=3):
    url = f"http://{nvrname}/cgi-bin/loadfile.cgi?action=startLoad&channel={channel}&startTime={start_time}&endTime={end_time}"
    username = "admin"
    password = "osaro51423"
    
    start_time_obj = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time_obj = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    video_length = (end_time_obj - start_time_obj).total_seconds()
    
    print(f"Downloading video: {vid_name}")
    print(f"Video length: {video_length:.2f} seconds")
    
    max_retries, delay = 5, 5
    for attempt in range(max_retries):
        try:
            with requests.get(url, auth=HTTPDigestAuth(username, password), stream=True) as response:
                download_start_time = time.time()
                if response.status_code == 200:
                    file_size = int(response.headers.get('content-length', 0))
                    progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True)
                    
                    create_folder(folder_name)
                    dav_file = unique_filename(os.path.join(folder_name, f"{vid_name}.dav"))
                    
                    print("Starting download")
                    with open(dav_file, 'wb') as out_file:
                        for chunk in response.iter_content(chunk_size=4096):
                            progress_bar.update(len(chunk))
                            if chunk:
                                out_file.write(chunk)
                    progress_bar.close()
                    download_end_time = time.time()
                    print(f"Download finished in {download_end_time - download_start_time:.2f} seconds")
                    return dav_file  # Return file path for parallel conversion
                else:
                    print("Error", response.status_code)
                    break
        except Exception as e:
            print(f"An error occurred: {e}")
            if attempt == max_retries - 1:
                raise Exception("Maximum retries reached")
            else:
                time.sleep(delay)
    return None

def main():
    parser = argparse.ArgumentParser(description="Download video clips based on UTC timestamps from a CSV file.")
    parser.add_argument("--csv_file", required=True, help="Path to the CSV file")
    parser.add_argument("--nvr", default="localhost:8010", help="NVR server address")
    parser.add_argument("--pre_seconds", type=int, default=3, help="Seconds before UTC time to start the video")
    parser.add_argument("--post_seconds", type=int, default=15, help="Seconds after UTC time to end the video")
    parser.add_argument("--cell", default="", help="Optional cell information for the filename")
    parser.add_argument("--channel", type=int, default=3, help="NVR channel number (default: 3)")
    parser.add_argument("--bagger_code", nargs='*', default=None, help="Filter videos by bagger_code (default: all)")
    args = parser.parse_args()
    
    csv_file_path = args.csv_file
    nvr = args.nvr
    pre_seconds = args.pre_seconds
    post_seconds = args.post_seconds
    cell = args.cell.strip()
    channel = args.channel
    bagger_code_filter = args.bagger_code
    
    folder_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    create_folder(folder_name)
    
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        
        download_tasks = []
        with ThreadPoolExecutor(max_workers=3) as download_executor, ProcessPoolExecutor(max_workers=3) as convert_executor:
            for row in reader:
                if bagger_code_filter and row.get("bagger_code") not in bagger_code_filter:
                    continue
                
                utc_time_str = row["utc_time"]
                bagger_code = row.get("bagger_code", "Unknown")
                start_timestamp = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00")) - timedelta(seconds=pre_seconds)
                end_timestamp = start_timestamp + timedelta(seconds=pre_seconds + post_seconds)
                
                start_time_str = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                vid_name = f"{cell}_{utc_time_str}_ErrorCode_{bagger_code}" if cell else f"{utc_time_str}_ErrorCode_{bagger_code}"
                
                print(f"Processing utc_time {utc_time_str} starting at {start_time_str}")
                
                future = download_executor.submit(get_video, nvr, start_time_str, end_time_str, vid_name, folder_name, channel)
                download_tasks.append(future)
            
            ### Convert videos in parallel
            for future in download_tasks:
                file_path = future.result()
                if file_path:
                    convert_executor.submit(convert_video, file_path)

if __name__ == '__main__':
    main()
