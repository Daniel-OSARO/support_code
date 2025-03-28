"""
1. grafana의 Item Stuck Barcode Info에서 csv파일 받기
2. 터미널에서 ssh -L 8005:192.168.8.10:80 -A cx (x는 셀 번호) 에 접속하기
3. item_stuck_videos.py와 csv파일 같은 폴더에 두기
4. python3 item_stuck_videos.py --csv_file "csv파일 이름"  --duration 30 --cell "cell번호"
    예시 : python item_stuck_videos.py --csv_file "Item Stuck Barcode Info-data-2025-02-12 13_19_42.csv" --duration 30 --cell "cell7"
"""

import csv
import os
import time
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPDigestAuth
from tqdm import tqdm
import argparse

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

def get_video(nvrname, start_time, end_time, vid_name):
    """
    Downloads a video clip from the NVR for the period between start_time and end_time.
    
    Parameters:
      - nvrname: The NVR server address (e.g., "localhost:8005")
      - start_time: The start time as a string in the format 'YYYY-MM-DD HH:MM:SS'
      - end_time: The end time as a string in the format 'YYYY-MM-DD HH:MM:SS'
      - vid_name: The base filename for saving the video (without extension)
    """
    # Construct the download URL (format based on the NVR)
    url = f"http://{nvrname}/cgi-bin/loadfile.cgi?action=startLoad&channel=2&startTime={start_time}&endTime={end_time}"
    username = "admin"
    password = "osaro51423"
    
    # Calculate the video length in seconds
    start_time_obj = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time_obj = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    video_length = (end_time_obj - start_time_obj).total_seconds()
    
    print(f"Downloading video: {vid_name}")
    print(f"Video length: {video_length:.2f} seconds")
    
    max_retries, delay = 5, 5  # Maximum of 5 retries, with a 5-second delay between attempts
    for attempt in range(max_retries):
        try:
            with requests.get(url, auth=HTTPDigestAuth(username, password), stream=True) as response:
                download_start_time = time.time()
                if response.status_code == 200:
                    file_size = int(response.headers.get('content-length', 0))
                    progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True)
                    
                    # Extract the date from start_time and create the corresponding folder
                    date_folder = create_date_folder(start_time)
                    dav_file = unique_filename(os.path.join(date_folder, f"{vid_name}.dav"))
                    
                    print("Starting download")
                    with open(dav_file, 'wb') as out_file:
                        for chunk in response.iter_content(chunk_size=4096):
                            progress_bar.update(len(chunk))
                            if chunk:
                                out_file.write(chunk)
                    progress_bar.close()
                    download_end_time = time.time()
                    print(f"Download finished in {download_end_time - download_start_time:.2f} seconds")
                    break  # Exit loop upon success
                else:
                    print("Error", response.status_code)
                    break
        except Exception as e:
            print(f"An error occurred: {e}")
            if attempt == max_retries - 1:
                raise Exception("Maximum retries reached")
            else:
                time.sleep(delay)

def main():
    parser = argparse.ArgumentParser(
        description="Download 30-second video clips starting from the induction_error_time in the CSV file."
    )
    parser.add_argument(
        "--csv_file",
        required=True,
        help="Path to the Item Stuck Barcode Info CSV file (e.g., 'Item Stuck Barcode Info-data-2025-02-12 13_19_42.csv')"
    )
    parser.add_argument(
        "--nvr",
        default="localhost:8005",
        help="NVR server address (default: localhost:8005)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Duration of the video clip in seconds (default: 30 seconds)"
    )
    parser.add_argument(
        "--cell",
        default="",
        help="Optional cell information to include in the video filename"
    )
    args = parser.parse_args()
    
    csv_file_path = args.csv_file
    nvr = args.nvr
    duration = args.duration
    cell = args.cell.strip()  # Remove any surrounding whitespace

    # Open and read the CSV file.
    # In this example, the CSV file is assumed to be comma-separated.
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        
        for row in reader:
            induction_time_str = row["induction_error_time"]
            # Parse the ISO format timestamp (replacing 'Z' with '+00:00' for UTC)
            start_timestamp = datetime.fromisoformat(induction_time_str.replace("Z", "+00:00")) - timedelta(seconds=3)
            end_timestamp = start_timestamp + timedelta(seconds=duration)
            
            # Convert timestamps to strings in the format 'YYYY-MM-DD HH:MM:SS'
            start_time_str = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = end_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            barcode = row["barcode"]
            # Include cell information in the filename if provided
            if cell:
                vid_name = f"{cell}_{barcode}_clip_{start_time_str.replace(' ', '_')}"
            else:
                vid_name = f"{barcode}_clip_{start_time_str.replace(' ', '_')}"
            
            print(f"Processing barcode {barcode} starting at {start_time_str}")
            get_video(nvr, start_time_str, end_time_str, vid_name)

if __name__ == '__main__':
    main()
