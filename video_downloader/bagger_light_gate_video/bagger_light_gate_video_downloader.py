"""
1. grafana의 Item Stuck Barcode Info에서 csv파일 받기
2. 터미널에서 ssh -L 8005:192.168.8.10:80 -A cx (x는 셀 번호) 에 접속하기
3. bagger_light_gate_video_downloader.py와 csv파일 같은 폴더에 두기
4. python3 bagger_light_gate_video_downloader.py --csv_file "csv파일 이름" --channel [채널번호]
    예시 : python bagger_light_gate_video_downloader.py --csv_file "bagger_light_gate_test.csv" --channel 2
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
import re
import socket
import structlog
import sys
import logging

# Configure structlog
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True
)

logger = structlog.get_logger()

def extract_cell_number(hostname):
    """Extract cell number from hostname (e.g., cpg-ech02-5 -> 5)"""
    match = re.search(r'cpg-ech02-(\d+)', hostname)
    if match:
        cell_num = int(match.group(1))
        logger.info("cell_number_found", cell_number=cell_num)
        return cell_num
    logger.error("invalid_hostname_format", hostname=hostname)
    return None

def get_nvr_address(cell_number):
    """Get NVR address based on cell number"""
    nvr = f"192.168.111.{10 + cell_number}:8010"
    logger.info("generated_nvr_address", nvr=nvr, cell_number=cell_number)
    return nvr

def create_output_folders():
    """Create folders for DAV and MP4 files"""
    base_folder = datetime.now().strftime("videos_%Y%m%d_%H%M%S")
    dav_folder = os.path.join(base_folder, "dav_files")
    mp4_folder = os.path.join(base_folder, "mp4_files")
    
    for folder in [base_folder, dav_folder, mp4_folder]:
        if not os.path.exists(folder):
            os.makedirs(folder)
    
    return base_folder, dav_folder, mp4_folder

def unique_filename(base_filename, folder):
    """
    Returns a unique filename by appending a counter if the file already exists.
    """
    counter = 1
    filename, file_extension = os.path.splitext(base_filename)
    new_filename = os.path.join(folder, base_filename)
    while os.path.exists(new_filename):
        new_filename = os.path.join(folder, f"{filename}_({counter}){file_extension}")
        counter += 1
    return new_filename

def convert_video(input_file, output_folder):
    """Convert DAV file to MP4"""
    output_file = os.path.join(output_folder, os.path.basename(input_file).replace(".dav", ".mp4"))
    
    command = ["ffmpeg", "-y", "-i", input_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", output_file]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    if os.path.exists(output_file):
        logger.info("conversion_complete", output_file=output_file)
        return output_file
    else:
        logger.error("conversion_failed", input_file=input_file)
        return None

def check_connection(nvrname):
    """Check if NVR is reachable"""
    logger.info("checking_nvr_connection", nvr=nvrname)
    try:
        host, port = nvrname.split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)  # 5 second timeout
        result = sock.connect_ex((host, int(port)))
        sock.close()
        if result == 0:
            logger.info("nvr_connection_successful", nvr=nvrname)
        else:
            logger.error("nvr_connection_failed", nvr=nvrname, error_code=result)
        return result == 0
    except Exception as e:
        logger.error("nvr_connection_error", nvr=nvrname, error=str(e))
        return False

def get_video(nvrname, start_time, end_time, vid_name, channel, dav_folder):
    """
    Downloads a video clip from the NVR.
    """
    
    # Check connection first
    if not check_connection(nvrname):
        logger.error("nvr_not_reachable", nvr=nvrname)
        return None

    # Construct the download URL
    url = f"http://{nvrname}/cgi-bin/loadfile.cgi?action=startLoad&channel={channel}&startTime={start_time}&endTime={end_time}"
    username = "admin"
    password = "osaro51423"
    
    # Calculate the video length in seconds
    start_time_obj = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time_obj = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    video_length = (end_time_obj - start_time_obj).total_seconds()
    
    logger.info("video_details", length_seconds=video_length)
    
    max_retries, delay = 5, 5
    for attempt in range(max_retries):
        logger.info("download_attempt", attempt=attempt + 1, max_attempts=max_retries)
        try:
            with requests.get(url, auth=HTTPDigestAuth(username, password), stream=True, timeout=30) as response:
                download_start_time = time.time()
                
                if response.status_code == 200:
                    file_size = int(response.headers.get('content-length', 0))
                    logger.info("starting_download", file_size=file_size)
                    
                    progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True)
                    dav_file = os.path.abspath(unique_filename(f"{vid_name}.dav", dav_folder))
                    logger.info("saving_to_file", file_path=dav_file)
                    
                    with open(dav_file, 'wb') as out_file:
                        for chunk in response.iter_content(chunk_size=4096):
                            progress_bar.update(len(chunk))
                            if chunk:
                                out_file.write(chunk)
                    progress_bar.close()
                    
                    download_end_time = time.time()
                    download_duration = download_end_time - download_start_time
                    logger.info("download_completed",
                              duration_seconds=download_duration)
                    
                    return dav_file
                else:
                    logger.error("download_failed",
                               status_code=response.status_code,
                               response_text=response.text)
                    break
        except requests.exceptions.Timeout:
            logger.error("request_timeout", nvr=nvrname)
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
        except requests.exceptions.ConnectionError as e:
            logger.error("connection_error",
                        nvr=nvrname,
                        error=str(e))
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
        except Exception as e:
            logger.error("unexpected_error",
                        nvr=nvrname,
                        error=str(e),
                        error_type=type(e).__name__)
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
    return None

def main():
    parser = argparse.ArgumentParser(
        description="Download video clips for bagger light gate errors."
    )
    parser.add_argument(
        "--csv_file",
        required=True,
        help="Path to the bagger light gate test CSV file"
    )
    parser.add_argument(
        "--nvr",
        help="Optional NVR server address to override default cell-based address"
    )
    parser.add_argument(
        "--channel",
        type=int,
        default=2,
        help="Camera channel number (default: 2)"
    )
    args = parser.parse_args()
    
    # Get absolute path of CSV file
    csv_file_path = os.path.abspath(args.csv_file)
    logger.info("script_started", csv_file=csv_file_path)

    # Create output folders and change to base folder
    base_folder, dav_folder, mp4_folder = create_output_folders()
    base_folder = os.path.abspath(base_folder)
    dav_folder = os.path.abspath(dav_folder)
    mp4_folder = os.path.abspath(mp4_folder)
    os.chdir(base_folder)

    try:
        # First, download all DAV files
        dav_files = []
        with open(csv_file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            logger.info("csv_headers", headers=reader.fieldnames)
            
            for row in reader:
                logger.info("processing_row", row_number=reader.line_num)
                hostname = row["jsonPayload.hostname"].strip("'")
                timestamp_str = row["timestamp"].strip("'")
                
                cell_number = extract_cell_number(hostname)
                if cell_number is None:
                    continue
                
                nvr = args.nvr if args.nvr else get_nvr_address(cell_number)
                
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                start_time = timestamp - timedelta(seconds=15)
                end_time = timestamp + timedelta(seconds=10)
                
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
                
                vid_name = f"cell{cell_number}_{timestamp.strftime('%Y%m%d_%H%M%S')}_bagger_light_gate_ch{args.channel}"
                
                dav_file = get_video(nvr, start_time_str, end_time_str, vid_name, args.channel, dav_folder)
                if dav_file:
                    dav_files.append(dav_file)
        
        # Then, convert all DAV files to MP4
        logger.info("starting_video_conversion", total_files=len(dav_files))
        for dav_file in dav_files:
            convert_video(dav_file, mp4_folder)
                
    except Exception as e:
        logger.error("script_error",
                    error=str(e),
                    error_type=type(e).__name__)
        raise

if __name__ == '__main__':
    main()
