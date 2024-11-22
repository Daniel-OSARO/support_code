import requests
from requests.auth import HTTPDigestAuth
import subprocess, os, sys
import time
from getpass import getpass
from tqdm import tqdm
from datetime import datetime, timedelta


def generate_time_ranges(start_time, end_time):
    """Cut the time range into smaller bits so you're not as wrecked if the download gets interrupted"""
    # Convert the start and end times to datetime objects
    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

    # Check if the total time is less than or equal to 5 minutes
    if end_time - start_time <= timedelta(minutes=5):
        print("Given times are under 5 minutes")
        return [{
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S')
        }]
    
    # Initialize an empty list to hold the time ranges
    time_ranges = []
    print("Given times are greater than 5 mins. Generating 5 min intervals.")
    
    # Start generating the time ranges
    current_start = start_time
    while current_start < end_time:
        # Calculate the next end time
        current_end = min(current_start + timedelta(minutes=5), end_time)
        
        # Add the current time range to the list
        time_range = {
            'start_time': current_start.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': current_end.strftime('%Y-%m-%d %H:%M:%S')
        }
        time_ranges.append(time_range)
        
        # Move to the next time range
        current_start = current_end
    
    return time_ranges

def unique_filename(base_filename):
    """
    Return a unique filename by appending (1), (2), etc. to the base filename if it exists.
    """
    counter = 1
    filename, file_extension = os.path.splitext(base_filename)
    
    new_filename = base_filename
    while os.path.exists(new_filename):
        new_filename = f"{filename}_({counter}){file_extension}"
        counter += 1

    return new_filename


def validate_times(start_time, end_time):
    # Verify that start time is before end time
    # Parse the strings into datetime objects
    try:
        start_datetime = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_datetime = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        start_datetime = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S')

    # Compare the datetime objects
    if end_datetime <= start_datetime:
        print("Error: End time should be later than start time.")
        sys.exit()
    else:
        print("Times are valid.")


def get_video(nvrname, start_time, end_time, cell_number, channel_number, vid_name=None):

    url = f"http://{nvrname}/cgi-bin/loadfile.cgi?action=startLoad&channel={channel_number}&startTime={start_time}&endTime={end_time}"
    #url = f"http://{nvrname}/cgi-bin/loadfile.cgi?action=startLoad&channel=15&startTime={start_time}&endTime={end_time}"

    username = "admin"
    #password = getpass("Enter NVR admin password: ")
    password = "osaro51423"
    
    max_retries, delay = 5, 5 #sometimes we have to retry the connection after 5 seconds if we get an error.
    

    for attempt in range(max_retries):    
        try:
            with requests.get(url, auth=HTTPDigestAuth(username, password), stream=True) as response:

                #Start timing the download
                download_start_time = time.time()
                print(url)

                
                #print(response.status_code)
                if response.status_code == 200:
                    print(response.headers)
                    # Get the total file size for progress bar
                    file_size = int(response.headers.get('content-length', 0))
                    progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True)

                    download_path = "/Users/daniel/Screenshots"
                    #dav_file = unique_filename('output.dav')
                    file_start_time = start_time.replace("-", "_").replace(":", "_").replace(" ", "_")
                    dav_file = unique_filename(f"Cell{cell_number}_{channel_number}_{file_start_time}.dav")
                    # dav_file = unique_filename(os.path.join(download_path, f"Cell{cell_number}_{channel_number}_{file_start_time}.dav"))

                    print('Starting download')
                    with open(dav_file, 'wb') as out_file:
                    #with open('output.dav', 'ab') as out_file:
                        for chunk in response.iter_content(chunk_size=4096):
                            progress_bar.update(len(chunk))
                            if chunk:
                                out_file.write(chunk)

                    progress_bar.close()
                    download_end_time = time.time()
                    print('Download is finished. It took {:.2f} seconds'.format(download_end_time - download_start_time))


                    #Start timing conversion
                    conversion_start_time = time.time()
                    print('Starting conversion')


                    # Convert dav to mp4, source and output files.
                    input_file = dav_file
                    #output_file = unique_filename('output.mp4')
                    if vid_name != None:
                        output_file = unique_filename(f"{vid_name}.mp4")
                        # output_file = unique_filename(os.path.join(download_path, f"{vid_name}.mp4"))
                    else:
                        output_file = unique_filename(f"Cell{cell_number}_{channel_number}_{file_start_time}.mp4")
                        # output_file = unique_filename(os.path.join(download_path, f"Cell{cell_number}_{channel_number}_{file_start_time}.mp4"))

                    # Subprocess command string, requires VLC installed.
                    #cmd = '/Applications/VLC.app/Contents/MacOS/VLC -I dummy -vvv {} --sout="#transcode{{vcodec=h264,vb=500,deinterlace}}:standard{{access=file,mux=mp4,dst={}}}" vlc://quit'.format(input_file, output_file)
                    cmd = """/Applications/VLC.app/Contents/MacOS/VLC --quiet -I dummy "{}" --sout="#transcode{{vcodec=h264,vb=500,deinterlace}}:standard{{access=file,mux=mp4,dst='{}'}}" vlc://quit""".format(input_file, output_file)
                    #cmd = "ffmpeg -i {} -c:v libx264 -b:v 500k -vf yadif=0:-1:0 -f mp4 {}".format(input_file, output_file)
                    #cmd = "ffmpeg -hwaccel videotoolbox -i {} -c:v libx264 -b:v 500k -vf yadif=0:-1:0 -f mp4 {}".format(input_file, output_file)
                    

                    # Run the command
                    subprocess.call(cmd, shell=True)
                    # Calculate and print conversion time
                    conversion_end_time = time.time()
                    print('Conversion finished. It took {:.2f} seconds.'.format(conversion_end_time - conversion_start_time))
                    print(f"Store the video to {download_path}.")
                    os.remove(dav_file)
                    break #break out of retry loop on success
                else:
                    print("Error", response.status_code)
                    break #break out of retry loop because this is probably not a connection error.
        except Exception as e:
            print(f"An error occurred: {e}")
            if attempt == max_retries - 1:
                raise Exception("Maximum retries reached")
            else:
                time.sleep(delay)



if __name__ == '__main__':
    nvrname = "192.168.111.17:8010"
    channel_number = "4"
    start_time = "2024-11-13 05:14:00"
    end_time = "2024-11-13 05:20:00"
    cell_number = int(nvrname.split('.')[-1].split(':')[0])%10
    #cut this into 5 minute chunks so we don't get boned if we lose connection.
    #if the time is less than or = to 5 minutes we'll just return the original start & end times.
    validate_times(start_time, end_time)
    chunked_times = generate_time_ranges(start_time, end_time)
    for chunk in chunked_times:
        get_video(nvrname, chunk['start_time'], chunk['end_time'], cell_number, channel_number)