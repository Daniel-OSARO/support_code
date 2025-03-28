import json
from datetime import datetime

def extract_information(data):
    # Attempt to strip out non-JSON lines and find the JSON content
    try:
        json_start = data.find('{')
        json_end = data.rfind('}')
        json_data = json.loads(data[json_start:json_end+1])
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")
        return

    # Getting the current date for naming
    today = datetime.today().strftime('%m%d')
    server_name = "CXST" + today

    # Suction Check
    suction_total = len(json_data["suctionCheck"])
    suction_success = sum(1 for item in json_data["suctionCheck"] if item["status"] == "SUCCESS")
    suction_result = f"- Suction {suction_success}/{suction_total}"

    # Calibration Check
    # Calibration Check
    calibration_total = len(json_data["calibrationCheck"])
    calibration_success = 0
    non_calibrated_cameras = []

    for item in json_data["calibrationCheck"]:
        if item["status"] == "CALIBRATED":
            calibration_success += 1
        else:
            non_calibrated_cameras.append(item["cameraId"])

    if non_calibrated_cameras:
        non_calibrated_cameras_list = ", ".join(non_calibrated_cameras)
        calibration_result = f"- Camera validation {calibration_success}/{calibration_total} ({non_calibrated_cameras_list})"
    else:
        calibration_result = f"- Camera validation {calibration_success}/{calibration_total}"

    # Force Compression Check
    force_check_total = sum(1 for item in json_data["forceCompressionCheck"] for status in ["idleStatus", "pressedStatus", "deeperStatus"])
    force_check_success = sum(1 for item in json_data["forceCompressionCheck"] 
                              for status in ["idleStatus", "pressedStatus", "deeperStatus"] 
                              if item[status] == "SUCCESS")
    
    # Track statuses with "THRESHOLD_EXCEEDED"
    threshold_exceeded_statuses = []
    for item in json_data["forceCompressionCheck"]:
        for status in ["idleStatus", "pressedStatus", "deeperStatus"]:
            if item[status] == "THRESHOLD_EXCEEDED":
                threshold_exceeded_statuses.append(status)

    if threshold_exceeded_statuses:
        exceeded_statuses_str = ", ".join(threshold_exceeded_statuses)
        force_compression_result = f"- Force compression {force_check_success}/{force_check_total} ({exceeded_statuses_str})"
    else:
        force_compression_result = f"- Force compression {force_check_success}/{force_check_total}"

    # Robot Check
    robot_check_result = ""
    if "robotCheck" in json_data:
        if json_data["robotCheck"]["status"] == "SUCCESS":
            robot_check_result = "- Robot check OK"
        elif json_data["robotCheck"]["status"] == "CONFIG_CHECK_FAILED":
            robot_check_result = "- Config check failed"

    # Brightness Check
    brightness_total = len(json_data["brightnessCheck"])
    brightness_success = sum(1 for item in json_data["brightnessCheck"] if item["status"] == "SUCCESS")
    failed_cameras = [item["cameraId"] for item in json_data["brightnessCheck"] if item["status"] != "SUCCESS"]

    # Create the brightness result string
    if brightness_success == brightness_total:
        brightness_result = f"- Brightness check {brightness_success}/{brightness_total}"
    else:
        failed_cameras_list = ", ".join(failed_cameras)
        brightness_result = f"- Brightness check {brightness_success}/{brightness_total} ({failed_cameras_list})"

    # Printing the results
    print(server_name)
    print(suction_result)
    print(calibration_result)
    print(force_compression_result)
    if robot_check_result:
        print(robot_check_result)
    print(brightness_result)

    # Extracting and rounding the unsealedKpa and sealedKpa for specific endEffector
    for item in json_data["suctionCheck"]:
        if item["endEffector"] == "single_piab_30mm_BL30-3":
            unsealed_kpa = round(item["unsealedKpa"], 2)
            sealed_kpa = round(item["sealedKpa"], 2)
            print(f"- 30mm cup : {unsealed_kpa} / {sealed_kpa}")

if __name__ == "__main__":
    # Ask user to paste the script
    print("Paste the JSON part of the output here (end with an empty line):")
    user_input = []
    while True:
        line = input()
        if line.strip() == "":
            break
        user_input.append(line)
    extract_information("\n".join(user_input))
