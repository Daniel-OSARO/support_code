import json
from datetime import datetime, timedelta, timezone
import sys
from io import StringIO
import os

def extract_information(cellnum,json_data):
  # Convert the date to a datetime object and adjust to GMT+9
  date_obj = datetime.now(tz=timezone.utc)
  date_gmt9 = date_obj + timedelta(hours=9)

  # Format the server name based on the adjusted date
  today_gmt9 = date_gmt9.strftime('%m%d')
  server_name = f"C{cellnum}ST{today_gmt9}"

  # Suction Check
  suction_total = len(json_data["suctionCheck"])
  suction_success = sum(1 for item in json_data["suctionCheck"] if item["status"] == "SUCCESS")
  suction_result = f"- Suction {suction_success}/{suction_total}"

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

  if brightness_success == brightness_total:
    brightness_result = f"- Brightness check {brightness_success}/{brightness_total}"
  else:
    failed_cameras_list = ", ".join(failed_cameras)
    brightness_result = f"- Brightness check {brightness_success}/{brightness_total} ({failed_cameras_list})"

  # Prepare the summarized results
  output = StringIO()
  print("\n", file=output)
  print("===== Summarized Results =====", file=output)
  print(server_name, file=output)
  print(suction_result, file=output)

  for item in json_data["suctionCheck"]:
    tool_size = item["endEffector"].split('_')[2]
    unsealed_kpa = round(item["unsealedKpa"], 2)
    sealed_kpa = round(item["sealedKpa"], 2)
    print(f"  * {tool_size} : {unsealed_kpa} / {sealed_kpa}", file=output)

  print(calibration_result, file=output)
  print(force_compression_result, file=output)
  if robot_check_result:
    print(robot_check_result, file=output)
  print(brightness_result, file=output)
  return output.getvalue()



if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("Error: Missing actionID argument")
    sys.exit(1)

  action_id = sys.argv[1]

  # Read the full results and extract only the JSON part
  script_dir = os.path.expanduser("~/script")
  with open(os.path.join(script_dir, "full_results.txt"), "r") as f:
    data = f.read()

  json_start = data.find('{')
  json_end = data.rfind('}')
  json_data_str = data[json_start:json_end+1]

  # Save the extracted JSON to target_data.json
  with open(os.path.join(script_dir, "target_data.json"), "w") as json_file:
    json_file.write(json_data_str)

  # Load and process the JSON data
  with open(os.path.join(script_dir, "target_data.json"), "r") as f:
    json_data = json.loads(f.read())

  # Extract and save the summarized results
  extract_information(json_data, action_id)
