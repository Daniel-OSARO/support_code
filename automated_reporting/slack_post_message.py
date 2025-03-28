import requests,json,os,re
from box import Box
from slack_user_id_mapping import coupang_onsite_team_slack_id_mapping
token = os.environ['SLACK_MAGIC']
channel_id = 'C0364DL4SR3' #real deal
#channel_id = 'C0832HHUJ0P'  #testing
app_user_id='U083KJZDNLU'


# Headers for the HTTP request
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json; charset=utf-8'
}
username_pattern=re.compile(r'(@\w+)')

def convert_post_id_to_ts(pcal):
  return pcal[:-6]+'.'+pcal[-6:]
def extract_usernames_from_input(message_string):
  return username_pattern.findall(message_string)
def translate_mentions(message_string:str,mention_uid_mapping=coupang_onsite_team_slack_id_mapping):
  for match in extract_usernames_from_input(message_string):
    message_string=message_string.replace(match,'<@%s>'%mention_uid_mapping[match])
  return message_string
def api_call_slack(method,**kwargs):
  return Box(requests.post(f'https://slack.com/api/{method}',headers=headers,json=kwargs).json())
def find_latest_production_message():
  previous200=api_call_slack('conversations.history',channel=channel_id)
  production_messages=(message for message in previous200.messages if message.user==app_user_id and 'Production plan:' in message.text)
  return next(iter(production_messages),None)

def api_post_slack(message,**kwargs):
  chat_response_share=requests.post('https://slack.com/api/chat.postMessage',headers=headers,json={
    'unfurl_links':True,
    'unfurl_media':True,
    'channel':channel_id,
    'text':translate_mentions(message),
    **kwargs}
  )
  return Box(json.loads(chat_response_share.text))

def api_upload_slack(input_data, filename, message,**msg_kwargs):
  """
  Uploads a file or string content to Slack using files.getUploadURLExternal and files.completeUploadExternal.

  Args:
    input_data (str): File path or string content to upload.
    filename (str): Name of the file (used if input_data is not a file path).
    message (str): Notes to go with the file.

  Returns:
    str: Slack file ID on successful upload, or None if upload fails.
  """
  slack_api_base = "https://slack.com/api"

  try:
    # Determine if the input is a file path
    if os.path.isfile(input_data):
      with open(input_data, 'rb') as file:
        file_contents = file.read()
      file_name = os.path.basename(input_data)
    else:
      # If it's not a path, treat it as a string and use filename or default
      file_contents = input_data.encode('utf-8')
      file_name = filename if filename else "uploaded_file.txt"

    # Step 1: Request an upload URL
    get_upload_url_endpoint = f"{slack_api_base}/files.getUploadURLExternal"
    updated_headers={}
    updated_headers.update(headers)
    updated_headers['Content-Type']='application/x-www-form-urlencoded'
    upload_url_response = requests.post(get_upload_url_endpoint, headers=updated_headers, data={"length":len(file_contents),'filename':filename if filename is not None else 'File'})
    if not upload_url_response.ok:
      print(f"Failed to get upload URL: {upload_url_response.text}")
      return None

    upload_url_data = upload_url_response.json()
    if not upload_url_data.get("ok"):
      print(f"Error in getUploadURLExternal: {upload_url_data}")
      return None

    upload_url = upload_url_data["upload_url"]
    file_id = upload_url_data["file_id"]

    # Step 2: Upload file contents to the provided URL
    upload_headers = {"Content-Type": "application/octet-stream"}
    upload_response = requests.post(upload_url, headers=upload_headers, data=file_contents)
    if not upload_response.ok:
      print(f"Failed to upload file: {upload_response.text}")
      return None

    # Step 3: Complete the file upload
    complete_upload_endpoint = f"{slack_api_base}/files.completeUploadExternal"
    complete_payload = {
      "files": [
        {
          "id": file_id,
          "title": file_name,
        }
      ],
      'channel_id': channel_id,
      'initial_comment':message,
      **msg_kwargs

    }
    complete_response = requests.post(complete_upload_endpoint, headers=headers, json=complete_payload)
    if not complete_response.ok:
      print(f"Failed to complete upload: {complete_response.text}")
      return None

    complete_data = complete_response.json()
    if not complete_data.get("ok"):
      print(f"Error in completeUploadExternal: {complete_data}")
      return None

    return file_id

  except Exception as e:
    print(f"Error: {e}")
    return None
