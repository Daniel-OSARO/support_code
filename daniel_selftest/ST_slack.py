import os
import datetime
import ssl
import certifi
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ------------------------------
# Module Functions for Slack
# ------------------------------

def initialize_slack_client(token):
    """
    Initializes and returns the Slack client with a custom SSL context.
    :param token: Slack Bot User OAuth Token.
    """
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    return WebClient(token=token, ssl=ssl_context)

def send_message_to_slack(client, channel_id, message):
    """
    Sends a custom text message to a specified Slack channel.
    :param client: Initialized Slack WebClient.
    :param channel_id: The Slack channel ID to post the message to.
    :param message: The message content to be posted.
    """
    try:
        response = client.chat_postMessage(channel=channel_id, text=message)
        print(f"Message posted successfully to Slack channel {channel_id}.")
    except SlackApiError as e:
        print(f"Error posting to Slack: {e.response['error']}")
        print(f"Full response: {e.response}")

def send_file_content_to_slack(client, channel_id, file_path):
    """
    Posts the content of the file to a specified Slack channel as a text block.
    :param client: Initialized Slack WebClient.
    :param channel_id: The Slack channel ID to post the message to.
    :param file_path: Path to the file to be posted.
    """
    try:
        with open(file_path, "r") as file:
            content = file.read()

        # Send the content of the file as a message
        response = client.chat_postMessage(channel=channel_id, text=f"```\n{content}\n```")
        print(f"File content posted successfully to Slack channel {channel_id}.")
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
    except SlackApiError as e:
        print(f"Error posting to Slack: {e.response['error']}")
        print(f"Full response: {e.response}")

def send_file_to_slack(client, channel_id, file_path, file_name_itself):
    """
    Uploads a file to a specified Slack channel using files_upload_v2.
    :param client: Initialized Slack WebClient.
    :param channel_id: The Slack channel ID to post the file to.
    :param file_path: Path to the file to be uploaded.
    :param file_name_itself: The name of the file to be displayed in Slack.
    """
    try:
        with open(file_path, "rb") as file:
            response = client.files_upload_v2(
                channel=channel_id,
                file=file,
                file_name_itself=file_name_itself
            )
        print(f"File {file_name_itself} uploaded successfully to Slack channel {channel_id}.")
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
    except SlackApiError as e:
        print(f"Error uploading file to Slack: {e.response['error']}")
        print(f"Full response: {e.response}")



# ------------------------------
# Main Script Functions
# ------------------------------

def get_today_date():
    """Returns the current date in MMDD format."""
    return datetime.datetime.now().strftime("%m%d")

def main():
    # Slack configuration
    SLACK_CHANNEL_ID = "C084S1KEGNM"

    # Initialize Slack client
    slack_client = initialize_slack_client(SLACK_TOKEN)

    # Send a custom message (e.g., today's self test results in bold)
    today_date = get_today_date()
    send_message_to_slack(slack_client, SLACK_CHANNEL_ID, f"*{today_date}'s Self Test Results*")

    # Send the content of a file as a message
    file_name_content = "integrated_summary_Cell12_1210.txt"
    file_path = os.path.expanduser(f"~/Documents/{file_name_content}")
    send_file_content_to_slack(slack_client, SLACK_CHANNEL_ID, file_path)

    # Send the file itself to Slack
    file_name_itself = "integrated_summary_Cell12_1210.txt"
    file_path_to_send = os.path.expanduser(f"~/Documents/{file_name_itself}")  # Example path
    send_file_to_slack(slack_client, SLACK_CHANNEL_ID, file_path_to_send, file_name_itself)

if __name__ == "__main__":
    main()
