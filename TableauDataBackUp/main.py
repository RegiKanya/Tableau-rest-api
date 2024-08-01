import os
import json
import tableauserverclient as TSC
import configparser
from google.cloud import storage
from datetime import datetime
from slack_sdk.webhook import WebhookClient

config = configparser.ConfigParser()
config.read('config.ini')
today = datetime.today().strftime('%Y-%m-%d')

#tableau
token_name = config['tableau']['token_name']
server_url = config['tableau']['server_url']
api_version = config['tableau']['api_version']
site_name = config['tableau']['site_name']
personal_access_token = os.environ['tableau_backup']

#gcp
bucket_name = config['gcp']['bucket_name']
gcp_credentials = os.environ['tableau_to_pdf_service_account_credentials']
storage_client = storage.Client.from_service_account_info(json.loads(gcp_credentials))
bucket = storage_client.bucket(bucket_name)

slack_webhook_url = os.environ["slack_webhook_url"]
webhook = WebhookClient(slack_webhook_url)

tableau_server_config = {
    'tableau_prod': {
        'server': server_url,
        'api_version': api_version,
        'personal_access_token_name': token_name,
        'personal_access_token_secret': personal_access_token,
        'site_name': '',
        'site_url': '',
        'site_id': ''
    }
}


def upload_to_gcs(file_name, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_name)
    print(f"uploaded {file_name} to google cloud storage")


def send_slack_notification(message):
    webhook.send(text=message)


def main():
    try:
        tableau_auth = TSC.PersonalAccessTokenAuth(token_name, personal_access_token, site_id='')
        server = TSC.Server(server_url)
        with server.auth.sign_in(tableau_auth):
            page_options = TSC.RequestOptions(1, 5)
            items_to_download = [server.datasources, server.workbooks]
            for items in items_to_download:
                for item in TSC.Pager(items, page_options):
                    item_file_safe_name = item.name.replace("/", "_")
                    downloaded_file = items.download(item.id, item_file_safe_name)
                    print(f"Downloaded the file to {item.project_name}/{item.name}.")
                    upload_to_gcs(downloaded_file, f"{today}/{item.project_name}/{item_file_safe_name}")
                    os.remove(downloaded_file)
    except Exception as e:
        send_slack_notification(f"Could not create a backup of Tableau because of the following exception {e}")
    send_slack_notification("Finished creating a backup of Tableau")


if __name__ == '__main__':
    main()