import csv
import logging
import sys
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(
    account_url="https://salearn220623.blob.core.windows.net/",credential="96rxu4rzwCrz0hpaD/Dr9gvs5WJPXAo6pl1feH3xyth6Z9NCkWcNDzKwTuxU0/VMeOm9pJKWgEzO+AStWlnjiQ==")

def check_user_exists(name, container_client, blob_name):
    with container_client.get_blob_client(blob_name) as blob_client:
        downloaded_blob = blob_client.download_blob()
        csv_data = downloaded_blob.content_as_text()
        csvreader = csv.DictReader(csv_data.splitlines())
        for row in csvreader:
            if row['Name'] == name:
                return True
    return False

def add_user(name, age, gender, container_client, blob_name):
    fieldnames = ['Name', 'Age', 'Gender']
    user_exists = check_user_exists(name, container_client, blob_name)
    try:
     if user_exists:
           logger.info("User already exists. Checking and updating details.")
           updated_rows = []
           with container_client.get_blob_client(blob_name) as blob_client:
            downloaded_blob = blob_client.download_blob()
            csv_data = downloaded_blob.content_as_text()
            csvreader = csv.DictReader(csv_data.splitlines())
            for row in csvreader:
                if row['Name'] == name:
                    row['Age'] = str(age)
                    row['Gender'] = gender
                updated_rows.append(row)
                updated_csv_data = '\n'.join([','.join(csvreader.fieldnames)] + [','.join(row.values()) for row in updated_rows])
                with container_client.get_blob_client(blob_name) as blob_client:
                    blob_client.upload_blob(updated_csv_data, overwrite=True)
                    logger.warning("User details updated.")
        
     else:
        logger.info("User does not exist. Adding as a new user.")
        new_user_data = ','.join([name, str(age), gender])
        with container_client.get_blob_client(blob_name) as blob_client:
            downloaded_blob = blob_client.download_blob()
            csv_data = downloaded_blob.content_as_text()
            updated_csv_data = '\n'.join([csv_data, new_user_data])
            blob_client.upload_blob(updated_csv_data, overwrite=True)
        logger.warning("New user added successfully.")
    except Exception as e:
        logging.error(e)
        logging.error("An error occurred")

connection_string = "DefaultEndpointsProtocol=https;AccountName=salearn220623;AccountKey=96rxu4rzwCrz0hpaD/Dr9gvs5WJPXAo6pl1feH3xyth6Z9NCkWcNDzKwTuxU0/VMeOm9pJKWgEzO+AStWlnjiQ==;EndpointSuffix=core.windows.net"
container_name = "raw"
blob_name = "names.csv"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Example usage
name = input("Enter name: ")
age = int(input("Enter age: "))
gender = input("Enter gender (M/F): ")

add_user(name, age, gender, container_client, blob_name)
