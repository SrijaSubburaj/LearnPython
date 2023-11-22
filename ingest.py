import os
from connection import get_blob_service_client
from azure.storage.blob import BlobClient, BlobServiceClient
import requests
import wget
from prefect import get_run_logger

# Function to download a file from a URL and upload it to Azure Blob Storage
def download_and_ingest(url, container_name="landing"):
    logger = get_run_logger()
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    # Extracting file name from the URL
    file_name = url.split("/")[-1]
    # Downloading the file from the URL
    response = requests.get(url, stream=True)
    # Logging information about the successful download
    logger.info(f"Zip file '{file_name}' downloaded successfully")
    # Uploading the file to Azure Blob Storage
    blob_client = blob_container.get_blob_client(file_name)
    blob_client.upload_blob(data=response.content, overwrite=True)
    # Logging information about the successful upload
    print(f"Zip file '{file_name}' uploaded to the container '{container_name}'")
    logger.info(f"Zip file '{file_name}' uploaded to the container '{container_name}'")
    # Checking if the blob exists in the container
    blob_exists = blob_client.exists()
    if blob_exists:
        print(f"Blob '{file_name}' exists in the container '{container_name}'.")
    else:
        print(f"Blob '{file_name}' does not exist in the container '{container_name}'.")

# Function to list all blobs in a specified container
def list_blobs(container_name="landing"):
    logger = get_run_logger()
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    # Listing all blobs in the container
    blobs = blob_container.list_blobs()
    # Printing and logging information about each blob
    print("List of blobs in the container LANDING:")
    logger.info("List of blobs in the container LANDING:")
    for blob in blobs:
        print(f"Blob name: {blob['name']}")
        logger.info(f"Blob name: {blob['name']}")


# Function to delete all blobs in a specified container
def clear_blobs(container_name="landing"):
    logger = get_run_logger()
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    # Listing all blobs in the container
    blobs = blob_container.list_blobs()
    # Deleting each blob in the container
    for blob in blobs:
        blob_client = blob_container.get_blob_client(blob["name"])
        blob_client.delete_blob()
        print(f"Blob '{blob['name']}' deleted from the container '{container_name}'")
        logger.info(f"Blob '{blob['name']}' deleted from the container '{container_name}'")
    # Logging information about the deletion of all blobs
    print(f"All blobs deleted from the container '{container_name}'")
    logger.info(f"All blobs deleted from the container '{container_name}'")


# Main block of code to test the functions
if __name__ == "__main__":
    # Uncomment and provide a data source URL to test the download_and_ingest function
    # data_source_url = "https://github.com/datablist/sample-csv-files/raw/main/files/customers/customers-100.zip"
    # download_and_ingest(data_source_url)
    # Testing the list_blobs function
    list_blobs()
    # Uncomment to test the clear_blobs function
    # clear_blobs()
