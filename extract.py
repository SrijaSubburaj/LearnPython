import os
from connection import get_blob_service_client
from azure.storage.blob import BlobClient, BlobServiceClient, ContentSettings
from prefect import get_run_logger
from io import BytesIO
import zipfile

# Function to extract files from a zip archive in one container and move them to another container
def extract_and_move(container_name_source="landing", container_name_destination="raw"):
    logger = get_run_logger()
    # Get the Blob Service Client
    blob_service_client = get_blob_service_client()
    # Get the container clients for source and destination
    blob_container_source = blob_service_client.get_container_client(container_name_source)
    blob_container_destination = blob_service_client.get_container_client(container_name_destination)
    # List all blobs in the source container
    blobs = blob_container_source.list_blobs() 
    # Iterate through each blob in the source container
    for blob in blobs:
        blob_name = blob['name']
        # Get the blob client for the source blob
        blob_client_source = blob_container_source.get_blob_client(blob_name)
        # Download the zip content
        zip_content = blob_client_source.download_blob().readall()
        # Extract files from the zip archive
        with zipfile.ZipFile(BytesIO(zip_content), 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                # Read the content of each file in the zip archive
                file_content = zip_ref.read(file_info.filename)
                # Get folder and file names
                folder_name = os.path.dirname(file_info.filename)
                blob_name_destination = os.path.join(folder_name, os.path.basename(file_info.filename))
                # Get the blob client for the destination blob
                blob_client_destination = blob_container_destination.get_blob_client(blob_name_destination)
                # Upload the file content to the destination blob
                blob_client_destination.upload_blob(data=file_content, overwrite=True, content_settings=ContentSettings(content_type='text/plain'))
                # Log and print information about the file extraction and move
                print(f"File '{file_info.filename}' extracted and moved to the container '{container_name_destination}'")
                logger.info(f"File '{file_info.filename}' extracted and moved to the container '{container_name_destination}'")
    # Log and print information about the completion of extraction and move
    print("Extraction and move completed.")
    logger.info("Extraction and move completed.")

# Function to list all blobs in a specified container
def list_blobs(container_name="raw"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    print("List of blobs in the container RAW :")
    for blob in blobs:
        print(f"Blob name: {blob['name']}")
        
# Function to delete all blobs in a specified container
def clear_blobs(container_name="raw"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    for blob in blobs:
        blob_client = blob_container.get_blob_client(blob["name"])
        blob_client.delete_blob()
        print(f"Blob '{blob['name']}' deleted from the container '{container_name}'")
    print(f"All blobs deleted from the container '{container_name}'")

# if __name__ == "__main__":
#     # extract_and_move()
#     # list_blobs("raw")
#     clear_blobs("raw")  