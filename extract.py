import os
from connection import get_blob_service_client
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from io import BytesIO
import zipfile

def extract_and_move(container_name_source="landing", container_name_destination="raw"):
    blob_service_client = get_blob_service_client()
    blob_container_source = blob_service_client.get_container_client(container_name_source)
    blob_container_destination = blob_service_client.get_container_client(container_name_destination)
    blobs = blob_container_source.list_blobs()
    for blob in blobs:
        blob_name = blob['name']
        blob_client_source = blob_container_source.get_blob_client(blob_name)
        zip_content = blob_client_source.download_blob().readall()
        with zipfile.ZipFile(BytesIO(zip_content), 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                file_content = zip_ref.read(file_info.filename)
                folder_name = os.path.dirname(file_info.filename)
                blob_name_destination = os.path.join(folder_name, os.path.basename(file_info.filename))
                blob_client_destination = blob_container_destination.get_blob_client(blob_name_destination)
                blob_client_destination.upload_blob(data=file_content, content_settings=ContentSettings(content_type='text/plain'))
                print(f"File '{file_info.filename}' extracted and moved to the container '{container_name_destination}'")
    print("Extraction and move completed.")

def list_blobs(container_name="raw"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    print("List of blobs in the container RAW :")
    for blob in blobs:
        print(f"Blob name: {blob['name']}")

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