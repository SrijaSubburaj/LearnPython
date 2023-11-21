import os
from connection import get_blob_service_client
from azure.storage.blob import BlobClient, BlobServiceClient
import requests
import wget
def download_and_ingest(url, container_name="landing"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    file_name = url.split("/")[-1]
    response=requests.get(url,stream=True)
    blob_client = blob_container.get_blob_client(file_name)
    blob_client.upload_blob(data=response.content, overwrite=True)
    print(f"Zip file '{file_name}' uploaded to the container '{container_name}'")
    blob_exists = blob_client.exists()
    if blob_exists:
        print(f"Blob '{file_name}' exists in the container '{container_name}'.")
    else:
        print(f"Blob '{file_name}' does not exist in the container '{container_name}'.")

def list_blobs(container_name="landing"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    print("List of blobs in the container LANDING:")
    for blob in blobs:
        print(f"Blob name: {blob['name']}")

def clear_blobs(container_name="landing"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    for blob in blobs:
        blob_client = blob_container.get_blob_client(blob["name"])
        blob_client.delete_blob()
        print(f"Blob '{blob['name']}' deleted from the container '{container_name}'")
    print(f"All blobs deleted from the container '{container_name}'")


# if __name__ == "__main__":
#     # data_source_url = "https://github.com/datablist/sample-csv-files/raw/main/files/customers/customers-100.zip"
#     # download_and_ingest(data_source_url)
#     # list_blobs()
#     clear_blobs()
