import os
import pandas as pd
from datetime import datetime
from connection import get_blob_service_client
from azure.storage.blob import BlobServiceClient, ContentSettings
import json
from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.csv as pv
from prefect import get_run_logger

# Function to transform a CSV file from one container, add a custom column, and write it as Parquet to another container
def transform_and_write_parquet(container_name_source="raw", container_name_destination="cleanse"):
    logger = get_run_logger()  
    # Get the Blob Service Client
    blob_service_client = get_blob_service_client()
    # Get the container clients for source and destination
    blob_container_source = blob_service_client.get_container_client(container_name_source)
    blob_container_destination = blob_service_client.get_container_client(container_name_destination)
    # List all blobs in the source container
    blobs = blob_container_source.list_blobs()
    # Path to metadata file
    metadata_file = "./metadata.json"
    # Load metadata from the JSON file
    with open(metadata_file, "r") as json_file:
        metadata = json.load(json_file)
    # Iterate through each blob in the source container
    for blob in blobs:
        blob_name = blob['name']
        # Get the blob client for the source blob
        blob_client_source = blob_container_source.get_blob_client(blob_name)
        # Download the blob content
        blob_content = blob_client_source.download_blob().readall()
        # Read CSV content into a Pandas DataFrame
        df = pd.read_csv(BytesIO(blob_content))
        # Add a custom column 'Timeseries' with current timestamp
        current_timestamp = datetime.now()
        timeseries_array = pa.array([current_timestamp] * len(df))
        table = pa.table({**df.to_dict('list'), 'Timeseries': timeseries_array})
        # Log and print information about the successful addition of the custom column
        print("Custom Column added successfully")
        logger.info("Custom Column added successfully")
        # Merge metadata from JSON file with existing schema metadata
        custom_metadata = metadata
        merged_metadata = {**custom_metadata, **(table.schema.metadata or {})}
        fixed_table = table.replace_schema_metadata(merged_metadata)
        # Specify the destination blob name
        blob_name_destination = blob_name.replace("raw", "cleanse").replace(".csv", ".parquet")
        # Get the blob client for the destination blob
        blob_client_destination = blob_container_destination.get_blob_client(blob_name_destination) 
        try:
            # Write the Parquet table to a BytesIO buffer
            with BytesIO() as buf:
                pq.write_table(fixed_table, buf)
                buf.seek(0)
                # Upload the Parquet data to the destination blob
                blob_client_destination.upload_blob(data=buf.read(), overwrite=True, content_settings=ContentSettings(content_type='application/octet-stream'))
                # Log and print information about the successful transformation and write
                print(f"Data transformed and written as Parquet to the container '{container_name_destination}' with blob name '{blob_name_destination}'")
                logger.info(f"Data transformed and written as Parquet to the container '{container_name_destination}' with blob name '{blob_name_destination}'")
        except KeyError as e:
            # Handle KeyErrors (if any) and log information about the error
            print(f"Error: {e}")
            logger.info(f"Error: {e}")
            print(f"DataFrame columns: {df.columns}")
            logger.error(f"DataFrame columns: {df.columns}")

# Function to list all blobs in a specified container
def list_blobs(container_name="cleanse"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    print("List of blobs in the container:")
    for blob in blobs:
        print(f"Blob name: {blob['name']}")
        
# Function to delete all blobs in a specified container
def clear_blobs(container_name="cleanse"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    for blob in blobs:
        blob_client = blob_container.get_blob_client(blob["name"])
        blob_client.delete_blob()
        print(f"Blob '{blob['name']}' deleted from the container '{container_name}'")
    print(f"All blobs deleted from the container '{container_name}'")
   
# Checking the parquet file about the timestamp data and metadata 
def view_parquet_head_and_metadata(blob_service_client, container_name="cleanse"):
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    for blob in blobs:
        blob_name = blob['name']
        blob_client = blob_container.get_blob_client(blob_name)
        blob_content = blob_client.download_blob().readall()
        table = pq.read_table(BytesIO(blob_content))
        df = table.to_pandas()
        print(f"Head of Parquet file '{blob_name}':")
        print(df.head())
        print("Metadata:")
        metadata=table.schema.metadata
        print(metadata)
        
# if __name__ == "__main__":
#     # transform_and_write_parquet()
#     # list_blobs()
#     # view_parquet_head_and_metadata(get_blob_service_client(), container_name="cleanse")
#     clear_blobs()
