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
def transform_and_write_parquet(container_name_source="raw", container_name_destination="cleanse"):
    blob_service_client = get_blob_service_client()
    blob_container_source = blob_service_client.get_container_client(container_name_source)
    blob_container_destination = blob_service_client.get_container_client(container_name_destination)
    blobs = blob_container_source.list_blobs()
    metadata_file = "./metadata.json"
    with open(metadata_file, "r") as json_file:
        metadata = json.load(json_file)
    for blob in blobs:
        blob_name = blob['name']
        blob_client_source = blob_container_source.get_blob_client(blob_name)
        blob_content = blob_client_source.download_blob().readall()
        df = pv.read_csv(BytesIO(blob_content))
        current_timestamp=datetime.now()
        timeseries_array=pa.array([current_timestamp]*len(df))
        table = df.append_column("Timeseries", timeseries_array)
        print("Done successfully")
        custom_metadata=metadata
        merged_metadata = {**custom_metadata, **(table.schema.metadata or {})}
        fixed_table = table.replace_schema_metadata(merged_metadata)
        blob_name_destination = "cleanse"
        blob_client_destination = blob_container_destination.get_blob_client(blob_name_destination)
        try:
            with BytesIO() as buf:
                pq.write_table(fixed_table, buf)
                buf.seek(0)
                blob_client_destination.upload_blob(data=buf.read(), content_settings=ContentSettings(content_type='application/octet-stream'))

            print(f"Data transformed and written as Parquet to the container '{container_name_destination}' with blob name '{blob_name_destination}'")
        except KeyError as e:
            print(f"Error: {e}")
            print(f"DataFrame columns: {df.columns}")
     
def list_blobs(container_name="cleanse"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    print("List of blobs in the container:")
    for blob in blobs:
        print(f"Blob name: {blob['name']}")

def clear_blobs(container_name="cleanse"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    for blob in blobs:
        blob_client = blob_container.get_blob_client(blob["name"])
        blob_client.delete_blob()
        print(f"Blob '{blob['name']}' deleted from the container '{container_name}'")
    print(f"All blobs deleted from the container '{container_name}'")
    
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
#     # clear_blobs()
