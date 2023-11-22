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
import pandera as pand
from prefect import get_run_logger
from pandera.errors import SchemaError

def schema_validation(container_name_source="cleanse", container_name_destination="curate"):
    logger=get_run_logger()
    blob_service_client = get_blob_service_client()
    blob_container_source = blob_service_client.get_container_client(container_name_source)
    blob_container_destination = blob_service_client.get_container_client(container_name_destination)
    blobs = blob_container_source.list_blobs()
    for blob in blobs:
        blob_name=blob['name']
    blob_client_source = blob_container_source.get_blob_client(blob_name)
    blob_content = blob_client_source.download_blob().readall()
    parq=pd.read_parquet(BytesIO(blob_content),engine="pyarrow")
    schema = pand.DataFrameSchema({
        "Index": pand.Column(int),
        "Customer Id": pand.Column(str),
        "First Name": pand.Column(str),
        "Last Name": pand.Column(str),
        "Company": pand.Column(str),
        "City": pand.Column(str),
        "Country": pand.Column(str),
        "Phone 1": pand.Column(object),
        "Phone 2": pand.Column(object),
        "Email": pand.Column(str),
        "Subscription Date": pand.Column(object),
        "Website": pand.Column(str),
        "Timeseries": pand.Column(pd.Timestamp)
    })
      
    try:
        # Validate DataFrame against the defined schema
        valid_records = schema.validate(parq)    
        # Print a success message
        print("Defined validation is successful")
        logger.info("Defined validation is successful")
        blob_name_destination = "curate"
        blob_client_destination = blob_container_destination.get_blob_client(blob_name_destination)
        try:
                with BytesIO() as buf:
                    valid_records.to_parquet(buf,engine='pyarrow',index=False)
                    buf.seek(0)
                    blob_client_destination.upload_blob(data=buf.read(),overwrite=True, content_settings=ContentSettings(content_type='application/octet-stream'))
                print(f"written as Parquet to the container '{container_name_destination}' with blob name '{blob_name_destination}'")
                logger.info(f"written as Parquet to the container '{container_name_destination}' with blob name '{blob_name_destination}'.parquet")
        except KeyError as e:
                print(f"Error: {e}")
                logger.error(f"Error: {e}")
    except SchemaError as e:
        print("Valdiation Failed in ",e.schema)
        affected_records = parq.loc[e.failure_cases.index]
        print("Affected Records: ", affected_records)
        print(affected_records.head(3))        
        # logger.error("Validation Failed",e)

def list_blobs(container_name="curate"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    print("List of blobs in the container Curate :")
    for blob in blobs:
        print(f"Blob name: {blob['name']}")
        
def clear_blobs(container_name="curate"):
    blob_service_client = get_blob_service_client()
    blob_container = blob_service_client.get_container_client(container_name)
    blobs = blob_container.list_blobs()
    for blob in blobs:
        blob_client = blob_container.get_blob_client(blob["name"])
        blob_client.delete_blob()
        print(f"Blob '{blob['name']}' deleted from the container '{container_name}'")
    print(f"All blobs deleted from the container '{container_name}'")

    

# if __name__=="__main__":
#     # schema_validation()
#     # list_blobs()
#     clear_blobs()