import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import yaml
import os
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient 
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

def data_cleansing():

    # Accessing the azure storage account
    load_dotenv("Credentials_azure1.env")
    token_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url="https://demolpdblob.blob.core.windows.net",credential=token_credential)

    # Creating a container for paraquet files
    try:
        container_client1 = blob_service_client.create_container(name="02-cleanse")
    except ResourceExistsError:
        print('Container with this name already exists')

    # fetching the list of blobs
    container_client2 = blob_service_client.get_container_client(container="source")
    blob_list = container_client2.list_blobs()
    for each in blob_list:
        
        # Download logs file
        blob_client_instance1 = blob_service_client.get_blob_client("01-ingest-logs", "ingest_" + each.name, snapshot=None)
        with open("ingest_"+ each.name, "wb") as my_blob1:
            blob_data1 = blob_client_instance1.download_blob()
            blob_data1.readinto(my_blob1)

        # Download meta file
        blob_client_instance2 = blob_service_client.get_blob_client("01-ingest-meta", each.name.split('.')[0]+".yaml", snapshot=None)
        with open(each.name.split('.')[0]+".yaml", "wb") as my_blob2:
            blob_data2 = blob_client_instance2.download_blob()
            blob_data2.readinto(my_blob2)

        df = pd.read_csv("ingest_"+ each.name)
        os.remove("ingest_"+ each.name) # remove the downloaded blob csv file

        yaml_file = each.name.split('.')[0]+".yaml"
        with open(yaml_file, 'r') as file:
            meta_info = yaml.safe_load(file)
        os.remove (each.name.split('.')[0]+".yaml") # remove the downloaded blob yaml file

        table = pa.Table.from_pandas(df)

        #meta data updation
        custom_meta_key = each.name.split('.')[0]
        custom_meta_json = json.dumps(meta_info)
        existing_meta = table.schema.metadata
        combined_meta = {custom_meta_key.encode() : custom_meta_json.encode(),**existing_meta}
        table = table.replace_schema_metadata(combined_meta)
        
        pq.write_table(table, each.name.split('.')[0] + ".parquet", compression='GZIP')

        # uploading the parquet file
        container_client3 = blob_service_client.get_container_client(container="02-cleanse")
        with open(file=each.name.split('.')[0]+".parquet", mode="rb") as data:
                blob_client = container_client3.upload_blob(name=each.name.split('.')[0] + ".parquet", data=data)
        
        os.remove(each.name.split('.')[0]+".parquet")

    return True