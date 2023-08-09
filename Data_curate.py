import pandas as pd
import os
import pyarrow
import pandera as pa
import os
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient 
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

def data_curation():

    # Accessing the azure storage account
    load_dotenv("Credentials_azure1.env")
    token_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url="https://demolpdblob.blob.core.windows.net",credential=token_credential)

    # Creating a container for curated files
    try:
        container_client1 = blob_service_client.create_container(name="03-curated")
    except ResourceExistsError:
        print('Container with this name already exists')

    # fetching the list of blobs
    container_client2 = blob_service_client.get_container_client(container="02-cleanse")
    blob_list = container_client2.list_blobs()
    for each in blob_list:
        
        # Download parquet file
        blob_client_instance1 = blob_service_client.get_blob_client("02-cleanse", each.name, snapshot=None)
        with open(each.name, "wb") as my_blob1:
            blob_data1 = blob_client_instance1.download_blob()
            blob_data1.readinto(my_blob1)

        # Schema used for validation
        schema = {"^.Accel": pa.Column(float, pa.Check.in_range(min_value=-156.8,max_value=156.8),regex=True),
                "^.Gyro" : pa.Column(float, pa.Check.in_range(min_value=-2000,max_value=2000),regex=True)}
        df_schema = pa.DataFrameSchema(schema,strict='filter')

        df = pd.read_parquet(each.name, engine='pyarrow')

        container_client3 = blob_service_client.get_container_client(container="03-curated")
        if len(df_schema.validate(df)) == len(df):
            # uploading the validated file
            with open(file=each.name, mode="rb") as data:
                    blob_client = container_client3.upload_blob(name="passed_"+each.name, data=data)
        else:
            # uploading the unvalidated file
            with open(file=each.name, mode="rb") as data:
                    blob_client = container_client3.upload_blob(name="failed_"+each.name, data=data)
        
        os.remove(each.name) # removing the downloaded parquet file

    return True