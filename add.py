#Reading names.csv from azure
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from io import StringIO
container_name = "raw"
blob_name = "names.csv"
token_credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(
    account_url="https://salearn220623.blob.core.windows.net/",token_credential=token_credential)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content=blob_client.download_blob().content_as_text()
csv_data=StringIO(blob_content)
df = pd.read_csv(csv_data)
df = pd.read_csv(csv_data)  #reads the csv data
print(df)
