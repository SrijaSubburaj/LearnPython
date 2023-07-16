import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from io import StringIO
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url="https://salearn220623.blob.core.windows.net/",credential="96rxu4rzwCrz0hpaD/Dr9gvs5WJPXAo6pl1feH3xyth6Z9NCkWcNDzKwTuxU0/VMeOm9pJKWgEzO+AStWlnjiQ==")
  
def convert_csv_to_parquet(parquet_file):
    blob_client = blob_service_client.get_blob_client(container="raw", blob=csv_file)
    csv_data = blob_client.download_blob().content_as_text()
    data = pd.read_csv(StringIO(csv_data))
    data.to_parquet(parquet_file)
    logging.info("Data converted to Parquet file")

    blob_client = blob_service_client.get_blob_client(container="cleansed", blob=parquet_file)
    try:
        with open(parquet_file, "rb") as file:
            blob_client.upload_blob(file)
    except :
         with open(parquet_file, "rb") as file:
            blob_client.upload_blob(file,overwrite=True)
         logging.info("Parquet file uploaded to cleansed")

csv_file= "names.csv"
parquet_file = "names.parquet"

convert_csv_to_parquet(parquet_file)

