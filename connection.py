import os
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv


load_dotenv()

def get_blob_service_client():
    try:
        account_name = os.getenv("ACCOUNT_NAME")
        account_key = os.getenv("ACCOUNT_KEY")
        connection_string = connection_string = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
        print("Connection Successfull")
        return BlobServiceClient.from_connection_string(connection_string)
    except Exception as e:
        print(e)
if __name__ == "__main__":
    get_blob_service_client()