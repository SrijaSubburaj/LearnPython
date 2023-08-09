from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient 
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import os

def source_upload():

    load_dotenv("Credentials_azure1.env")

    token_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url="https://demolpdblob.blob.core.windows.net",credential=token_credential)

    # Creating a container
    try:
        container_client1 = blob_service_client.create_container(name="source")
        container_client3 = blob_service_client.create_container(name="user-info")
    except ResourceExistsError:
        print('A container with this name already exists')

    # Uploading the logs
    container_client2 = blob_service_client.get_container_client(container="source")
    main_folder = r"C:\Users\ruv5cob\Desktop\Test_Azure\Source"
    list_of_files = os.listdir(main_folder)

    for each in list_of_files:
        with open(file=os.path.join(main_folder, each), mode="rb") as data:
            blob_client = container_client2.upload_blob(name=each, data=data)

    # Uploading the user-info file
    container_client4 = blob_service_client.get_container_client(container="user-info")
    path_to_user_info = r'C:\Users\ruv5cob\Desktop\My files\LPD\User_info.xlsx'

    with open(file=path_to_user_info, mode="rb") as data:
        blob_client = container_client4.upload_blob(name="User_info.xlsx", data=data)

    return True