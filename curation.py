import pandas as pd
import pandera as pa
import logging
from azure.storage.blob import BlobServiceClient
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
connection_string="DefaultEndpointsProtocol=https;AccountName=salearn220623;AccountKey=96rxu4rzwCrz0hpaD/Dr9gvs5WJPXAo6pl1feH3xyth6Z9NCkWcNDzKwTuxU0/VMeOm9pJKWgEzO+AStWlnjiQ==;EndpointSuffix=core.windows.net"
container_name="curated"
curated_parquet_file="curated.parquet"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client =blob_service_client.get_blob_client(container=container_name, blob=curated_parquet_file)


schema = pa.DataFrameSchema({
    "Name": pa.Column(pa.String, checks=pa.Check.str_matches(r'^[A-Z][a-zA-Z]*$')),
    "Age": pa.Column(pa.Int, checks=[pa.Check.greater_than_or_equal_to(1), pa.Check.less_than_or_equal_to(100)]),
    "Gender": pa.Column(pa.String, checks=pa.Check.isin(["M", "F"]))
})

def add_user(name, age, gender):
    data = pd.DataFrame({'Name': [name], 'Age': [age], 'Gender': [gender]})

    try:
        schema.validate(data)
        data.to_parquet('curated_parquet_file')
        
        with open(curated_parquet_file, "rb") as file:
            blob_client.upload_blob(file)
            logging.info("Data uploaded to curated")
        
        
    except pa.errors.SchemaError as e:
        logging.error("Schema validation failed.")
        logging.error(e)
        data.to_parquet('failed.parquet')


name = input("Enter name: ")
age = int(input("Enter age: "))
gender = input("Enter gender (M/F): ")


add_user(name, age, gender)

