from prefect import Flow, task
import zipfile
import wget
import datetime
import json
import pandas as pd
import pandera as pa
@task
def ingest(url,directory):
    wget.download(url, out=directory)
    
@task
def extract(fetch,move):
    archive = zipfile.ZipFile(fetch)
    for file in archive.namelist():
            archive.extract(file, move)
            
@task
def cleanse():
   metadata_file = "./metadata.json"
   with open(metadata_file, "r") as json_file:
       metadata = json.load(json_file)
   current_time = datetime.datetime.now()
   directory = "./raw/customers-100.csv"
   to_directory = "./cleanse/cleanse.parquet"
   csv = pd.read_csv(directory)
   for key, value in metadata.items():
       csv[key] = value
   csv["Timeseries"] = current_time
   csv.to_parquet(to_directory, index=False)
   print("Author:", csv["author"].iloc[0])
   print("Description:", csv["description"].iloc[0])
   print("Created At:", csv["created_at"].iloc[0])
   parq = pd.read_parquet('./cleanse/cleanse.parquet', engine='fastparquet')
   print(parq.head())
    
@task
def curate():
   metadata_file = "./metadata.json"
   with open(metadata_file, "r") as json_file:
       metadata = json.load(json_file)
   initial_directory = "./cleanse/cleanse.parquet"
   df = pd.read_parquet(initial_directory, engine='pyarrow')
   schema = pa.DataFrameSchema({
       "Index": pa.Column(int),
       "Customer Id": pa.Column(str),
       "First Name": pa.Column(str),
       "Last Name": pa.Column(str),
       "Company": pa.Column(str),
       "City": pa.Column(str),
       "Country": pa.Column(str),
       "Phone 1": pa.Column(object),
       "Phone 2": pa.Column(object),
       "Email": pa.Column(str),
       "Subscription Date": pa.Column(object),
       "Website": pa.Column(str),
       "Timeseries": pa.Column(pd.Timestamp)
   })
   try:
       valid_records = schema.validate(df)
       for key, value in metadata.items():
           valid_records[key] = value
       curated_directory = "./curated/curated.parquet"
       valid_records.to_parquet(curated_directory, engine='pyarrow', index=False)
       print(valid_records.head())
       print("Defined validation is successful")
   except Exception as e:
       print("The validation failed because of: ", e)

@Flow
def final():
    process_1=ingest("https://github.com/datablist/sample-csv-files/raw/main/files/customers/customers-100.zip", "./landing/")
    process_2=extract('./landing/customers-100.zip',"./raw/")
    process_3=cleanse()
    process_4=curate()

if __name__ == "__main__":
    final()
