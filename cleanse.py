import pandas as pd
import datetime
import json
metadata_file = "./metadata.json"
with open(metadata_file, "r") as json_file:
    metadata = json.load(json_file)
current_time = datetime.datetime.now()
directory = "./raw/customers-100.csv"
to_directory = "./cleanse/cleanse.parquet"
csv = pd.read_csv(directory)
print(csv.head())
csv["Timeseries"] = current_time
for key, value in metadata.items():
    setattr(csv, key, value)
csv.to_parquet(to_directory)
print("Author:", csv.author)
print("Description:", csv.description)
print("Created At:", csv.created_at)
parq=pd.read_parquet('./cleanse/cleanse.parquet', engine='fastparquet')
print(parq.head())