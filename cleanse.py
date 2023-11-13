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
for key, value in metadata.items():
    csv[key] = value
csv["Timeseries"] = current_time
csv.to_parquet(to_directory, index=False)
print("Author:", csv["author"].iloc[0])
print("Description:", csv["description"].iloc[0])
print("Created At:", csv["created_at"].iloc[0])
parq = pd.read_parquet('./cleanse/cleanse.parquet', engine='fastparquet')
print(parq.head())