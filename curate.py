import pandas as pd
import pandera as pa
import json
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
