import pandas as pd
import pandera as pa
directory = "./cleanse/cleanse.parquet"
df = pd.read_parquet(directory, engine='pyarrow')
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
    curated_directory = "./curated/curated.parquet"
    valid_records.to_parquet(curated_directory, engine='pyarrow', index=False)
    print(valid_records.head())
    print("Defined validation is success")
except Exception as e:
    print("The validation failed because of : ",e)
