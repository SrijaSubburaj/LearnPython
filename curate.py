import pandas as pd
import pandera as pa
import json

def Curate():
    # Path to the initial Parquet file
    initial_directory = "./cleanse/cleanse.parquet"
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(initial_directory, engine='pyarrow')
    # Define the expected schema for the DataFrame using Pandera
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
        # Validate DataFrame against the defined schema
        valid_records = schema.validate(df)
        # Path to the curated Parquet file
        curated_directory = "./curated/curated.parquet"
        # Write the curated DataFrame to a new Parquet file
        valid_records.to_parquet(curated_directory, engine='pyarrow', index=False)
        # Display the first few rows of the curated DataFrame
        print(valid_records.head())
        # Print a success message
        print("Defined validation is successful")
    except Exception as e:
        # Print an error message if validation fails
        print("The validation failed because of:", e)
