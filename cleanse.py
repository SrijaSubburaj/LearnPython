import datetime
import pyarrow as pa
import json

def cleanse():
    # Path to the metadata JSON file
    metadata_file = "./metadata.json"
    # Read metadata from JSON file
    with open(metadata_file, "r") as json_file:
        metadata = json.load(json_file)
    # Get the current time
    current_time = datetime.datetime.now()

    # Directory path for the CSV file
    csv_directory = "./raw/customers-100.csv"
    
    # Read CSV file into a PyArrow Table
    table = pa.csv.read_csv(csv_directory)

    # Add a new column "Timeseries" with the current timestamp to the Table
    timeseries_array = pa.array([current_time] * len(table), type=pa.timestamp('us'))
    table = table.append_column("Timeseries", timeseries_array)

    # Print the metadata from the original CSV file (if any)
    print("Metadata in CSV: ", table.schema.metadata)

    # Merge the custom metadata with the existing metadata (if any)
    custom_metadata = metadata
    merged_metadata = {**custom_metadata, **(table.schema.metadata or {})}

    # Replace the schema metadata in the PyArrow Table with the merged metadata
    fixed_table = table.replace_schema_metadata(merged_metadata)

    # Write the PyArrow Table to a Parquet file with the merged metadata
    print("Saved as Parquet successfully")
    pa.parquet.write_table(fixed_table, './cleanse/cleanse.parquet')

    # Read the Parquet file into a PyArrow Table
    parquet_table = pa.parquet.read_table('./cleanse/cleanse.parquet')
    # Print the final metadata in the Parquet file
    final_metadata = parquet_table.schema.metadata
    print("Metadata of the Parquet file: ", final_metadata)

