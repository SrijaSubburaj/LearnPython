import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import yaml
import os

def cleaning_step():
    main_path = r'C:\Users\ruv5cob\Desktop\Test\Laptop_position_detection\v1\data'
    list_of_files = os.listdir(os.path.join(main_path,"DP01_Data_Ingestion","01_Logs"))
    for each in list_of_files:
        df = pd.read_csv(os.path.join(main_path,"DP01_Data_Ingestion","01_Logs",each))
        yaml_file = os.path.join(main_path,"DP01_Data_Ingestion","02_Meta",each.split('.')[0]) + ".yaml"
        with open(yaml_file, 'r') as file:
            meta_info = yaml.safe_load(file)
        
        table = pa.Table.from_pandas(df)

        #meta data updation
        custom_meta_key = each.split('.')[0]
        custom_meta_json = json.dumps(meta_info)
        existing_meta = table.schema.metadata
        combined_meta = {custom_meta_key.encode() : custom_meta_json.encode(),**existing_meta}
        table = table.replace_schema_metadata(combined_meta)
        
        pq.write_table(table, os.path.join(main_path,"DP02_Data_cleansing",each.split('.')[0]) + ".parquet", compression='GZIP')

    return True