import pandas as pd
import os
import pyarrow
import pandera as pa
import shutil

def validation_step():
    schema = {"^.Accel": pa.Column(float, pa.Check.in_range(min_value=-156.8,max_value=156.8),regex=True),
            "^.Gyro" : pa.Column(float, pa.Check.in_range(min_value=-2000,max_value=2000),regex=True)}
    df_schema = pa.DataFrameSchema(schema,strict='filter')

    main_path = r'C:\Users\ruv5cob\Desktop\Test\Laptop_position_detection\v1\data'
    list_of_files = os.listdir(os.path.join(main_path,"DP02_Data_cleansing"))
    for each in list_of_files:
    
        df = pd.read_parquet(os.path.join(main_path,"DP02_Data_cleansing",each), engine='pyarrow')
        if len(df_schema.validate(df)) == len(df):
            source = os.path.join(main_path,"DP02_Data_cleansing",each)
            destination = os.path.join(main_path,"DP03_Data_validation",each)
            shutil.copy(source,destination)
        
        print(each + " - Success")

    return True