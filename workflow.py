from prefect import Flow,task

from ingest import download_and_ingest
from extract import extract_and_move
from cleanse import transform_and_write_parquet
from curate import schema_validation

@task
def task1(name="Ingest"):
    download_and_ingest(url="https://github.com/datablist/sample-csv-files/raw/main/files/customers/customers-100.zip")
@task   
def task2(name="Extract"):
    extract_and_move()
@task    
def task3(name="Cleanse"):
    transform_and_write_parquet()
@task    
def task4(name="Curate"):
    schema_validation()
    
@Flow
def flow_final():
    process_1=task1()
    process_2=task2(wait_for=[process_1])
    process_3=task3(wait_for=[process_2])
    process_4=task4(wait_for=[process_3])
    
if __name__=="__main__":
    flow_final()