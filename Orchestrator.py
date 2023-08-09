from prefect import task,flow
from Data_upload import source_upload
from Data_ingest import data_ingestion
from Data_cleanse import data_cleansing
from Data_curate import data_curation

@task
def step1():
    source_upload()
    return True

@task
def step2():
    data_ingestion()
    return True

@task
def step3():
    data_cleansing()
    return True

@task
def step4():
    data_curation()
    return True

@flow
def main_flow():
    t1 = step1()
    print("Data upload step successful")
    t2 = step2(wait_for=[t1])
    print("Data ingestion step successful")
    t3 = step3(wait_for=[t2])
    print("Data cleansing step successful")
    t4 = step4(wait_for=[t3])
    print("Data curation step successful")

main_flow()