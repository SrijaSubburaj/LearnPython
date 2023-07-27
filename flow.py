from prefect import task,flow
from ingest import ingestion_step 
from cleanse import cleaning_step
from curate import validation_step

@task
def step1():
    ingestion_step()
    return True

@task
def step2():
    cleaning_step()
    return True

@task
def step3():
    validation_step()
    return True

@flow
def main_flow():
    t1 = step1()
    t2 = step2(wait_for=[t1])
    t3 = step3(wait_for=[t2])

main_flow()