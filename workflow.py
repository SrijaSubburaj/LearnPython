from prefect import Flow, task
import zipfile
import wget
import datetime
import json
import pandas as pd
import pandera as pa
# Import custom tasks from separate modules
from ingest import Ingest
from extract import Extract
from cleanse import Cleanse
from curate import Curate
# Task 1: Ingest data
@task
def task1():
    Ingest()
# Task 2: Extract data
@task
def task2():
    Extract()
# Task 3: Cleanse data
@task
def task3():
    Cleanse()
# Task 4: Curate data
@task
def task4():
    Curate()
# Define the Prefect flow
@Flow
def flow():
    # Define the sequence of tasks and their dependencies
    process_1 = task1()
    process_2 = task2(wait_for=[process_1])
    process_3 = task3(wait_for=[process_2])
    process_4 = task4(wait_for=[process_3])
if __name__ == "__main__": 
    flow()
