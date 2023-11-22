from prefect import Flow, task, get_run_logger
from ingest import download_and_ingest
from extract import extract_and_move
from cleanse import transform_and_write_parquet
from curate import schema_validation
from pandera.errors import SchemaError
from datetime import datetime

@task
def ingest(name="Ingest"):
    """
    Task to download and ingest data.
    """
    download_and_ingest(url="https://github.com/datablist/sample-csv-files/raw/main/files/customers/customers-100.zip")

@task
def extract(name="Extract"):
    """
    Task to extract and move data.
    """
    extract_and_move()

@task
def cleanse(name="Cleanse"):
    """
    Task to cleanse and transform data.
    """
    transform_and_write_parquet()

@task
def curate(name="Curate"):
    """
    Task to curate and validate data schema.
    """
    schema_validation()

@Flow
# Running the task in order manner
def automation():
    try:
        logger = get_run_logger()
        process_1 = ingest()
        process_2 = extract(wait_for=[process_1])
        process_3 = cleanse(wait_for=[process_2])
        process_4 = curate(wait_for=[process_3])
        logger.info("Automation completed successfully at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    except Exception as e:
        logger.error("Automation failed at %s. Exception: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(e))

if __name__ == "__main__":
    automation()
