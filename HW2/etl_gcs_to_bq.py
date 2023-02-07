from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """Load trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    df = pd.read_parquet(Path(f"data/{gcs_path}"))
   

@task()
def write_bq(df: pd.DataFrame):
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcampcred")

    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="warm-ring-375016",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    


@flow()
def etl_gcs_to_bq(month: int, year: int, color: str) -> int:
    """ETL flow to load data from one file into Big Query"""
   
    df = extract_from_gcs(color, year, month)
    write_bq(df)
    return len(df.index)
    
@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    """Main ETL flow to load data into Big Query"""
    rows_processed = 0
    for month in months:
        rows_processed += etl_gcs_to_bq(month, year, color)
    print (f"Rows processed: {rows_processed}")

if __name__ == "__main__":
    etl_parent_flow()
