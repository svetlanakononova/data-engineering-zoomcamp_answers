from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import urllib.request 


@task(log_prints=True)
def write_local_parquet_from_https(year: int, month: int) -> Path:
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    df = pd.read_csv(dataset_url)
    path = Path(f"fhv_tripdata/{dataset_file}.parquet")
    path.parent.mkdir(parents = True, exist_ok = True)
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def write_local_from_https(year: int, month: int) -> Path:
    dataset_file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    path = Path(f"fhv_tripdata/{dataset_file}")
    path.parent.mkdir(parents = True, exist_ok = True)
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}"
    urllib.request.urlretrieve(dataset_url, path)
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(log_prints=True)
def etl_tripdata_to_gcs(
    params: dict = {2019: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 
    2020: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    2021: [1, 2, 3, 4, 5, 6, 7],
    },
    format: str = 'parquet'
):
    for year in params:
        for month in params[year]:
            path = (write_local_parquet_from_https(year, month) if format == 'parquet' else write_local_from_https (year, month)) 
            write_gcs(path)
        
 
if __name__ == "__main__":
    etl_tripdata_to_gcs()