from pathlib import Path
import requests
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3)
def write_local(df: pd.DataFrame, path: str) -> Path:
    """Write data from pandas DataFrame into parquet file"""
    parquet_path = Path(f"data/fhv/{path}")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    # NOTE the stream=True parameter below
    with open(parquet_path, 'wb') as f:
        df.to_parquet()
    return parquet_path

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    month_str = "{:02d}".format(month)
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month_str}.csv.gz"
    df = fetch(dataset_url)
    parquet_path = write_local(df, f"fhv_tripdata_{year}-{month_str}.parquet")
    write_gcs(parquet_path)


@flow()
def etl_parent_flow(
    months: list[int] = None, years: list[int] = None
):
    if not months:
        months = list(range(1,13))
    if not years:
        years = [2019, 2020, 2021]

    for year in years:
        for month in months:
            etl_web_to_gcs(year, month)


if __name__ == "__main__":
    etl_parent_flow(years=[2020])
