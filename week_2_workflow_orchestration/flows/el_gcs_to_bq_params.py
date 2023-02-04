from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="de_zoomcamp_mb_01.rides",
        project_id="taxi-rides-ny-375719",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df =  pd.read_parquet(path)
    write_bq(df)
    return len(df)



@flow(log_prints=True)
def el_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    total_count = 0
    for month in months:
        total_count = total_count + etl_gcs_to_bq(year, month, color)
    print(f"TOTAL ROWS: {total_count}")



if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    el_parent_flow(months, year, color)
