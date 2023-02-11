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
    df.to_parquet(path=parquet_path)
    return parquet_path


@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read gzipped csv data from web into pandas DataFrame"""
    df = None
    try:
        df = pd.read_csv(dataset_url, encoding='utf-8')
    except:
        for encoding in ["cp1252", "iso-8859-1", "latin1", "utf-16"]:
            try:
                df = pd.read_csv(dataset_url, encoding=encoding)
            except:
                continue
    if df is not None:
        print(df.dtypes)
        return df
    raise Exception(f"Could not decode {dataset_url}")


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["DOlocationID"] = pd.to_numeric(df["DOlocationID"], downcast="float")
    df["PUlocationID"] = pd.to_numeric(df["PUlocationID"], downcast="float")
    df["SR_Flag"] = pd.to_numeric(df["SR_Flag"], downcast="float")
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], infer_datetime_format=True)
    df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"], infer_datetime_format=True)
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
    df = clean(df)
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
    etl_parent_flow(years=[2019])
