from pathlib import Path
import requests
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3)
def fetch(dataset_url: str) -> Path:
    """Get gzipped csv data files from web and save to local storage"""
    path = Path(f"data/fhv/{dataset_url.split('/')[-1]}")
    path.parent.mkdir(parents=True, exist_ok=True)
    # NOTE the stream=True parameter below
    with open(path, 'wb') as f:
        with requests.get(dataset_url, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return path


@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local gzipped csv files to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    month_str = "{:02d}".format(month)
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month_str}.csv.gz"
    path = fetch(dataset_url)
    write_gcs(path)


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
    etl_parent_flow()
