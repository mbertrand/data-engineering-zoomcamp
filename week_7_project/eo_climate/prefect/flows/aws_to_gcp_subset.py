from json import load
from datetime import datetime, timedelta
import os
from pathlib import Path

import boto3
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from pystac_client import Client
from pyproj import Transformer
import rasterio as rio
from rasterio.features import bounds


@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local image subsets to GCS"""
    gcs_block = GcsBucket.load("eo-climate-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@task(retries=3)
def write_subset(band_name: str, geotiff_url: str, bbox: object, date: str):
    """
    Extract a subset of a COG getiff and save locally
    """
    aws_session = rio.session.AWSSession(boto3.Session(), requester_pays=True)
    with rio.Env(aws_session):
        with rio.open(geotiff_url) as landsat_tif:

            # Calculate pixels in bbox
            transformer = Transformer.from_crs("epsg:4326", landsat_tif.crs)
            lat_north, lon_west = transformer.transform(bbox[3], bbox[0])
            lat_south, lon_east = transformer.transform(bbox[1], bbox[2])
            x_top, y_top = landsat_tif.index( lat_north, lon_west )
            x_bottom, y_bottom = landsat_tif.index( lat_south, lon_east )
            window = rio.windows.Window.from_slices( ( x_top, x_bottom ), ( y_top, y_bottom ) )

            # request this bbox subset from the cog geotiff on s3
            subset = landsat_tif.read(1, window=window)
            profile = landsat_tif.profile
            basename, ext = os.path.splitext(geotiff_url.split("/")[-1])
            path = Path(f"landsat/{date}/{basename[:37]}/{band_name}{ext.lower()}")
            path.parent.mkdir(parents=True, exist_ok=True)
            with rio.open(path, 'w', **profile) as dst:
                dst.write(subset, 1)
        return path


@flow()
def aws_to_gcs_subset(image: dict, bands: list[str], bbox) -> None:
    """The main ETL function"""
    date = image['properties']['datetime'][0:10]
    for band in bands:
        band_s3_url = image['assets'][band]['alternate']['s3']['href']
        path = write_subset(band, band_s3_url, bbox, date)
        write_gcs(path)
        path.unlink()


@flow()
def get_landsat_data(collections, geometry, start_dt, end_dt, query=None):
    """Retrieve a list of landsat images matching the criteria"""
    landsat_STAC = Client.open("https://landsatlook.usgs.gov/stac-server", headers=[])
    landsat_search = landsat_STAC.search(
        intersects=geometry,
        datetime=f"{start_dt}/{end_dt}",
        query=query,
        collections=collections)
    return [item.to_dict() for item in landsat_search.get_items()]


@flow(log_prints=True)
def landsat_parent_flow(
    collections: list[str] = None,
    bands: list[str] = None,
    geojson_file: str = None,
    start_date: str = None,
    end_date: str = None,
    query: dict = None
):
    today = datetime.today()
    if not collections:
        collections = ["landsat-c2l2-sr"]
    if not geojson_file:
        geojson_file = "saguaro_np_east.json"
    if not bands:
        bands = ["red", "nir08", "qa_pixel"]
    if not start_date:
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = today.strftime("%Y-%m-%d")

    with open(geojson_file, "r") as geojson_inf:
        geojson_content = load(geojson_inf)
    geometry = geojson_content["features"][0]["geometry"]
    bbox = bounds(geometry)
    images = get_landsat_data(collections, geometry, start_date, end_date, query=query)
    print(f"Found {len(images)} Landsat images between {start_date}-{end_date}")
    for image in images:
        aws_to_gcs_subset(image, bands, bbox)


if __name__ == "__main__":
    landsat_parent_flow()
