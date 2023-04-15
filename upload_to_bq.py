import os
import numpy as np
import pandas as pd
from pathlib import Path
from shapely.geometry import Point 
import xarray as xr
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow, task

from prefect_gcp.bigquery import BigQueryWarehouse


default_cols = ['lon', 'lat', 'time']
geo_cols = ['T2M', 'T10M', # temperature
            'WS2M', 'WS10M', # wind speed
            'PRECTOTCORR', # precipitations
            'RH2M', # relative humidity
            'CDD0', 'CDD10'] # days above temp

flux_cols = ['lon', 'lat', 'time',
            'TOA_SW_DWN', # total solar irradiance on top of atmosphere. Shortwave downward.
            'CLRSKY_SFC_LW_DWN', # termal infrarred irradiance under clear sky conditions. Longwave downward 
            'ALLSKY_SFC_LW_DWN', # termal infrarred irradiance under all sky conditions. Longwave downward 
            'CLRSKY_SFC_SW_DWN', # termal infrarred irradiance under clear sky conditions. Shortwave downward 
            'ALLSKY_SFC_SW_DWN'] # termal infrarred irradiance under all sky conditions. Shortwave downward


url_geos = "https://power-datastore.s3.amazonaws.com/v9/daily/{year}/{month:02}/power_901_daily_{datetime}_geos5124_utc.nc"
url_flux = "https://power-datastore.s3.amazonaws.com/v9/daily/{year}/{month:02}/power_901_daily_{datetime}_flashflux_lst.nc"


class DataType:
    GEOS = "geos" # string to identify data from geos system (meteorology data)
    FLUX = "flux" # string to identify data from fluxflash system (solar irradiance data)

@task()
def extract_data(ts: pd.Timestamp, type: str) -> pd.DataFrame:        
    year = ts.year
    month = ts.month
    day = ts.day
    datetime = f"{year}{month:02}{day:02}"

    url = url_geos if type == DataType.GEOS else url_flux        
    url = url.format(year=year, month=month, datetime=datetime)

    os.system(f"wget -O {type}.nc {url}")    
    return xr.open_dataset(f'{type}.nc').to_dataframe().reset_index()    
    

@task()
def transform_data(df: pd.DataFrame, select_cols) -> pd.DataFrame:
    
    df["GEO_ID"] = df.index
    df = df[default_cols + select_cols]

    # if neccesary, transform data types to strings to avoid failures when creating tables from parquet
    # for col in select_cols:
    #     df[col] = df[col].astype(str)

    return df

@task()
def write_local(df: pd.DataFrame, type: str):
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/temp_{type}.parquet")
    print("===============> rows processed" ,len(df))
    if not os.path.exists(f"data"):
        os.makedirs("data")

    df.to_parquet(path, compression="gzip")
    return path

@task
def write_to_gcs(type: str, dt: pd.Timestamp):
    gcs_block = GcsBucket.load("gcs-connector")
    dt_str = dt.strftime('%Y_%m_%d')
    source_path = f"data/temp_{type}.parquet"
    target_path = f"{type}/{dt.year}/{dt.month:02}/{type}_{dt_str}.parquet"
    gcs_block.upload_from_path(from_path=source_path, to_path=target_path)

@task
def write_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")
    table_id = "dataworks-gis.test_gis.geos_table"

    df.to_gbq(
        destination_table=table_id,
        project_id="dataworks-gis",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",        
    )

@flow(log_prints=True)
def run_data_range_flow(start_date, end_date):
    for dat in pd.date_range(start_date, end_date):
        df = extract_data(dat)
        print(f"dt={dat} shape={df.shape}")
        exec_query(df)

@flow(log_prints=True)
def upload_cities():
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")

    cities_df = pd.read_csv("worldcities.csv", sep=",")
    cities_df["GEO_ID"] = cities_df.index
    cities_df["COORDS"] = cities_df.apply(lambda row: Point(row["lng"], row["lat"]).wkt, axis = 1)
    cities_df["COORDS"] = cities_df["COORDS"].astype(str)

    cities_df.to_gbq(
        destination_table="dataworks-gis.test_gis.cities",
        project_id="dataworks-gis",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",        
    )


@flow(log_prints=True)
def initialize_tables():
    pass
    # cols = ",".join("st_geogfromtext(COORDS, make_valid => TRUE) as COORDS" if col == "COORDS" else col for col in df_flat)
    # query = f"CREATE OR REPLACE TABLE {table_id} AS SELECT {cols} FROM {table_id}"
    # print(query)
    # with BigQueryWarehouse.load("bq-block") as warehouse:        
    #     warehouse.execute(query)
    

@flow(log_prints=True)
def run_geos_flow(date):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """
    data_type = DataType.GEOS
    ts = pd.to_datetime(date, format='%Y-%m-%d')
    geos_df = extract_data(ts, data_type)
    tf_geos_df = transform_data(geos_df, select_cols=geo_cols)

    write_local(tf_geos_df, data_type)
    write_to_gcs(data_type, ts)
    

if __name__ == "__main__":
    # downloads
    # dt = "2022-05-08"

    # month = "0" + str(i + 1)
    # month = month[-2:]

    # url = f"https://power-datastore.s3.amazonaws.com/v9/hourly/2022/{month}/power_901_hourly_{datetime}_ceres_utc.nc"
    # !wget -O latest.nc {url}


    # start = "2022-05-08"
    # end = "2022-05-12"
    # run_data_range_flow(start, end)
    # upload_cities()

    dt = "2023-04-01"
    run_geos_flow(dt)