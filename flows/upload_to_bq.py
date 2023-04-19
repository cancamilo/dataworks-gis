import os
import requests
import numpy as np
import logging
import pandas as pd
from pathlib import Path
import xarray as xr
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow, task

logger = logging.getLogger("root")

default_cols = ['lon', 'lat', 'time']

geo_cols = ['T2M', 'T10M', # temperature
            'WS2M', 'WS10M', # wind speed
            'PRECTOTCORR', # precipitations
            'RH2M', # relative humidity
            'CDD0', 'CDD10'] # days above temp

flux_cols = ['TOA_SW_DWN', # total solar irradiance on top of atmosphere. Shortwave downward.
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

    # os.system(f"wget -O {type}.nc {url}")
    r = requests.get(url, allow_redirects=True)

    if r.status_code == 200:
        open(f'{type}.nc', 'wb').write(r.content)
        return xr.open_dataset(f'../{type}.nc').to_dataframe().reset_index()
    else:
        return None

@task()
def transform_data(df: pd.DataFrame, select_cols) -> pd.DataFrame:
    
    df["GEO_ID"] = df.index

    # add dt to make quering by partition easier. Truncating to month.
    df["dt"] = df['time'].dt.strftime('%Y-%m-01')
    df = df[default_cols + select_cols]

    # if neccesary, transform data types to strings to avoid failures when creating tables from parquet
    # for col in select_cols:
    #     df[col] = df[col].astype(str)

    return df

@task()
def write_local(df: pd.DataFrame, type: str):
    """Write DataFrame out locally as parquet file"""
    path = Path(f"../data/temp_{type}.parquet")
    print("===============> rows processed" ,len(df))
    if not os.path.exists(f"../data"):
        os.makedirs("../data")

    df.to_parquet(path, compression="gzip")
    return path

@task
def write_to_gcs(type: str, dt: pd.Timestamp):
    gcs_block = GcsBucket.load("gcs-connector")
    dt_str = dt.strftime('%Y_%m_%d')
    source_path = f"../data/temp_{type}.parquet"
    target_path = f"{type}/{dt.year}/{dt.month:02}/{type}_{dt_str}.parquet"
    gcs_block.upload_from_path(from_path=source_path, to_path=target_path)

@task
def write_to_bq(df: pd.DataFrame, table_id: str) -> None:
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")    

    df.to_gbq(
        destination_table=table_id,
        project_id="dataworks-gis",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"        
    )

@task
def read_from_gcs(type: str, dt: pd.Timestamp):
    """Download trip data from GCS"""

    dt_str = dt.strftime('%Y_%m_%d')
    gcs_path = f"{type}/{dt.year}/{dt.month:02}/{type}_{dt_str}.parquet"
    gcs_block = GcsBucket.load("gcs-connector")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@flow(log_prints=True)
def run_geos_flow(date: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """
    data_type = DataType.GEOS
    ts = pd.to_datetime(date, format='%Y-%m-%d')
    geos_df = extract_data(ts, data_type)

    if geos_df is None:
        logger.warning(f"Could not load geos data for {date}")
        return 
    
    tf_geos_df = transform_data(geos_df, select_cols=geo_cols)

    write_local(tf_geos_df, data_type)
    write_to_gcs(data_type, ts)

    bq_table_id = "dataworks-gis.geos_flux_data.geos_table_partitioned"                                        
    write_to_bq(tf_geos_df, bq_table_id)

@flow(log_prints=True)
def run_flux_flow(date: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """
    data_type = DataType.FLUX    
    flux_df = extract_data(date, data_type)

    if flux_df is None:
        logger.warn(f"Could not load flux data for {date}")
        return
    
    tf_flux_df = transform_data(flux_df, select_cols=flux_cols)

    write_local(tf_flux_df, data_type)
    write_to_gcs(data_type, date)

    bq_table_id = "dataworks-gis.geos_flux_data.flux_table_partitioned"
    write_to_bq(tf_flux_df, bq_table_id)
    

@flow(log_prints=True)
def run_data_range_flow(start_date, end_date):
    for dat in pd.date_range(start_date, end_date):
        run_flux_flow(dat)
        run_geos_flow(dat)
        

if __name__ == "__main__":

    ## run single flow
    # dt = "2022-05-08"
    # ts = pd.to_datetime(dt, format='%Y-%m-%d')
    # run_flux_flow(ts)

    start = "2022-02-01"
    end = "2022-03-31"
    run_data_range_flow(start, end)
    