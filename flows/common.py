import pandas as pd
import requests
import os
import xarray as xr
from pathlib import Path
from prefect import task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

class DataType:
    GEOS = "geos" # string to identify data from geos system (meteorology data)
    FLUX = "flux" # string to identify data from fluxflash system (solar irradiance data)

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

script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
data_path = "data"
abs_data_path = os.path.join(script_dir, data_path)

@task()
def extract_data(ts: pd.Timestamp, type: str) -> str:
    year = ts.year
    month = ts.month
    day = ts.day
    datetime = f"{year}{month:02}{day:02}"

    url = url_geos if type == DataType.GEOS else url_flux
    url = url.format(year=year, month=month, datetime=datetime)

    # os.system(f"wget -O {type}.nc {url}")
    r = requests.get(url, allow_redirects=True,  timeout=(20,200))

    write_path = f"{abs_data_path}/temp_{type}.nc"
    parquet_path = f"{abs_data_path}/temp_{type}.parquet"

    if r.status_code == 200:        
        if not os.path.exists(abs_data_path):
            os.makedirs(abs_data_path)

        open(write_path, 'wb').write(r.content)
        df = xr.open_dataset(write_path).to_dataframe().reset_index()
        df.to_parquet(parquet_path, compression="gzip")
        return df
    else:
        return None
    
@task()
def transform_data(df: pd.DataFrame, select_cols) -> pd.DataFrame:
    
    df["GEO_ID"] = df.index

    # add dt to make quering by partition easier. Truncating to month.
    df["dt"] = df['time'].dt.strftime('%Y-%m-01')
    df = df[default_cols + select_cols]
    return df

@task
def write_to_gcs(type: str, dt: pd.Timestamp):
    gcs_block = GcsBucket.load("gcs-connector")
    dt_str = dt.strftime('%Y_%m_%d')

    source_path = f"{abs_data_path}/temp_{type}.parquet"    
    target_path = f"{type}/{dt.year}/{dt.month:02}/{type}_{dt_str}.parquet"
    
    gcs_block.upload_from_path(from_path=source_path, to_path=target_path, timeout=120, num_retries=5)

@task
def read_from_gcs(type: str, dt: pd.Timestamp):
    """Download trip data from GCS"""

    dt_str = dt.strftime('%Y_%m_%d')
    gcs_path = f"{type}/{dt.year}/{dt.month:02}/{type}_{dt_str}.parquet"
    gcs_block = GcsBucket.load("gcs-connector")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"{abs_data_path}")
    return pd.read_parquet(f"{abs_data_path}/{gcs_path}")    

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
def clean_files(type: str):
    nc_path = f"{abs_data_path}/temp_{type}.nc"
    parquet_path = f"{abs_data_path}/temp_{type}.parquet"
    os.remove(nc_path)
    os.remove(parquet_path)



