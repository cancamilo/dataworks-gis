import os
import numpy as np
import pandas as pd
from shapely.geometry import Point 
import xarray as xr
from prefect_gcp import GcpCredentials
from prefect import flow, task

from prefect_gcp.bigquery import BigQueryWarehouse

import shapely
import json
from shapely.validation import make_valid

#determine schema
type_dict = {
    'b' : 'BOOLEAN',
    'i' : 'INTEGER',
    'f' : 'FLOAT',
    'O' : 'STRING',
    'S' : 'STRING',
    'U' : 'STRING',
    'M': 'TIMESTAMP'
}

@task()
def extract_data(ts: pd.Timestamp) -> pd.DataFrame:        
    year = ts.year
    month = ts.month
    day = ts.day
    datetime = f"{year}{month:02}{day:02}"
    url_geos = f"https://power-datastore.s3.amazonaws.com/v9/daily/{year}/{month:02}/power_901_daily_{datetime}_geos5124_utc.nc"
    url_flux = f"https://power-datastore.s3.amazonaws.com/v9/daily/{year}/{month:02}/power_901_daily_{datetime}_flashflux_lst.nc"
    os.system(f"wget -O geos.nc {url_geos}")
    os.system(f"wget -O flux.nc {url_flux}")
    geos_df = xr.open_dataset('geos.nc').to_dataframe()
    flux_df = xr.open_dataset('flux.nc').to_dataframe()
    return geos_df, flux_df

@task()
def transform_data(df_geos: pd.DataFrame, df_flux: pd.DataFrame):
    pass

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-credentials")
    df_flat = df.reset_index().iloc[:10000] # debug for a few records

    df_flat["GEO_ID"] = df_flat.index
    df_flat["COORDS"] = df_flat.apply(lambda row: Point(row["lat"], row["lon"]), axis = 1)
    df_flat["CLRSKY_DAYS"] = df_flat["CLRSKY_DAYS"].astype(str)

    schema = [
        {
            'name' : col_name, 
            'type' : "GEOGRAPHY" if col_name == "COORDS" else type_dict.get(col_type.kind, 'STRING')
        } for (col_name, col_type) in df_flat.dtypes.iteritems()
    ]    

    df_flat["COORDS"] = df_flat.apply(lambda row: Point(row["lat"], row["lon"]), axis = 1)
    df_flat["CLRSKY_DAYS"] = df_flat["CLRSKY_DAYS"].astype(str)

    # df_json = pd.DataFrame({
    #     col: (df_flat[col] if col != 'COORDS' else df_flat[col].map(lambda x: json.dumps(shapely.geometry.mapping(make_valid(x)))))
    #     for col in df_flat
    # })

    # should replace data for the new day.
    df_flat.to_gbq(
        destination_table="dataworks-gis.test_gis.ceres",
        project_id="dataworks-gis",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace",
        table_schema=schema
    )

@task
def exec_query(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")

    selected_cols = ['lon', 'lat', 'time',
                     'T2M', 'T10M', # temperature
                     'WS2M', 'WS10M', # wind speed
                     'PRECTOTCORR', # precipitations
                     'RH2M', # relative humidity
                     'CDD0', 'CDD10'] # days above temp
    
    # TODO create similar function for processing radiation data

    df_flat = df.reset_index() # .iloc[:10000] # debug for a few records    

    df_flat = df_flat[selected_cols]  

    # note this should be changed 
    # if data is appendended to the table. e.g a combination of timestamp with lat and lon
    df_flat["GEO_ID"] = df_flat.index
    df_flat["COORDS"] = df_flat.apply(lambda row: Point(row["lon"], row["lat"]).wkt, axis = 1)
    df_flat["COORDS"] = df_flat["COORDS"].astype(str)
    # df_flat["CLRSKY_DAYS"] = df_flat["CLRSKY_DAYS"].astype(str)

    # schema = [
    #     {
    #         'name' : col_name, 
    #         'type' : "GEOGRAPHY" if col_name == "COORDS" else type_dict.get(col_type.kind, 'STRING')
    #     } for (col_name, col_type) in df_flat.dtypes.iteritems()
    # ]

    table_id = "dataworks-gis.test_gis.ceres"

    df_flat.to_gbq(
        destination_table=table_id,
        project_id="dataworks-gis",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
        #table_schema=schema
    )

    # cols = ",".join("st_geogfromtext(COORDS, make_valid => TRUE) as COORDS" if col == "COORDS" else col for col in df_flat)
    # query = f"CREATE OR REPLACE TABLE {table_id} AS SELECT {cols} FROM {table_id}"
    # print(query)
    # with BigQueryWarehouse.load("bq-block") as warehouse:        
    #     warehouse.execute(query)

@flow(log_prints=True)
def run_data_range_flow(start_date, end_date):
    for dat in pd.date_range(start_date, end_date):
        df = extract_data(dat)
        print(f"dt={dat} shape={df.shape}")
        exec_query(df)

@flow(log_prints=True)
def run_single_flow(date):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """

    pd.to_datetime(date, format='%Y-%m-%d')
    df = extract_data()

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
    upload_cities()