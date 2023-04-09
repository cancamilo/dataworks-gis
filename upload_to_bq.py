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

def exec_query(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")

    df_flat = df.reset_index() # .iloc[:10000] # debug for a few records

    selected_cols = ['lon', 'lat', 'time', 'ALLSKY_SFC_PAR_TOT', 'TOA_SW_DWN',
       'CLRSKY_SRF_ALB', 'GLOBAL_ILLUMINANCE', 'CLOUD_OD',  'SKY_CLEARNESS', 'ALLSKY_KT', 
       'CLRSKY_SFC_PAR_TOT', 'DIRECT_ILLUMINANCE', 'CLRSKY_SFC_LW_DWN', 
       'ALLSKY_SFC_UV_INDEX', 'ZENITH_LUMINANCE',
       'DIFFUSE_ILLUMINANCE', 'ALLSKY_SRF_ALB', 'AIRMASS']

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
        destination_table="dataworks-gis.test_gis.ceres",
        project_id="dataworks-gis",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace",
        #table_schema=schema
    )

    # cols = ",".join("st_geogfromtext(COORDS, make_valid => TRUE) as COORDS" if col == "COORDS" else col for col in df_flat)
    # query = f"CREATE OR REPLACE TABLE {table_id} AS SELECT {cols} FROM {table_id}"

    # print(query)

    # with BigQueryWarehouse.load("bq-block") as warehouse:        
    #     warehouse.execute(query)

@flow(log_prints=True)
def run_flow():
    ds = xr.open_dataset('latest.nc')
    df = ds.to_dataframe()
    exec_query(df)

if __name__ == "__main__":
    # downloads
    year = "2022"
    month = "05"
    day = "08"

    datetime = f"{year}{month}{day}"

    # month = "0" + str(i + 1)
    # month = month[-2:]

    # url = f"https://power-datastore.s3.amazonaws.com/v9/hourly/2022/{month}/power_901_hourly_{datetime}_ceres_utc.nc"
    # !wget -O latest.nc {url}
    run_flow()