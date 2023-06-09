import os
import requests
import numpy as np
import logging
import pandas as pd
from pathlib import Path
import xarray as xr
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow
from datetime import datetime
import common
from common import DataType, write_to_bq, read_from_gcs, transform_data

logger = logging.getLogger("root")

@flow(log_prints=True)
def gcs2bq_geos_flow(dt: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """
    data_type = DataType.GEOS    

    df = read_from_gcs(data_type, dt)
    df = transform_data(df, common.geo_cols)

    bq_table_id = "dataworks-gis.geos_flux_data.geos_table_partitioned"
    write_to_bq(df, bq_table_id)
    


@flow(log_prints=True)
def gcs2bq_flux_flow(dt: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """
    data_type = DataType.FLUX   

    df = read_from_gcs(data_type, dt)
    df = transform_data(df, common.flux_cols)
    
    bq_table_id = "dataworks-gis.geos_flux_data.flux_table_partitioned"
    write_to_bq(df, bq_table_id)
    

@flow(log_prints=True)
def gcs_to_bq_data_range_flow(start_date, end_date):
    for dat in pd.date_range(start_date, end_date):
        gcs2bq_flux_flow(dat)
        gcs2bq_geos_flow(dat)

@flow(log_prints=True)
def gcs_to_bq_flow(date=None):

    if date is None:
        date = datetime.today().strftime('%Y-%m-%d')

    dt = pd.to_datetime(date, format='%Y-%m-%d')

    gcs2bq_flux_flow(dt)
    gcs2bq_geos_flow(dt)

if __name__ == "__main__":
    start = "2022-01-02"
    end = "2022-01-05"
    gcs_to_bq_data_range_flow(start, end)