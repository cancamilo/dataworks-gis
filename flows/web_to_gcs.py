import os
import requests
import numpy as np
import logging
import pandas as pd
from prefect import flow
from common import DataType, extract_data, write_to_gcs, clean_files

logger = logging.getLogger("root")

url_geos = "https://power-datastore.s3.amazonaws.com/v9/daily/{year}/{month:02}/power_901_daily_{datetime}_geos5124_utc.nc"
url_flux = "https://power-datastore.s3.amazonaws.com/v9/daily/{year}/{month:02}/power_901_daily_{datetime}_flashflux_lst.nc"

@flow(log_prints=True)
def web2gcs_geos_flow(date: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """
    data_type = DataType.GEOS
    ts = pd.to_datetime(date, format='%Y-%m-%d')
    geos_df = extract_data(ts, data_type)

    if geos_df is None:
        logger.warning(f"Could not load geos data for {date}")
        return
    
    write_to_gcs(data_type, ts)
    clean_files(data_type)


@flow(log_prints=True)
def web2gcs_flux_flow(date: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """
    data_type = DataType.FLUX    
    flux_df = extract_data(date, data_type)

    if flux_df is None:
        logger.warn(f"Could not load flux data for {date}")
        return    
    
    write_to_gcs(data_type, date)
    clean_files(data_type)

@flow(log_prints=True)
def run_data_range_flow(start_date, end_date):
    for dat in pd.date_range(start_date, end_date):
        web2gcs_flux_flow(dat)
        web2gcs_geos_flow(dat)

if __name__ == "__main__":

    ## run single flow
    # dt = "2022-05-08"
    # ts = pd.to_datetime(dt, format='%Y-%m-%d')
    # run_flux_flow(ts)

    start = "2022-01-02"
    end = "2022-01-05"
    run_data_range_flow(start, end)