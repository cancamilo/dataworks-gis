import logging
import pandas as pd
from prefect import flow
import common
from common import (DataType, 
                    extract_data, 
                    write_to_gcs,
                    write_to_bq,                     
                    transform_data)

logger = logging.getLogger("root")

@flow(log_prints=True)
def run_geos_flow(date: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """

    print(f"running geos flow for {date}")
    data_type = DataType.GEOS
    ts = pd.to_datetime(date, format='%Y-%m-%d')
    geos_df = extract_data(ts, data_type)

    if geos_df is None:
        logger.warning(f"Could not load geos data for {date}")
        return 
    
    write_to_gcs(data_type, date)

    tf_geos_df = transform_data(geos_df, select_cols=common.geo_cols)    

    bq_table_id = "dataworks-gis.geos_flux_data.geos_table_partitioned"                                        
    write_to_bq(tf_geos_df, bq_table_id)

@flow(log_prints=True)
def run_flux_flow(date: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """

    print(f"running flux flow for {date}")
    data_type = DataType.FLUX    
    ts = pd.to_datetime(date, format='%Y-%m-%d')
    flux_df = extract_data(ts, data_type)

    if flux_df is None:
        logger.warn(f"Could not load flux data for {date}")
        return
    
    write_to_gcs(data_type, date)
    
    tf_flux_df = transform_data(flux_df, select_cols=common.flux_cols)

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

    start = "2022-01-01"
    end = "2022-01-31"
    run_data_range_flow(start, end)
    