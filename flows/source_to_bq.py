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
def run_geos_flow(dt: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """

    print(f"running geos flow for {dt}")
    data_type = DataType.GEOS    
    geos_df = extract_data(dt, data_type)

    if geos_df is None:
        logger.warning(f"Could not load geos data for {dt}")
        return 
    
    write_to_gcs(data_type, dt)

    tf_geos_df = transform_data(geos_df, select_cols=common.geo_cols)    

    bq_table_id = "dataworks-gis.geos_flux_data.geos_table_partitioned"                                        
    write_to_bq(tf_geos_df, bq_table_id)

@flow(log_prints=True)
def run_flux_flow(dt: pd.Timestamp):
    """run the pipeline for only one day. the date should be formatted as '%Y-%m-%d':    
    """

    print(f"running flux flow for {dt}")
    data_type = DataType.FLUX    
    flux_df = extract_data(dt, data_type)

    if flux_df is None:
        logger.warn(f"Could not load flux data for {dt}")
        return
    
    write_to_gcs(data_type, dt)
    
    tf_flux_df = transform_data(flux_df, select_cols=common.flux_cols)

    bq_table_id = "dataworks-gis.geos_flux_data.flux_table_partitioned"
    write_to_bq(tf_flux_df, bq_table_id) 

@flow(log_prints=True)
def run_data_range_flow(start_date, end_date):
    for dat in pd.date_range(start_date, end_date):
        run_flux_flow(dat)
        run_geos_flow(dat)
        

if __name__ == "__main__":

    start = "2022-03-01"
    end = "2023-04-01"
    run_data_range_flow(start, end)
    