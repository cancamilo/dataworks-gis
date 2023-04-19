import os
from prefect import flow
from prefect_gcp.bigquery import BigQueryWarehouse

script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
data_path = "queries"
abs_queries_path = os.path.join(script_dir, data_path)

@flow(log_prints=True)
def initialize_tables():
    with open(f"{abs_queries_path}/geos_table_creation.sql") as file:
        geo_script = file.read()

    with open(f"{abs_queries_path}/flux_table_creation.sql") as file:
        flux_script = file.read()
    
    with BigQueryWarehouse.load("bq-block") as warehouse:        
        warehouse.execute(geo_script)
        warehouse.execute(flux_script)

if __name__ == "__main__":

    ## excecute first time for creating partitioned tables
    initialize_tables()