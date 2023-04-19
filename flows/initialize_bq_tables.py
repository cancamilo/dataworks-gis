from prefect import flow
from prefect_gcp.bigquery import BigQueryWarehouse

@flow(log_prints=True)
def initialize_tables():
    with open("queries/geos_table_creation.sql") as file:
        geo_script = file.read()

    with open("queries/flux_table_creation.sql") as file:
        flux_script = file.read()
    
    with BigQueryWarehouse.load("bq-block") as warehouse:        
        warehouse.execute(geo_script)
        warehouse.execute(flux_script)

if __name__ == "__main__":

    ## excecute first time for creating partitioned tables
    initialize_tables()