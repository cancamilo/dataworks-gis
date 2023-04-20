import pandas as pd
from prefect_gcp import GcpCredentials
from prefect import flow, task
from shapely.geometry import Point

@flow(log_prints=True)
def upload_cities():
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")

    cities_df = pd.read_csv("worldcities.csv", sep=",")
    cities_df["GEO_ID"] = cities_df.index
    cities_df["COORDS"] = cities_df.apply(lambda row: Point(row["lng"], row["lat"]).wkt, axis = 1)
    cities_df["COORDS"] = cities_df["COORDS"].astype(str)

    cities_df.to_gbq(
        destination_table="dataworks-gis.geos_flux_data.cities",
        project_id="dataworks-gis",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"        
    )

if __name__ == "__main__": 
    ## upload city data to bq. Only needed once.
    upload_cities()