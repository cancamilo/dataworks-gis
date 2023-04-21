# NASA POWER data pipeline

This is a data engineering project that uses the [NASA POWER](https://power.larc.nasa.gov/) data to showcase the extraction, processing and visualization of large amounts of geospatial data using google cloud technologies.

## Project description

The NASA Power project (prediction of worldwide energy resources) provides access to free data intended for supporting the research and development of renewable energies, building energy efficiency, and agricultural needs.

My objective is to extract the NASA power data on a daily basis with an automated pipeline, upload the raw data to a data lake and together with data for several city locations, create visualizations to gain insights into climate trends and solar radiation in a specific area of interest. The parameters chosen from the POWER data include temperature, wind speed, precipitations and solar radiation. For specific details about the meteorological and radiation parameters check [this link](https://gist.github.com/abelcallejo/d68e70f43ffa1c8c9f6b5e93010704b8).

The dashboard provides the following visualizations:

- Time series charts showing the variations of several climate and solar irradiance parameters for a selected region in a selected time frame.
- A bar chart visualization of the top months by precipitations, temperature or solar radiation in a specific region.
- A pivot table that shows time trends of a specific parameter and a selected group of cities. Useful for comparing the trends between different regions.

Such visualizations can aid in decision-making processes related to renewable energy and sustainable building development.

## Technology Stack

I make use of the following technologies:

- Google Cloud Storage as the datalake to store the raw dataset.
- Google BigQuery as the data warehouse.
- Terraform to create google cloud storage buckets and BigQuery datasets.
- Dbt core as the transformation tool for data modeling.
- Self-hosted Prefect core to manage and monitor the workflow.
- Looker as the dashboard for end-user for visualization
- Makefile
- Poetry for managing python dependencies

## Data Pipeline Architecture and workflow

<br>
<br>

<p align="center">
  <img src="./images/pipeline.png"/>
</p>

### Nasa Power Data

The Nasa Power data is stored in s3 and can be accessed [https://power.larc.nasa.gov/data/](here).

### Data Orchestration with Prefect flows

Prefect is used for data orchestrations and consist of multiple flows and deployments:

- Web to GCS Flow:

    Each execution downloads both solar irradiance and meteorological data from s3 for a speficic day and upload it into google cloud storage. The structure of the data saved in the bucket is as follows:

            flux_data:
                - year
                    - month
                        - day1
                        - day2
                        ...
            geos_data:
                - year
                    - month
                        - day1
                        - day2
                        ...


- GCS to BigQuery Flow:

    It downloads data from GCS for a specific date. Once loaded, the data is transformed. The transformation consist of selecting the parameters of interest from the parquet data and creating a new dt column computed by truncating the timestamp to a monthly frequency. This last step is useful when executing queries that require referencing the monthly part of the timestamp as the data in bigquery is partitioned by month.

    After the flow transforms the data, a following task is to update the BigQuery table with the new data for the currentling executing timestamp.

- Initialize BigQuery tables Flow:

    It creates two partitioned tables: `flux_table_partitioned` to store solar irradiance related metrics and `geos_table_partitioned` to store meteorological related data. 

- Upload cities table Flow

    It simply uploads the city table to BigQuery. Only need to be executed once.

I included default deployments for each of the flows. They can be easily applied using the [Makefile](./Makefile). Check the replication steps for more information.

### Dbt

Dbt is used for the modelling part. The data is read from bigQuery and different models are created. 

### Data visualization

Looker is used for the visualizations. It uses the data models in bigQuery created by a dbt Job.

## Data sources and Data modelling

The NASA Power data is consists of several [sources](https://power.larc.nasa.gov/docs/methodology/data/sources/). For this project, I have selected two different sources of data:

1. Fast Longwave and Shortwave Flux (FLASHFlux): Provides solar irradiance data. The source of the flux data is the CERES project. It stands for `Clouds and Earth’s Radiant Energy System`. It uses measurements from satellites along with data from many other instruments to produce a comprehensive set of data products for climate, weather and applied science research. I have selected the fux data source that is updated more frequently (< 5 days from measurement) so the pipeline can be programmed to read data in a similar period of time. 

    The FLUX parameters selected are:

        - TOA_SW_DWN: Total solar irradiance on top of atmosphere. Shortwave downward.
        - CLRSKY_SFC_LW_DWN:  Termal infrarred irradiance under clear sky conditions. Longwave downward.
        - ALLSKY_SFC_LW_DWN: Termal infrarred irradiance under all sky conditions. Longwave downward.
        - CLRSKY_SFC_SW_DWN: Termal infrarred irradiance under clear sky conditions. Shortwave downward.
        - ALLSKY_SFC_SW_DWN: Termal infrarred irradiance under all sky conditions. Shortwave downward.


2. NASA's Goddard Earth Observing System (GEOS): Provides meteorological data. Types of observations include land surface observations of surface pressure, ocean surface observations of sea level pressure and winds, sea level winds inferred from backscatter returns from space-borne radars, conventional upper-air data from rawinsondes (e.g., height, temperature, wind and moisture), and (6) remotely sensed information from satellites. The data is updated in S3 with a daily frequency.

    The GEOS parameters selected are:

        - T2M_CELCIUS:  Temperature at 2 meters in Celcius degrees.
        - T10M_CELCIUS: Temperature at 10 meters in Celcius degrees.
        - WS2M: Windspeed at 2 meters.
        - WS10M: Windspeed at 10 meters.
        - PRECTOTCORR:  Average of total precipitation at the surface of the earth in water mass.
        - RH2M: Relative Humidity at 2 Meters.
        - CDD0: Cooling Degree Days Above 0 C.
        - CDD10: Cooling Degree Days Above 10 C.

I extract each of the these parameters measured on a daily basis. The measurements of the flux data are provided for each date on a 1° x 1° latitude/longitude grid whereas the meteorological data are provided on a ½° x ⅝° latitude/longitude grid. The resulting schemas in BigQuery are:

Flux schema:

<img src="./images/flux_schema.png" alt="Flux schema" width="30%" height="30%">

Geos schema:

<img src="./images/geos_schema.png" alt="Geos schema" width="30%" height="30%">

### Partitions

Both raw tables are partitioned by the time column truncated by month [See creation query](./flows/queries/flux_table_creation.sql). In this way, common operations such as groping or filtering by a specific datetime are optimized. This has an important impact in the models calculated subsequently with dbt specially when computing the monthyl average of some parameters

### Dbt modelling

Instead of querying and visualizing of all datapoints in the global grid, it is more practical to limit the datapoints that are close to a city or region of interest. In order to achieve this, I added a new table for `cities`. It contains information about all cities of the world with their coordinates. As it is a samall file, I include it as a csv in this repo and can be accessed [here](./worldcities.csv).

In dbt, given the cities table, the raw flux and geos tables I perform an inner [spatial join](./dbt_nasa_power/models/staging/city_flux_model.sql) with the flux and geos tables separately to create two new tables which contain data from the cities joint with solar irradiance and meteorological measurements. The resulting models are a time series for each city and a set of metrics.  


## Dashboard preview

## Replication steps

### Setup google cloud

1. Start by creating a new Google Cloud account and setting up a new project. 

2. Create a new service account and grant it Compute Admin, Service Account User, Storage Admin, Storage Object Admin, and BigQuery Admin roles. 

3. After creating the service account, click on "Manage Keys" under actions Menu. Click on the Add Key dropdown and click on Create new key and create a new key in JSON format, saving it to your computer.

4. Install the Google Cloud CLI and log in by running "gcloud init" in an Ubuntu Linux environment or similar. 

5. Choose the cloud project you created to use. Set the environment variable to point to your downloaded service account keys JSON file by running "export GOOGLE_APPLICATION_CREDENTIALS=<path/to/your/service-account-authkeys>.json". 

6. Refresh your token/session and verify authentication by running "gcloud auth application-default login". 

7. Ensure that the following APIs are enabled for your project: 

https://console.cloud.google.com/apis/library/iam.googleapis.com 
https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com 
https://console.cloud.google.com/apis/library/compute.googleapis.com


## Future work