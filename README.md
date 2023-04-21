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
- dbt core as the transformation tool for data modeling.
- Self-hosted Prefect core to manage and monitor the workflow.
- Looker as the dashboard for end-user for visualization
- Makefile
- Poetry for managing python dependencies

## Data Pipeline Architecture and workflow

![Pipeline architecture](pipeline.png)

- The Nasa Power data is stored in s3 and can be accessed [https://power.larc.nasa.gov/data/](here).

- Prefect is used to extract the data from s3, load it into google cloud storage, transform it and push it to the bigQuery tables. 


- Dbt is used for the modelling part. The data is read from bigQuery and different models are created. 

- Looker is used for the visualizations. It uses the data models in bigQuery created by a dbt Job.


### Data sources and Data modelling

The NASA Power data is consists of several [sources](https://power.larc.nasa.gov/docs/methodology/data/sources/). For this project, I have selected two different sources of data:

1. Fast Longwave and Shortwave Flux (FLASHFlux): Proides solar irradiance data. The source of the flux data is the CERES project. It stands for `Clouds and Earth’s Radiant Energy System`, and it provides satellite-based observations of ERB and clouds. It uses measurements from satellites along with data from many other instruments to produce a comprehensive set of ERB data products for climate, weather and applied science research. I have selected the fux data source that is updated more frequently (< 5 days from measurement) so the pipeline can be programmed to read data in a similar period of time. 

The flux parameters selected are:

    - TOA_SW_DWN: Total solar irradiance on top of atmosphere. Shortwave downward.

    - CLRSKY_SFC_LW_DWN:  Termal infrarred irradiance under clear sky conditions. Longwave downward

    - ALLSKY_SFC_LW_DWN: Termal infrarred irradiance under all sky conditions. Longwave downward.

    - CLRSKY_SFC_SW_DWN: Termal infrarred irradiance under clear sky conditions. Shortwave downward 

    - ALLSKY_SFC_SW_DWN: Termal infrarred irradiance under all sky conditions. Shortwave downward



### Dashboard preview

## Replication steps