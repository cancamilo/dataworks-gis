# NASA POWER data pipeline

This is a data engineering project that uses the [NASA POWER](https://power.larc.nasa.gov/) data to showcase the extraction, processing and visualization of large amounts of geospatial data using google cloud technologies.

## Project description

The NASA Power project (prediction of worldwide energy resources) provides access to free data intended for supporting the research and development of renewable energies, building energy efficiency and agricultural needs.

My objective is to extract the NASA power data on a daily basis with an automated pipeline, upload the raw data to a data lake and together with data for several city locations, create visualizations to gain insights into climate trends and solar radiation in a specific area of interest. The parameters chosen from the POWER data include temperature, wind speed, precipitations and solar radiation. For specific details about the meteorological and radiation parameters check the [schema](link to schema) of the tables.

The dashboard provides the following visualizations:

- A time series showing the variations of several climate parameters for a selected region.
- A heat map visualization of a selected region and a selected parameter.
- A bar chart visualization of the top months by precipitations, temperature or solar radiation.

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

## Data Pipeline Architecture



### Data structure

### Dashboard preview

## Replication steps