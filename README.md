# NASA POWER data pipeline

The goal of this project is to obtain data from NASA power different sources, process it and visualize it in a dashboard.
The source is geospatial data obtained from NASA satellites measuring climate data as well as solar irradiation data. The ideal visualization dashboard should contain a map of the earth where several parameters can be chosen. Other types of aggregations can be added. By default, the data shown in the dashboard should be the latest available. It should also be possible to visualize historical data.


Pipeline overview:

- Data extraction from S3

    available under https://power.larc.nasa.gov/data/

Create a python script that reads the data from the s3 bucket above and downloads a specific date. The script should take as input a datetime and the source of measurement. (GEOS, CERES OR MERRA). Check [this link](https://power.larc.nasa.gov/docs/methodology/data/sources/) for more details on the data sources available. 

- Data transformation

TBD

- Data loading

A python script to upload the data to GCS. 

- Orchestration

The prefect framework is used for orchestration. It will help execute the extraction transforming and loading pipelines. Taking into account the update frequency of the data sources, the pipeline should be programmed to run once per day at any hour.

- Tables and views creation. 

Create big query tables (partitioned by date) from the sources uploaded to GCS. 

Open questions:

    - What kind of aggregations should I have?
    - Should the creation of these tables be automated with prefect or with dbt or other tool?
    - Should I use spark or dbt?


# Next Steps

Create a manually executable pipeline. 

- Configure google cloud account ~ 1 hour

- Write script that downloads, transforms and uploads to BG ~ 3-4 hours
- Create query for a specific point in time ~ 1 hour
- Configure looker to visualize the dat and do qa ~ 2 hour

- Create a prefect workflow ~ 3 hours
- Configure virtual machine in GC that runs prefect and test it ~ 3 hours
- Spark vs dbt? choose how to process the data and what too show in the dashboard. Include unit tests. 10 hours

- Refine all the above steps ~ 5 hours
- Write documentation ~ 2 hour






