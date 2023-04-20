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

- Configure google cloud account ~ 1 hour [x]
- Write script that downloads, transforms and uploads to BG ~ 3-4 hours [x]
- Try to convert data point to geometry ~ 1 hour [x]
- Create query for a specific point in time ~ 1 hour [x]
- Fix basic visualization ~ 1 hour [x]

- Spark vs dbt? choose how to process the data and what to show in the dashboard. Include unit tests. 10 hours []

Spark can work either on the raw data in the data lake or directly interact with BigQuery. It requires to spin-up a cluster for computations. On the other hand, dbt uses the query engine of the data warehouse, Bigquery in this case. 

- Create a prefect workflow ~ 3 hours []
- Configure virtual machine in GC that runs prefect and test it ~ 3 hours []
- Configure looker to visualize the data and do qa ~ 2 hour []

- Refine all the above steps ~ 5 hours []
- Write documentation ~ 2 hour []






