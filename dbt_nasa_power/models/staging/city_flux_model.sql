{{ config(materialized='table') }}

WITH cities_data AS (
  SELECT
      id AS city_id,
      city AS city_name,
      city_ascii,
      country,
      population,
      ST_GEOGPOINT(lng, lat) AS city_coords
    FROM {{ source('staging','cities') }}
    -- dbt build --m <model.sql> --var 'is_test_run: false'
    {% if var('is_test_run', default=true) %} 
    limit 100 -- use only a few cities for testing
    {% endif %}
), metrics_flux AS (
  SELECT
    {{ dbt_utils.surrogate_key(['lon', 'lat', 'time']) }} AS GEO_ID,
    time,
    TOA_SW_DWN,
    CLRSKY_SFC_LW_DWN,
    ALLSKY_SFC_LW_DWN,
    CLRSKY_SFC_SW_DWN,
    ALLSKY_SFC_SW_DWN,
    ST_GEOGPOINT(lon, lat) AS metric_coords
  FROM {{ source('staging','flux_table_partitioned') }}
)
SELECT * FROM cities_data a
JOIN metrics_flux b
ON ST_DWithin(a.city_coords, b.metric_coords, {{var('join_radius', default=50000)}})