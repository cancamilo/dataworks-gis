WITH city_geo AS (
  SELECT
      id,
      city,
      country,
      capital,
      population,
      admin_name,      
      ST_GEOGPOINT(lng, lat) AS city_coords
    FROM {{ source('staging','flux_table_partitioned') }}
), metrics_geo AS (
  SELECT
    GEO_ID,
    time,
    TOA_SW_DWN,
    CLRSKY_SFC_LW_DWN,
    ALLSKY_SFC_LW_DWN,
    CLRSKY_SFC_SW_DWN,
    ALLSKY_SFC_SW_DWN,
    ST_GEOGPOINT(lon, lat) AS metric_coords
  FROM {{ source('staging','cities') }}
)
SELECT * FROM city_geo a
JOIN metrics_geo b
ON ST_DWithin(a.city_coords, b.metric_coords, 50000)


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}