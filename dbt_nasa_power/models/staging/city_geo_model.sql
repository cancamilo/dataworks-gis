WITH city_geo AS (
  SELECT
      id,
      city,
      country,
      capital,
      population,
      admin_name,      
      ST_GEOGPOINT(lng, lat) AS city_coords
    FROM `dataworks-gis.test_gis.cities`
), metrics_geo AS (
  SELECT
    GEO_ID,
    time,
    T2M,
    T10M,
    WS2M,
    WS10M,
    PRECTOTCORR,
    ST_GEOGPOINT(lon, lat) AS metric_coords
  FROM `dataworks-gis.test_gis.ceres`
)
SELECT * FROM city_geo a
JOIN metrics_geo b
ON ST_DWithin(a.city_coords, b.metric_coords, 50000)

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}