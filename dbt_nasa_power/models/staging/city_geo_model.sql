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
), metrics_geo AS (
  SELECT
    {{ dbt_utils.surrogate_key(['lon', 'lat', 'time']) }} AS GEO_ID,
    time,
    T2M - 273.15,
    T10M - 273.15,
    WS2M,
    WS10M,
    PRECTOTCORR,
    ST_GEOGPOINT(lon, lat) AS metric_coords
  FROM {{ source('staging','geos_table_partitioned') }}
)
SELECT * FROM cities_data a
JOIN metrics_geo b
ON ST_DWithin(a.city_coords, b.metric_coords, 50000)