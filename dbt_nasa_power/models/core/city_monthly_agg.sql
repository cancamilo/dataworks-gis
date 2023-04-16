SELECT 
  city_name, 
  FORMAT_DATETIME("%B", DATETIME(time)),
  AVG(T2M_CELCIUS) AS monthly_average_temperature,
  AVG(WS2M) AS monthly_average_windspeed,
  AVG(PRECTOTCORR) AS monthly_average_precipitations,
  AVG(ALLSKY_SFC_LW_DWN) As mothly_average_termal_irradiance_lw,
  AVG(ALLSKY_SFC_SW_DWN) As mothly_average_termal_irradiance_sw
FROM {{ref('city_flux_model')}} a
INNER JOIN {{ref('city_geos_model')}} b
ON a.city_id = b.city_id
GROUP BY city, FORMAT_DATETIME("%B", DATETIME(time))