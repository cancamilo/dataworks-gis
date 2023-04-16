SELECT 
  a.city_name, 
  FORMAT_DATETIME("%B", DATETIME(a.time)) as month_name,
  AVG(a.T2M_CELCIUS) AS monthly_average_temperature,
  AVG(a.WS2M) AS monthly_average_windspeed,
  AVG(a.PRECTOTCORR) AS monthly_average_precipitations,
  AVG(b.ALLSKY_SFC_LW_DWN) As mothly_average_termal_irradiance_lw,
  AVG(b.ALLSKY_SFC_SW_DWN) As mothly_average_termal_irradiance_sw
FROM {{ref('city_geos_model')}} a
INNER JOIN {{ref('city_flux_model')}} b
ON a.city_id = b.city_id AND a.time = b.time
GROUP BY a.city_name, FORMAT_DATETIME("%B", DATETIME(a.time))