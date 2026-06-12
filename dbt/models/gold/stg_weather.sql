{{ config(materialized='view') }}

WITH raw_weather AS (
    SELECT * FROM {{ source('raw_data', 'weather_hourly') }}
)

SELECT 
    CAST(time AS TIMESTAMP) AS weather_hour,
    CAST(temperature_2m AS NUMERIC) AS temperature_2m,
    CAST(precipitation AS NUMERIC) AS precipitation
FROM raw_weather