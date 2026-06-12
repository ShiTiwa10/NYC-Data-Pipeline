{{ config(materialized='table') }}

WITH trips AS (
    SELECT 
        trip_hour,
        trip_date,
        COUNT(trip_id) AS total_trips,
        COUNT(CASE WHEN rider_type = 'member' THEN 1 END) AS member_trips,
        COUNT(CASE WHEN rider_type = 'casual' THEN 1 END) AS casual_trips
    FROM {{ ref('stg_citibike_trips') }}
    GROUP BY 1, 2
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }} 
),

holidays AS (
    SELECT * FROM {{ ref('nyc_holidays') }} 
)

SELECT 
    t.trip_hour,
    t.trip_date,
    t.total_trips,
    t.member_trips,
    t.casual_trips,
    w.temperature_2m,
    w.precipitation,
    CASE WHEN h.holiday_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_holiday,
    h.holiday_name
FROM trips t
LEFT JOIN weather w 
    ON t.trip_hour = w.weather_hour
LEFT JOIN holidays h 
    ON t.trip_date = h.holiday_date