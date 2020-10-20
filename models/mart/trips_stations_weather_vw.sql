{{ config(materialized='view') }}

--
-- connecting the trip and the weather data together
--

select *
from {{ ref('trips_stations_vw') }} 
    left outer join {{ ref('weather_vw') }} on date_trunc('hour', starttime) = observation_time
where city_id = 5128638
