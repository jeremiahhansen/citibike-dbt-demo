
  create or replace  view citibike.dbt_aeldridge.trips_stations_weather_vw  as (
    

with __dbt__CTE__weather_vw as (


--
-- extracting/calculating the attributes from the weather JSON
--

select v:city.id::int                              city_id
    ,date_trunc('hour', t)                         observation_time
    ,utils.degKtoC(avg(v:main.temp::float))        temp_avg_c
    ,utils.degKtoF(avg(v:main.temp::float))        temp_avg_f
    ,truncate(avg(v:wind.deg::float))              wind_dir
    ,truncate(avg(v:wind.speed::float), 1)         wind_speed_mph
    ,truncate(avg(v:wind.speed::float) * 1.61, 1)  wind_speed_kph
from citibike.demo.weather
group by 1, 2
)--
-- connecting the trip and the weather data together
--

select *
from citibike.dbt_aeldridge.trips_stations_vw 
    left outer join __dbt__CTE__weather_vw on date_trunc('hour', starttime) = observation_time
where city_id = 5128638
  );
