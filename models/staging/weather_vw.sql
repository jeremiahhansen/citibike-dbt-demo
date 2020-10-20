{{ config(materialized='ephemeral') }}

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
from {{ ref('weather') }}
group by 1, 2