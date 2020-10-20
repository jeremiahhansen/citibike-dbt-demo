{{ config(materialized='view') }}

--
-- populate the weather table from the reset DB
--

select v,
       convert_timezone('UTC', 'US/Eastern', v:time::timestamp_ntz) t
from citibike_reset.citibike_reset_v3.weather_json
where v:city.country::string='US'