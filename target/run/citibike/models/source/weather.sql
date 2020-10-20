

      create or replace transient table citibike.dbt_aeldridge.weather  as
      (select v,
       convert_timezone('UTC', 'US/Eastern', v:time::timestamp_ntz) t
from citibike_reset.citibike_reset_v3.weather_json
where v:city.country::string='US'
      );
    