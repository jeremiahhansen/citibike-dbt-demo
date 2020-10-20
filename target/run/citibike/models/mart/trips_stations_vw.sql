
  create or replace  view citibike.dbt_aeldridge.trips_stations_vw  as (
    

with __dbt__CTE__ss as (


select station_id start_stn_id,
    station_name start_station, region_name start_region,
    borough_name start_borough, neighborhood_name start_neighborhood,
    station_lat start_lat, station_lon start_lon, station_geo start_geo
from citibike.dbt_aeldridge.stations
),  __dbt__CTE__es as (


select station_id end_stn_id,
    station_name end_station, region_name end_region,
    borough_name end_borough, neighborhood_name end_neighborhood,
    station_lat end_lat, station_lon end_lon, station_geo end_geo
from citibike.dbt_aeldridge.stations
)--
-- trip data enhanced with station location data
--

select *
from citibike.demo.trips t
     left outer join __dbt__CTE__ss ss  on t.start_station_id = ss.start_stn_id
     left outer join __dbt__CTE__es es  on t.end_station_id = es.end_stn_id
  );
