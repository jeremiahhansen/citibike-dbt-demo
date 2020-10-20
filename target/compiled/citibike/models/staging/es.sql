

select station_id end_stn_id,
    station_name end_station, region_name end_region,
    borough_name end_borough, neighborhood_name end_neighborhood,
    station_lat end_lat, station_lon end_lon, station_geo end_geo
from citibike.dbt_aeldridge.stations