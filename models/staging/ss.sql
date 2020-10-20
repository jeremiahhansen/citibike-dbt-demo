{{ config(materialized='ephemeral') }}

select station_id start_stn_id,
    station_name start_station, region_name start_region,
    borough_name start_borough, neighborhood_name start_neighborhood,
    station_lat start_lat, station_lon start_lon, station_geo start_geo
from {{ ref('stations') }}