

with __dbt__CTE__station_json as (


--
-- get the station data in JSON format
--

with s as (
    select payload, row_inserted from citibike.dbt_aeldridge.gbfs_json
      where data = 'stations'
      and row_inserted = (select max(row_inserted) from citibike.dbt_aeldridge.gbfs_json))
select value station_v,
         payload:response.last_updated::timestamp last_updated,
         row_inserted
from s, lateral flatten (input => payload:response.data.stations)
),  __dbt__CTE__nhood_json as (


--
-- get the neighborhood data in JSON format
--

with n as (
    select payload, row_inserted from citibike.dbt_aeldridge.gbfs_json
      where data = 'neighborhoods'
      and row_inserted = (select max(row_inserted) from citibike.dbt_aeldridge.gbfs_json))
select value neighborhood_v,
    to_geography(value:geometry) geo,
    value:properties.borough::string borough,
    value:properties.neighborhood::string neighborhood
from n, lateral flatten (input => payload:response.features)
)---
--- augment the station records with the neighborhood they are in
---

select station_v,
    last_updated,
    row_inserted,
    st_point(station_v:lon::float, station_v:lat::float) station_geo,
    borough borough_name,
    neighborhood neighborhood_name
from __dbt__CTE__station_json inner join __dbt__CTE__nhood_json
where st_within(station_geo, geo)