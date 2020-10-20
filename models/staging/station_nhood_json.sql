{{ config(materialized='ephemeral') }}

---
--- augment the station records with the neighborhood they are in
---

select station_v,
    last_updated,
    row_inserted,
    st_point(station_v:lon::float, station_v:lat::float) station_geo,
    borough borough_name,
    neighborhood neighborhood_name
from {{ ref('station_json') }} inner join {{ ref('nhood_json') }}
where st_within(station_geo, geo)
