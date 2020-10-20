{{ config(materialized='ephemeral') }}

--
-- get the station data in JSON format
--

with s as (
    select payload, row_inserted from {{ ref('gbfs_json') }}
      where data = 'stations'
      and row_inserted = (select max(row_inserted) from {{ ref('gbfs_json') }}))
select value station_v,
         payload:response.last_updated::timestamp last_updated,
         row_inserted
from s, lateral flatten (input => payload:response.data.stations)
