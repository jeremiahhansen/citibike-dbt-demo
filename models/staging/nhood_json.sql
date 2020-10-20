{{ config(materialized='ephemeral') }}

--
-- get the neighborhood data in JSON format
--

with n as (
    select payload, row_inserted from {{ ref('gbfs_json') }}
      where data = 'neighborhoods'
      and row_inserted = (select max(row_inserted) from {{ ref('gbfs_json') }}))
select value neighborhood_v,
    to_geography(value:geometry) geo,
    value:properties.borough::string borough,
    value:properties.neighborhood::string neighborhood
from n, lateral flatten (input => payload:response.features)