

--
-- get the region data in JSON format
--

with r as (
  select payload, row_inserted from citibike.dbt_aeldridge.gbfs_json
      where data = 'regions'
      and row_inserted = (select max(row_inserted) from citibike.dbt_aeldridge.gbfs_json))
select value region_v,
         payload:response.last_updated::timestamp last_updated,
         row_inserted
from r, lateral flatten (input => payload:response.data.regions)