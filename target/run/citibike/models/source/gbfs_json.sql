

      create or replace transient table citibike.dbt_aeldridge.gbfs_json  as
      (--- 
--- get the source data from the GBFS web feeds
---

select $1 data,
    $2 url, citibike.utils.fetch_http_data($2) payload,
    current_timestamp() row_inserted
from (values
    ('regions', 'https://gbfs.citibikenyc.com/gbfs/en/system_regions.json'),
    ('stations', 'https://gbfs.citibikenyc.com/gbfs/en/station_information.json'),
    ('neighborhoods', 'https://snowflake-demo-stuff.s3.amazonaws.com/neighborhoods.geojson'))
      );
    