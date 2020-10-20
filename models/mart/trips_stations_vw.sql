{{ config(materialized='view') }}

--
-- trip data enhanced with station location data
--

select *
from {{ ref('trips') }} t
     left outer join {{ ref('ss') }} ss  on t.start_station_id = ss.start_stn_id
     left outer join {{ ref('es') }} es  on t.end_station_id = es.end_stn_id
