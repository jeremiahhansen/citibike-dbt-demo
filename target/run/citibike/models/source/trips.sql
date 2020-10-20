

      create or replace transient table citibike.dbt_aeldridge.trips  as
      (--
-- populate the trips table from the reset DB
--

select *
from citibike_reset.citibike_reset_v3.trips
      );
    