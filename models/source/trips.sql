{{ config(materialized='view') }}

--
-- populate the trips table from the reset DB
--

select *
from citibike_reset.citibike_reset_v3.trips