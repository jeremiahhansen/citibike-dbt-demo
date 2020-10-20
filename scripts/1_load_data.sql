/*--------------------------------------------------------------------------------
  In this chapter you will see how you can:
    - easily load your data into Snowflake
	- load semi-structured data, retaining flexible self-describing schemas
    - meet changing workloads by scaling up/down without interruption
    - "load and go" querying data without needing to tune for performance
    - leverage your existing ecosystem of skills and tools for ETL, BI, etc.
--------------------------------------------------------------------------------*/

-- set the context
use role dba_citibike;
create warehouse if not exists load_wh initially_suspended = true auto_suspend=120;
call citibike.utils.set_context('load_wh', 'small', 'citibike.demo');


/*--------------------------------------------------------------------------------
  We are working with the Citibike trip data
    https://www.citibikenyc.com/system-data
--------------------------------------------------------------------------------*/

-- the trip records are CSV files a blob storage container, partitioned by day
list @utils.trips/;

-- how much data is that?
select floor(sum($2)/power(1024, 3),1) total_compressed_storage_gb,
    floor(avg($2)/power(1024, 2),1) avg_file_size_mb,
    count(*) as num_files
  from table(result_scan(last_query_id()));


/*--------------------------------------------------------------------------------
  Load the data from the blob store into a Snowflake table
--------------------------------------------------------------------------------*/

create or replace table trips
  (tripduration integer, starttime timestamp, stoptime timestamp, 
  start_station_id integer, end_station_id integer, bikeid integer,
  usertype string, birth_year integer, gender integer);

alter table trips set change_tracking = true;


-- initially, we just copy one year of data
copy into trips from @utils.trips/2020/;

select count(*) from trips;


/*--------------------------------------------------------------------------------
  But we have more data to load... At this speed it would take over a minute
  which would be rather dull to watch.

  Let's elastically scale up the warehouse 4x to get the job done faster.

  How would you make a query run faster in your current environment? Can you?
--------------------------------------------------------------------------------*/

call citibike.utils.set_wh_size('load_wh', 'large');

copy into trips from @utils.trips/;

call citibike.utils.set_wh_size('load_wh', 'small');

-- check the results
select count(*) from trips;

select * from trips limit 20;


/*--------------------------------------------------------------------------------
  We have staged the weather data, but it's in JSON format
--------------------------------------------------------------------------------*/

list @citibike.utils.weather/;

-- how much data is that?
select truncate(sum($2)/power(1024, 3),1) total_compressed_storage_gb,
       truncate(avg($2)/power(1024, 2),1) avg_file_size_mb,
       count(*) as num_files
from table(result_scan(last_query_id()));

-- what does it look like?
select $1 from @citibike.utils.weather/2020/ limit 20;


/*--------------------------------------------------------------------------------
  Load the weather data into a VARIANT data type
--------------------------------------------------------------------------------*/

create or replace table weather (v variant, t timestamp);

call citibike.utils.set_wh_size('dev_wh', 'xlarge');

-- load the data and convert the timezone from UTC to US/Eastern to match trip data
copy into weather from
  (select $1, convert_timezone('UTC', 'US/Eastern', $1:time::timestamp_ntz)
  from @citibike.utils.weather/);

call citibike.utils.set_wh_size('dev_wh', 'small');

select count(*) from weather;

select * from weather limit 20;
