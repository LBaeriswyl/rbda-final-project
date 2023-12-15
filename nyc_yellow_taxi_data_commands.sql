-- open Hive with:
beeline -u jdbc:hive2://localhost:10000

-- drop if exists
drop table if exists yellow_taxi_data;

-- create external table 
create external table if not exists yellow_taxi_data (
  PU_datetime timestamp, 
  DO_datetime timestamp, 
  passenger_cnt int, 
  trip_dist float, 
  PU_zone int, 
  DO_zone int, 
  total float, 
  congestion_surcharge float) row format delimited fields terminated by ',' location '/user/bc2611_nyu_edu/Final-Project/cleaned_yellow_taxi_data/';

-- average passenger count for trips less than 5 miles
select avg(passenger_cnt) from yellow_taxi_data where trip_dist < 5;

-- ride concentration based on zone
select count(*) as num_trips, PU_zone from yellow_taxi_data group by PU_zone order by num_trips desc limit 10;

-- average number of trips at each hour of day
select avg(num_trips) as avg_num_trips, hour from (select count(*) as num_trips, hour(PU_datetime) as hour from yellow_taxi_data group by hour(PU_datetime)) 
group by hour order by avg_num_trips desc;

-- average congestion surcharge at each hour of day (for years > 2018)
select avg(congestion_surcharge), hour(PU_datetime) as hour from yellow_taxi_data where year(PU_datetime) > 2018 
group by hour(PU_datetime) order by avg(congestion_surcharge) desc limit 10;

-- highest pickup and dropoff zones
select PU_zone, count(PU_zone) as pu_zone_cnt from yellow_taxi_data group by PU_zone order by pu_zone_cnt desc limit 10;
select DO_zone, count(DO_zone) as do_zone_cnt from yellow_taxi_data group by DO_zone order by do_zone_cnt desc limit 10;

-- lowest pickup and dropoff zones
select PU_zone, count(PU_zone) as pu_zone_cnt from yellow_taxi_data group by PU_zone order by pu_zone_cnt asc limit 10;
select DO_zone, count(DO_zone) as do_zone_cnt from yellow_taxi_data group by DO_zone order by do_zone_cnt asc limit 10;

-- most common routes
select (PU_zone, DO_zone), count((PU_zone,DO_zone)) as cnt from yellow_taxi_data group by (PU_zone, DO_zone) order by cnt desc limit 10;

-- zones with highest discrepancy between pickup and dropoff
select PU_zone as zone, abs(t1.cnt1 - t2.cnt2) as diff from (
  (select PU_zone, count(PU_zone) as cnt1 from yellow_taxi_data group by PU_zone) as t1
  INNER JOIN
  ((select DO_zone, count(DO_zone) as cnt2 from yellow_taxi_data group by DO_zone) as t2)
  ON t1.PU_zone = t2.DO_zone)
  order by diff desc limit 10;

