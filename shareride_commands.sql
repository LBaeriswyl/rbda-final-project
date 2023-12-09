-- Open Hive with:
beeline -u jdbc:hive2://localhost:10000

-- drop if exists
drop table if exists shareride_data;

-- Created external table shareride_data in Hive with:
CREATE EXTERNAL TABLE shareride_data (
    base_passenger_fare FLOAT,
    bcf FLOAT,
    congestion_surcharge FLOAT,
    DOLocationID INT,
    dropoff_datetime TIMESTAMP,
    hvfhs_license_num STRING,
    PULocationID INT,
    pickup_datetime TIMESTAMP,
    request_datetime TIMESTAMP,
    sales_tax FLOAT,
    tips FLOAT,
    tolls FLOAT,
    total_charges FLOAT,
    trip_miles FLOAT,
    trip_time INT,
    wait_time INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
LOCATION '/user/yc6371_nyu_edu/project/cleaned-shareride-data-final';

-- testing
select * from shareride_data limit 1

-- Created external table fhv_zone_lookup in Hive with:
create external table if not exists fhv_zone_lookup (LocationID int, Borough string, Zone string, service_zone string) row format delimited fields terminated by ',' location '/user/yc6371_nyu_edu/project/zone_lookup';

presto
use hive.yc6371_nyu_edu;

-- Which areas have the highest concentration of ride-booking?
select count(*) as num_trips, PULocationID from shareride_data group by PULocationID order by num_trips desc limit 10;

-- average number of trips at each hour of day
select avg(num_trips) as avg_num_trips, hour from (select count(*) as num_trips, hour(pickup_datetime) as hour from shareride_data group by hour(pickup_datetime)) group by hour order by hour;

-- What is the average velocity of a trip by hvfhs_license_num?
select hvfhs_license_num, avg(trip_miles/trip_time) as avg_velocity from shareride_data group by hvfhs_license_num order by avg_velocity desc;

-- average number of trips per day by platform
select hvfhs_license_num, avg(num_trips) as avg_num_trips from (select count(*) as num_trips, hvfhs_license_num, date(pickup_datetime) as date from shareride_data group by date(pickup_datetime), hvfhs_license_num) group by hvfhs_license_num order by avg_num_trips desc;

-- What is the percentage of market share calculated from the sum of total_charges by hvfhs_license_num in each year?
select hvfhs_license_num, year(pickup_datetime) as year, sum(total_charges) as sum_total_charges, sum(total_charges)/(select sum(total_charges) from shareride_data) as market_share from shareride_data group by year(pickup_datetime), hvfhs_license_num order by year, market_share desc;

-- What is the average trip_time by hvfhs_license_num?
select hvfhs_license_num, avg(trip_time) as avg_trip_time from shareride_data group by hvfhs_license_num order by avg_trip_time desc;

-- What is the average tips (in %) by hvfhs_license_num?
select hvfhs_license_num, avg(tips/total_charges) as avg_tips from shareride_data group by hvfhs_license_num order by avg_tips desc;

-- What is the average trip_miles by hvfhs_license_num?
select hvfhs_license_num, avg(trip_miles) as avg_trip_miles from shareride_data group by hvfhs_license_num order by avg_trip_miles desc;
