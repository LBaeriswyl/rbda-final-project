-- top 10 routes including pickup_borough,pickup_zone,drop_borough,drop_zone,zipcodes,num_trips,trips_percent from 2019 to 2021 based on hive.pt2310_nyu_edu.yellow_taxi_data_final and hive.yc6371_nyu_edu.shareride_data_without_outliers_final.
use hive.bc2611_nyu_edu;

create table if not exists num_trips_yellow_taxi_2 AS SELECT * FROM (
    SELECT
        pickup_borough,
        pickup_zone,
        pulocationid,
        t2z_pu.zipcodes AS pickup_zipcodes,
        drop_borough,
        drop_zone,
        dolocationid,
        t2z_do.zipcodes AS drop_zipcodes,
        COUNT(*) AS num_trips,
        AVG(trip_miles) AS avg_trip_miles
    FROM 
        hive.pt2310_nyu_edu.yellow_taxi_data_final_2 AS yellow_taxi
    JOIN taxizone_to_zipcodes t2z_pu ON t2z_pu.LocationID = pulocationid
    JOIN taxizone_to_zipcodes t2z_do ON t2z_do.LocationID = dolocationid
    WHERE
        yellow_taxi.pickup_borough != '"Unknown"'
        AND yellow_taxi.drop_borough != '"Unknown"'
        AND yellow_taxi.pickup_datetime >= date '2019-01-01'
        AND yellow_taxi.pickup_datetime < date '2022-01-01'
    GROUP BY
        pickup_borough,
        pickup_zone,
        pulocationid,
        drop_borough,
        drop_zone,
        dolocationid,
        t2z_pu.zipcodes,
        t2z_do.zipcodes
) x;

create table if not exists num_trips_rideshare_2 AS SELECT * FROM (
    SELECT
        pickup_borough,
        pickup_zone,
        pulocationid,
        t2z_pu.zipcodes AS pickup_zipcodes,
        drop_borough,
        drop_zone,
        dolocationid,
        t2z_do.zipcodes AS drop_zipcodes,
        COUNT(*) AS num_trips,
        AVG(trip_miles) AS avg_trip_miles
    FROM 
        hive.yc6371_nyu_edu.shareride_data_without_outliers_final AS rideshare
    JOIN taxizone_to_zipcodes t2z_pu ON t2z_pu.LocationID = pulocationid
    JOIN taxizone_to_zipcodes t2z_do ON t2z_do.LocationID = dolocationid
    WHERE
        rideshare.pickup_borough != '"Unknown"'
        AND rideshare.drop_borough != '"Unknown"'
        AND rideshare.pickup_datetime >= date '2019-01-01'
        AND rideshare.pickup_datetime < date '2022-01-01'
    GROUP BY
        pickup_borough,
        pickup_zone,
        pulocationid,
        drop_borough,
        drop_zone,
        dolocationid,
        t2z_pu.zipcodes,
        t2z_do.zipcodes
) x;

select 
    taxi.pickup_borough,
    taxi.pickup_zone,
    taxi.pickup_zipcodes,
    taxi.drop_borough,
    taxi.drop_zone,
    taxi.drop_zipcodes,
    taxi.num_trips AS num_trips_yellow_taxi,
    rideshare.num_trips AS num_trips_rideshare,
    (taxi.num_trips + rideshare.num_trips) AS total_num_trips
from
    num_trips_yellow_taxi_2 AS taxi
join 
    num_trips_rideshare_2 AS rideshare
on
    (taxi.pulocationid = rideshare.pulocationid
    AND taxi.dolocationid = rideshare.dolocationid)
order by total_num_trips desc
limit 10;



select
*
from (
    SELECT
        pickup_borough,
        pickup_zone,
        pulocationid,
        t2z_pu.zipcodes AS pickup_zipcodes,
        drop_borough,
        drop_zone,
        dolocationid,
        t2z_do.zipcodes AS drop_zipcodes,
        COUNT(*) AS num_trips,
        AVG(trip_miles) AS avg_trip_miles
    FROM 
        hive.yc6371_nyu_edu.shareride_data_without_outliers_final AS rideshare
    JOIN taxizone_to_zipcodes t2z_pu ON t2z_pu.LocationID = pulocationid
    JOIN taxizone_to_zipcodes t2z_do ON t2z_do.LocationID = dolocationid
    WHERE
        rideshare.pickup_borough != '"Unknown"'
        AND rideshare.drop_borough != '"Unknown"'
        AND rideshare.pickup_datetime >= date '2019-01-01'
        AND rideshare.pickup_datetime < date '2022-01-01'
        AND rideshare.trip_miles > 2.73
    GROUP BY
        pickup_borough,
        pickup_zone,
        pulocationid,
        drop_borough,
        drop_zone,
        dolocationid,
        t2z_pu.zipcodes,
        t2z_do.zipcodes
)
order by num_trips desc
limit 10;

select
*
from (
    SELECT
        pickup_borough,
        pickup_zone,
        pulocationid,
        t2z_pu.zipcodes AS pickup_zipcodes,
        drop_borough,
        drop_zone,
        dolocationid,
        t2z_do.zipcodes AS drop_zipcodes,
        COUNT(*) AS num_trips,
        AVG(trip_miles) AS avg_trip_miles
    FROM 
        hive.pt2310_nyu_edu.yellow_taxi_data_final_2 AS yellow_taxi
    JOIN taxizone_to_zipcodes t2z_pu ON t2z_pu.LocationID = pulocationid
    JOIN taxizone_to_zipcodes t2z_do ON t2z_do.LocationID = dolocationid
    WHERE
        yellow_taxi.pickup_borough != '"Unknown"'
        AND yellow_taxi.drop_borough != '"Unknown"'
        AND yellow_taxi.pickup_datetime >= date '2019-01-01'
        AND yellow_taxi.pickup_datetime < date '2022-01-01'
        AND yellow_taxi.trip_miles > 1.6
    GROUP BY
        pickup_borough,
        pickup_zone,
        pulocationid,
        drop_borough,
        drop_zone,
        dolocationid,
        t2z_pu.zipcodes,
        t2z_do.zipcodes
)
order by num_trips desc
limit 10;

-- find the median of trip_miles of combined table of yellow taxi and rideshare
select APPROX_PERCENTILE (trip_miles, 0.5) as median_trip_miles from (
    select 
        trip_miles
    from hive.pt2310_nyu_edu.yellow_taxi_data_final_2 as yellow_taxi
    WHERE
        yellow_taxi.pickup_borough != '"Unknown"'
        AND yellow_taxi.drop_borough != '"Unknown"'
        AND yellow_taxi.pickup_datetime >= date '2019-01-01'
        AND yellow_taxi.pickup_datetime < date '2022-01-01'
        
    union all
    
    select
        trip_miles
    from
        hive.yc6371_nyu_edu.shareride_data_without_outliers_final as rideshare
    WHERE
        rideshare.pickup_borough != '"Unknown"'
        AND rideshare.drop_borough != '"Unknown"'
        AND rideshare.pickup_datetime >= date '2019-01-01'
        AND rideshare.pickup_datetime < date '2022-01-01'
) x;

select APPROX_PERCENTILE (trip_miles, 0.5) as yellow_taxi_median_trip_miles from (
    select 
        trip_miles
    from hive.pt2310_nyu_edu.yellow_taxi_data_final_2 as yellow_taxi
    WHERE
        yellow_taxi.pickup_borough != '"Unknown"'
        AND yellow_taxi.drop_borough != '"Unknown"'
        AND yellow_taxi.pickup_datetime >= date '2019-01-01'
        AND yellow_taxi.pickup_datetime < date '2022-01-01'
) x;

select APPROX_PERCENTILE (trip_miles, 0.5) as rideshare_median_trip_miles from (
    select
        trip_miles
    from
        hive.yc6371_nyu_edu.shareride_data_without_outliers_final as rideshare
    WHERE
        rideshare.pickup_borough != '"Unknown"'
        AND rideshare.drop_borough != '"Unknown"'
        AND rideshare.pickup_datetime >= date '2019-01-01'
        AND rideshare.pickup_datetime < date '2022-01-01'
) x;
