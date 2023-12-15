use hive.bc2611_nyu_edu;

WITH pickup_drop_data AS (
    SELECT
        pickup_borough,
        pickup_zone,
        t2z_pu.zipcodes AS pickup_zipcodes,
        drop_borough,
        drop_zone,
        t2z_do.zipcodes AS drop_zipcodes,
        COUNT(*) AS num_trips,
        SUM(trip_miles) AS trip_miles
    FROM 
        hive.pt2310_nyu_edu.yellow_taxi_data_final_2 AS yellow_taxi
        JOIN taxizone_to_zipcodes t2z_pu ON t2z_pu.LocationID = pulocationid
        JOIN taxizone_to_zipcodes t2z_do ON t2z_do.LocationID = dolocationid
    WHERE
        yellow_taxi.pickup_borough != '"Unknown"'
        AND yellow_taxi.drop_borough != '"Unknown"'
        AND yellow_taxi.pickup_datetime >= date '2019-01-01'
        AND yellow_taxi.pickup_datetime < date '2022-01-01'
        AND yellow_taxi.trip_miles > 1.6 // 1.6 miles is 50th percentile
        AND (
            t2z_pu.zipcodes LIKE '%11101%'
            OR t2z_pu.zipcodes LIKE '%11201%'
            OR t2z_pu.zipcodes LIKE '%10001%'
        ) 
    GROUP BY
        pickup_borough,
        pickup_zone,
        drop_borough,
        drop_zone,
        t2z_pu.zipcodes,
        t2z_do.zipcodes

    UNION ALL

    SELECT
        pickup_borough,
        pickup_zone,
        t2z_pu.zipcodes AS pickup_zipcodes,
        drop_borough,
        drop_zone,
        t2z_do.zipcodes AS drop_zipcodes,
        COUNT(*) AS num_trips,
        SUM(trip_miles) AS trip_miles
    FROM 
        hive.yc6371_nyu_edu.shareride_data_without_outliers_final AS rideshare
        JOIN taxizone_to_zipcodes t2z_pu ON t2z_pu.LocationID = pulocationid
        JOIN taxizone_to_zipcodes t2z_do ON t2z_do.LocationID = dolocationid
    WHERE
        rideshare.pickup_borough != '"Unknown"'
        AND rideshare.drop_borough != '"Unknown"'
        AND rideshare.pickup_datetime >= date '2019-01-01'
        AND rideshare.pickup_datetime < date '2022-01-01'
        AND rideshare.trip_miles > 2.73 // 2.73 miles is 50th percentile
        AND (
            t2z_pu.zipcodes LIKE '%11101%'
            OR t2z_pu.zipcodes LIKE '%11201%'
            OR t2z_pu.zipcodes LIKE '%10001%'
        ) 
    GROUP BY
        pickup_borough,
        pickup_zone,
        drop_borough,
        drop_zone,
        t2z_pu.zipcodes,
        t2z_do.zipcodes
)


SELECT 
 pickup_borough,
    pickup_zone,
    pickup_zipcodes,
    drop_borough,
    drop_zone,
    drop_zipcodes,
    num_trips,
    avg_trip_miles,
    rank
FROM (
    SELECT
        pickup_borough,
        pickup_zone,
        pickup_zipcodes,
        drop_borough,
        drop_zone,
        drop_zipcodes,
        num_trips,
        trip_miles / num_trips AS avg_trip_miles,
        RANK() OVER (PARTITION BY pickup_zipcodes ORDER BY num_trips DESC) AS rank
    FROM pickup_drop_data
) AS ranked_data
WHERE rank <= 5
ORDER BY pickup_zipcodes, num_trips DESC;




WITH pickup_drop_data AS (
    SELECT
        pickup_borough,
        pickup_zone,
        t2z_pu.zipcodes AS pickup_zipcodes,
        drop_borough,
        drop_zone,
        t2z_do.zipcodes AS drop_zipcodes,
        COUNT(*) AS num_trips,
        SUM(trip_miles) AS trip_miles
    FROM 
        hive.pt2310_nyu_edu.yellow_taxi_data_final_2 AS yellow_taxi
        JOIN taxizone_to_zipcodes t2z_pu ON t2z_pu.LocationID = pulocationid
        JOIN taxizone_to_zipcodes t2z_do ON t2z_do.LocationID = dolocationid
    WHERE
        yellow_taxi.pickup_borough != '"Unknown"'
        AND yellow_taxi.drop_borough != '"Unknown"'
        AND yellow_taxi.pickup_datetime >= date '2019-01-01'
        AND yellow_taxi.pickup_datetime < date '2022-01-01'
        AND yellow_taxi.trip_miles > 1.6 // 1.6 miles is 50th percentile
        AND (
            t2z_pu.zipcodes LIKE '%11217%'
            OR t2z_pu.zipcodes LIKE '%11206%'
            OR t2z_pu.zipcodes LIKE '%11101%'
        ) 
    GROUP BY
        pickup_borough,
        pickup_zone,
        drop_borough,
        drop_zone,
        t2z_pu.zipcodes,
        t2z_do.zipcodes

    UNION ALL

    SELECT
        pickup_borough,
        pickup_zone,
        t2z_pu.zipcodes AS pickup_zipcodes,
        drop_borough,
        drop_zone,
        t2z_do.zipcodes AS drop_zipcodes,
        COUNT(*) AS num_trips,
        SUM(trip_miles) AS trip_miles
    FROM 
        hive.yc6371_nyu_edu.shareride_data_without_outliers_final AS rideshare
        JOIN taxizone_to_zipcodes t2z_pu ON t2z_pu.LocationID = pulocationid
        JOIN taxizone_to_zipcodes t2z_do ON t2z_do.LocationID = dolocationid
    WHERE
        rideshare.pickup_borough != '"Unknown"'
        AND rideshare.drop_borough != '"Unknown"'
        AND rideshare.pickup_datetime >= date '2019-01-01'
        AND rideshare.pickup_datetime < date '2022-01-01'
        AND rideshare.trip_miles > 2.73 // 2.73 miles is 50th percentile
        AND (
            t2z_pu.zipcodes LIKE '%11217%'
            OR t2z_pu.zipcodes LIKE '%11206%'
            OR t2z_pu.zipcodes LIKE '%11101%'
        ) 
    GROUP BY
        pickup_borough,
        pickup_zone,
        drop_borough,
        drop_zone,
        t2z_pu.zipcodes,
        t2z_do.zipcodes
)


SELECT 
 pickup_borough,
    pickup_zone,
    pickup_zipcodes,
    drop_borough,
    drop_zone,
    drop_zipcodes,
    num_trips,
    avg_trip_miles,
    rank
FROM (
    SELECT
        pickup_borough,
        pickup_zone,
        pickup_zipcodes,
        drop_borough,
        drop_zone,
        drop_zipcodes,
        num_trips,
        trip_miles / num_trips AS avg_trip_miles,
        RANK() OVER (PARTITION BY pickup_zipcodes ORDER BY num_trips DESC) AS rank
    FROM pickup_drop_data
) AS ranked_data
WHERE rank <= 5
ORDER BY pickup_zipcodes, num_trips DESC;



