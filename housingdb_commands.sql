beeline -u jdbc:hive2://localhost:10000
set hive.execution.engine=mr;
set hive.fetch.task.conversion=minimal;
use lvb243_nyu_edu;

-- drop if exists
drop table if exists housing_data_inactive_included;

-- Create external table dob_data in Hive with:
-- this table fails to correctly create the data types, most likely because of CSVSerde
-- hence why we create dob_data_from_csv first and then create dob_data from that
CREATE EXTERNAL TABLE housing_data_inactive_included_from_csv (
    job_number INT, job_type STRING, residFlag BOOLEAN, nonresidFlag BOOLEAN, job_status INT, completion_year STRING, permit_year STRING,
    class_a_init INT, class_a_prop INT, class_a_net INT, hotel_init INT, hotel_prop INT, other_b_init INT, other_b_prop INT,
    units_co INT, --current number of units. If job is done, it will be class_a_prop + hotel_prop + other_b_prop
    borough INT, --int representing the borough. Assuming 1 is manhattan, 2 is bronx, 3 is brooklyn, 4 is queens, 5 is staten island (verify this)
    building_identification_number INT, --building identification number, can match on this with dob_data
    bbl INT, --borough block lot number
    address_house_num STRING, address_street String, occ_init STRING, occ_prop STRING, building_class STRING, 
    filing_date DATE, permit_date DATE, last_update_date DATE, completion_date DATE,
    zoning_district_1 STRING, landmark BOOLEAN, floors_init INT, floors_prop INT, enlargement STRING,
    census_block_20 INT, census_tract_20 INT, bctcb_20 STRING, bct_20 STRING, nta_20 STRING, nta_name_20 STRING, cdta_20 STRING, cdta_name_20 STRING,
    community_district STRING, council_district STRING, latitude FLOAT, longitude  FLOAT, geo_source STRING, dcp_edited STRING, hdb_version STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = '\"' --use "" if required
)
STORED AS TEXTFILE
LOCATION '/user/bc2611_nyu_edu/Final-Project/HousingDB_Cleaned_Inactive_Included';

CREATE TABLE housing_data_inactive_included AS 
SELECT CAST(job_number AS INT) AS job_number, job_type, CAST(residFlag AS BOOLEAN) AS residFlag, CAST(nonresidFlag AS BOOLEAN) AS nonresidFlag, CAST(job_status AS INT) AS job_status, completion_year, permit_year,
    CAST(class_a_init AS INT) AS class_a_init, CAST(class_a_prop AS INT) AS class_a_prop, CAST(class_a_net AS INT) AS class_a_net, CAST(hotel_init AS INT) AS hotel_init, CAST(hotel_prop AS INT) AS hotel_prop, CAST(other_b_init AS INT) AS other_b_init, CAST(other_b_prop AS INT) AS other_b_prop,
    CAST(units_co AS FLOAT) AS units_co, CAST(borough AS INT) AS borough, CAST(building_identification_number AS INT) AS building_identification_number, CAST(bbl AS INT) AS bbl, address_house_num, address_street, occ_init, occ_prop, building_class, 
    CAST(filing_date AS DATE) AS filing_date, CAST(permit_date AS DATE) AS permit_date, CAST(last_update_date AS DATE) AS last_update_date, CAST(completion_date AS DATE) AS completion_date,
    zoning_district_1, CAST(landmark AS BOOLEAN) AS landmark, CAST(floors_init AS INT) AS floors_init, CAST(floors_prop AS INT) AS floors_prop, enlargement,
    CAST(census_block_20 AS INT) AS census_block_20, CAST(census_tract_20 AS INT) AS census_tract_20, bctcb_20, bct_20, nta_20, nta_name_20, cdta_20, cdta_name_20,
    community_district, council_district, CAST(latitude AS FLOAT) AS latitude, CAST(longitude AS FLOAT) AS longitude, geo_source, dcp_edited, hdb_version
FROM housing_data_inactive_included_from_csv;


-- test contents were loaded correctly
select * from housing_data_inactive_included_from_csv limit 1;


---find total rows
Select count(*) from housing_data_inactive_included;

---find rows with matching dob_permit entries
WITH dob_zip AS (
  SELECT bin, max(zip_code) as zip_code
  FROM dob_data
  GROUP BY bin
)
Select count(*) from housing_data_inactive_included h
inner join dob_zip d on h.building_identification_number = d.bin;

---find rows per year
SELECT completion_year, count(*) as count
FROM housing_data_inactive_included
WHERE job_status = 5 AND residFlag = true
GROUP BY completion_year
ORDER BY completion_year DESC;

---find units per year
SELECT SUM(class_a_net) as new_units ---difference between class_a_prop and class_a_init
FROM housing_data_inactive_included
WHERE job_status = 5 AND residFlag = true
GROUP BY completion_year
ORDER BY completion_year DESC;

SELECT building_identification_number, count(*) as entries
FROM housing_data_inactive_included
WHERE job_status = 5 AND residFlag = true AND completion_year > '2020'
GROUP BY building_identification_number
ORDER BY entries DESC;

SELECT * from housing_data_inactive_included
WHERE building_identification_number = 4466645;

---find new units per borough (past 3 years)
SELECT borough, SUM(class_a_net) as new_units ---difference between class_a_prop and class_a_init
FROM housing_data_inactive_included
WHERE job_status = 5 AND residFlag = true AND completion_year > '2020'
GROUP BY borough
ORDER BY borough DESC;

---find planned new units per borough (past 3 years)
SELECT borough, SUM(class_a_net) as planned_new_units ---difference between class_a_prop and class_a_init
FROM housing_data_inactive_included
WHERE job_status = 3 AND residFlag = true AND permit_year > '2020'
GROUP BY borough
ORDER BY borough DESC;

--find new units since 2020 per zip code
WITH dob_zip AS (
  SELECT bin, max(zip_code) as zip_code
  FROM dob_data
  GROUP BY bin
), new_units AS (
SELECT d.zip_code, SUM(h.class_a_net) as new_units
FROM housing_data_inactive_included h
INNER JOIN dob_zip d on h.building_identification_number = d.bin
WHERE h.job_status = 5 AND h.residFlag = true AND h.completion_year > '2020'
GROUP BY d.zip_code
ORDER BY new_units DESC
), planned_new_units AS (
SELECT d.zip_code, SUM(h.class_a_net) as new_units
FROM housing_data_inactive_included h
INNER JOIN dob_zip d on h.building_identification_number = d.bin
WHERE h.job_status = 3 AND h.residFlag = true AND h.permit_year > '2020'
GROUP BY d.zip_code
ORDER BY new_units DESC
)
SELECT n.zip_code, n.new_units, p.new_units as planned_new_units
FROM new_units n
INNER JOIN planned_new_units p on n.zip_code = p.zip_code
ORDER BY n.new_units DESC;

--find planned new units since 2020 per zip code
WITH dob_zip AS (
  SELECT bin, max(zip_code) as zip_code
  FROM dob_data
  GROUP BY bin
)
SELECT d.zip_code, SUM(h.class_a_net) as new_units
FROM housing_data_inactive_included h
INNER JOIN dob_zip d on h.building_identification_number = d.bin
WHERE h.job_status = 3 AND h.residFlag = true AND h.permit_year > '2020'
GROUP BY d.zip_code
ORDER BY new_units DESC;


