-- Open Hive with:
beeline -u jdbc:hive2://localhost:10000

-- drop if exists
drop table if exists dob_data;

-- Create external table dob_data in Hive with:
-- this table fails to correctly create the data types, most likely because of CSVSerde
-- hence why we create dob_data_from_csv first and then create dob_data from that
CREATE EXTERNAL TABLE dob_data_from_csv (
    borough STRING,
    bin INT, --building identification number
    house_num STRING,
    street STRING,
    job_num INT,
    job_doc_num STRING, --job document number, mostly 01 or 02
    job_type STRING,
    block_num STRING,
    lot_num STRING,
    community_board STRING,
    zip_code INT, --is this safe? Seems like
    building_type STRING,
    residential STRING,
    work_type STRING,
    permit_status STRING,
    filing_status STRING,
    permit_type STRING,
    permit_sequence_num STRING, --permit sequence number, mostly 01 or 02
    permit_subtype STRING,
    filing_date DATE,
    issuance_date DATE,
    expiration_date DATE,
    job_start_date DATE,
    permit_si_no INT,
    latitude FLOAT,
    longitude FLOAT,
    council_district INT,
    cencus_tract INT,
    nta_area STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = '\"' --use "" if required
)
STORED AS TEXTFILE
LOCATION '/user/bc2611_nyu_edu/Final-Project/DOB_Permit_Issuance_Cleaned';

-- Create dob_data table from dob_data_from_csv
CREATE TABLE dob_data AS
SELECT 
    borough,
    CAST(bin AS INT) AS bin,
    house_num,
    street,
    CAST(job_num AS INT) AS job_num,
    job_doc_num,
    job_type,
    block_num,
    lot_num,
    community_board,
    CAST(zip_code AS INT) AS zip_code,
    building_type,
    residential,
    work_type,
    permit_status,
    filing_status,
    permit_type,
    permit_sequence_num,
    permit_subtype,
    CAST(filing_date AS DATE) AS filing_date,
    CAST(issuance_date AS DATE) AS issuance_date,
    CAST(expiration_date AS DATE) AS expiration_date,
    CAST(job_start_date AS DATE) AS job_start_date,
    CAST(permit_si_no AS INT) AS permit_si_no,
    CAST(latitude AS FLOAT) AS latitude,
    CAST(longitude AS FLOAT) AS longitude,
    CAST(council_district AS INT) AS council_district,
    CAST(cencus_tract AS INT) AS cencus_tract,
    nta_area
FROM 
    dob_data_from_csv;

-- test creation of table
Select * from dob_data limit 10;

-- New build permits issued
-- These are simply permits, so they include work types like plumbing, electrical, etc.
-- Get all new build permits issued
SELECT COUNT(*) as total_new_build_permits
FROM dob_data
WHERE job_type = 'NB';

-- Get all new build permits issued by borough
SELECT borough, COUNT(*) as new_build_permit_count
FROM dob_data
WHERE job_type = 'NB'
GROUP BY borough
ORDER BY new_build_permit_count DESC;

-- Get all new build permits issued by nta_area
SELECT nta_area, COUNT(*) as new_build_permit_count
FROM dob_data
WHERE job_type = 'NB'
GROUP BY nta_area
ORDER BY new_build_permit_count DESC;

-- Get all new build permits issued by zip_code
SELECT zip_code, COUNT(*) as new_build_permit_count
FROM dob_data
WHERE job_type = 'NB'
GROUP BY zip_code
ORDER BY new_build_permit_count DESC;

-- Actual new build information
-- These are the deduplicated permits only referring to actual new builds
-- See select new build permits
select count(*) as new_builds
from dob_data
where job_type = 'NB' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL';

-- Select all new build permits issued by work type



SELECT 
    YEAR(job_start_date) as year, 
    COUNT(*) as new_builds
FROM 
    dob_data
WHERE 
    job_type = 'NB' 
    AND work_type = '' 
    AND residential = 'YES' 
    AND permit_status = 'ISSUED' 
    AND filing_status = 'INITIAL'
GROUP BY 
    YEAR(job_start_date)
ORDER BY 
    year DESC;

SELECT 
    YEAR(filing_date) as year, 
    COUNT(*) as new_builds
FROM 
    dob_data
WHERE 
    job_type = 'NB' 
    AND work_type = '' 
    AND residential = 'YES' 
    AND permit_status = 'ISSUED' 
    AND filing_status = 'INITIAL'
GROUP BY 
    YEAR(filing_date)
ORDER BY 
    year DESC;

SELECT 
    YEAR(job_start_date) as year,
    COUNT(CASE WHEN borough = 'MANHATTAN' THEN 1 END) as new_builds_manhattan,
    COUNT(CASE WHEN borough = 'QUEENS' THEN 1 END) as new_builds_queens,
    COUNT(CASE WHEN borough = 'BROOKLYN' THEN 1 END) as new_builds_brooklyn,
    COUNT(CASE WHEN borough = 'BRONX' THEN 1 END) as new_builds_bronx,
    COUNT(CASE WHEN borough = 'STATEN ISLAND' THEN 1 END) as new_builds_staten_island
FROM 
    dob_data
WHERE 
    job_type = 'NB' 
    AND work_type = '' 
    AND residential = 'YES' 
    AND permit_status = 'ISSUED' 
    AND filing_status = 'INITIAL'
GROUP BY 
    YEAR(job_start_date)
ORDER BY 
    year DESC;





-- Random helper queries

-- See all work types
select work_type, count(*) as work_type_counts
from dob_data
group by work_type;

-- See all work types for new build jobs
select work_type, count(*) as work_type_counts
from dob_data
where job_type = 'NB'
group by work_type;

-- See all work types for alterations jobs
select work_type, count(*) as work_type_counts
from dob_data
where job_type = 'A1'
group by work_type;

-- see all job types where work type is 'NB'
select job_type, count(*) as job_type_counts
from dob_data
where work_type = 'NB'
group by job_type;