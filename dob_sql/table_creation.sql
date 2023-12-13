beeline -u jdbc:hive2://localhost:10000
set hive.execution.engine=mr;
set hive.fetch.task.conversion=minimal;
use lvb243_nyu_edu;

-- drop if exists
drop table if exists dob_data;
drop table if exists dob_data_from_csv;

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