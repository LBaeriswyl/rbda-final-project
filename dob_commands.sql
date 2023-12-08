-- Open Hive with:
beeline -u jdbc:hive2://localhost:10000

-- drop if exists
drop table if exists dob_data;

-- Create external table dob_data in Hive with:
CREATE EXTERNAL TABLE dob_data (
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
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/bc2611_nyu_edu/Final-Project/DOB_Permit_Issuance_Cleaned';

Select * from dob_data limit 10;