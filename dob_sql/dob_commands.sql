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

-- See all job types
select job_type, count(*) as job_type_counts
from dob_data
group by job_type;

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

-- See all work types for demolition jobs
select work_type, count(*) as work_type_counts
from dob_data
where job_type = 'DM'
group by work_type;


--- Analyses as described in the report
--- select only initial filings for residential new builds (deduplicated)
--- use this to do further analyses? Maybe can do nice graphics in tableau
select *
from dob_data
where job_type = 'NB' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL';

--- number of new build permits issued
select count(*) as new_builds
from dob_data
where job_type = 'NB' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL';

--- number of new build permits issued by year
select YEAR(issuance_date) as issuance_year, count(*) as new_builds
from dob_data
where job_type = 'NB' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL'
GROUP BY YEAR(issuance_date)
ORDER BY issuance_year DESC;

--- number of new build project start dates per year
select YEAR(job_start_date) as job_start_year, count(*) as new_builds
from dob_data
where job_type = 'NB' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL' and YEAR(job_start_date) < 2024
GROUP BY YEAR(job_start_date)
ORDER BY job_start_year DESC;

-- verify that permit issuances and start dates are interchangeable
select count(*) as new_builds
from dob_data
where job_type = 'NB' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL' and YEAR(job_start_date) = YEAR(issuance_date);

--- select only initial filings for residential alterations (deduplicated)
--- use this to do further analyses? Maybe can do nice graphics in tableau
select *
from dob_data
where job_type = 'A1' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL';

--- number of alterations permits issued
select count(*) as alterations
from dob_data
where job_type = 'A1' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL' and YEAR(job_start_date) = YEAR(issuance_date);

--- number of A1 permits issued by year
select YEAR(issuance_date) as issuance_year, count(*) as alterations
from dob_data
where job_type = 'A1' and work_type = '' and residential = 'YES' and permit_status = 'ISSUED' and filing_status = 'INITIAL'
GROUP BY YEAR(issuance_date)
ORDER BY issuance_year DESC;

--- number of A1 permits issued by year not deduped, for any work
--- this doesn't change things massively
select YEAR(issuance_date) as issuance_year, count(*) as alterations
from dob_data
where job_type = 'A1' and residential = 'YES' and permit_status = 'ISSUED'
GROUP BY YEAR(issuance_date)
ORDER BY issuance_year DESC;

select YEAR(issuance_date) as issuance_year, count(*) as permits
from dob_data
GROUP BY YEAR(issuance_date)
ORDER BY issuance_year DESC;

--- find currently ongoing work per year
WITH latest_issuance_dates AS (
    SELECT job_num, MAX(issuance_date) AS latest_issuance
    FROM dob_data
    GROUP BY job_num
)
SELECT YEAR(l.latest_issuance) as latest_issuance_year, COUNT(*) as ongoing_nb_work
FROM dob_data d
JOIN latest_issuance_dates l ON d.job_num = l.job_num
WHERE d.job_type = 'NB' 
    AND d.work_type = '' 
    AND d.residential = 'YES' 
    AND d.permit_status = 'ISSUED' 
    AND d.filing_status = 'INITIAL'
GROUP BY YEAR(l.latest_issuance)
ORDER BY latest_issuance_year DESC;

WITH latest_issuance_dates AS (
    SELECT job_num, MAX(issuance_date) AS latest_issuance
    FROM dob_data
    GROUP BY job_num
)
SELECT YEAR(l.latest_issuance) as latest_issuance_year, COUNT(*) as ongoing_a1_work
FROM dob_data d
JOIN latest_issuance_dates l ON d.job_num = l.job_num
WHERE d.job_type = 'A1' 
    AND d.work_type = '' 
    AND d.residential = 'YES' 
    AND d.permit_status = 'ISSUED' 
    AND d.filing_status = 'INITIAL'
GROUP BY YEAR(l.latest_issuance)
ORDER BY latest_issuance_year DESC;

--- look at issuances by neighborhood
SELECT 
    YEAR(issuance_date) as year,
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
    YEAR(issuance_date)
ORDER BY 
    year DESC;

--- A1 issuances by neighborhood
SELECT 
    YEAR(issuance_date) as year,
    COUNT(CASE WHEN borough = 'MANHATTAN' THEN 1 END) as new_builds_manhattan,
    COUNT(CASE WHEN borough = 'QUEENS' THEN 1 END) as new_builds_queens,
    COUNT(CASE WHEN borough = 'BROOKLYN' THEN 1 END) as new_builds_brooklyn,
    COUNT(CASE WHEN borough = 'BRONX' THEN 1 END) as new_builds_bronx,
    COUNT(CASE WHEN borough = 'STATEN ISLAND' THEN 1 END) as new_builds_staten_island
FROM 
    dob_data
WHERE 
    job_type = 'A1'
    AND work_type = '' 
    AND residential = 'YES' 
    AND permit_status = 'ISSUED' 
    AND filing_status = 'INITIAL'
GROUP BY 
    YEAR(issuance_date)
ORDER BY 
    year DESC;

--- A1 or NB issuances by neighborhood
SELECT 
    YEAR(issuance_date) as year,
    COUNT(CASE WHEN borough = 'MANHATTAN' THEN 1 END) as new_builds_manhattan,
    COUNT(CASE WHEN borough = 'QUEENS' THEN 1 END) as new_builds_queens,
    COUNT(CASE WHEN borough = 'BROOKLYN' THEN 1 END) as new_builds_brooklyn,
    COUNT(CASE WHEN borough = 'BRONX' THEN 1 END) as new_builds_bronx,
    COUNT(CASE WHEN borough = 'STATEN ISLAND' THEN 1 END) as new_builds_staten_island
FROM 
    dob_data
WHERE 
    (job_type = 'NB' OR job_type = 'A1')
    AND work_type = '' 
    AND residential = 'YES' 
    AND permit_status = 'ISSUED' 
    AND filing_status = 'INITIAL'
GROUP BY 
    YEAR(issuance_date)
ORDER BY 
    year DESC;

--- ongoing A1 or NB work
WITH latest_issuance_dates AS (
    SELECT job_num, MAX(issuance_date) AS latest_issuance
    FROM dob_data
    GROUP BY job_num
)
SELECT 
    YEAR(l.latest_issuance) as year,
    COUNT(CASE WHEN d.borough = 'MANHATTAN' THEN 1 END) as ongoing_manhattan,
    COUNT(CASE WHEN d.borough = 'QUEENS' THEN 1 END) as ongoing_queens,
    COUNT(CASE WHEN d.borough = 'BROOKLYN' THEN 1 END) as ongoing_brooklyn,
    COUNT(CASE WHEN d.borough = 'BRONX' THEN 1 END) as ongoing_bronx,
    COUNT(CASE WHEN d.borough = 'STATEN ISLAND' THEN 1 END) as ongoing_staten_island
FROM 
    dob_data d
JOIN latest_issuance_dates l ON d.job_num = l.job_num
WHERE 
    (d.job_type = 'NB' OR d.job_type = 'A1')
    AND d.work_type = '' 
    AND d.residential = 'YES' 
    AND d.permit_status = 'ISSUED' 
    AND d.filing_status = 'INITIAL'
GROUP BY 
    YEAR(l.latest_issuance)
ORDER BY 
    year DESC;

--- get horizontal per-year view of zip codes
SELECT zip_code,
    (SUM(CASE WHEN YEAR(issuance_date) = 2021 THEN 1 ELSE 0 END) + 
     SUM(CASE WHEN YEAR(issuance_date) = 2022 THEN 1 ELSE 0 END) + 
     SUM(CASE WHEN YEAR(issuance_date) = 2023 THEN 1 ELSE 0 END)) as last_3_years
    ---SUM(CASE WHEN YEAR(issuance_date) = 2010 THEN 1 ELSE 0 END) AS new_builds_2010,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2011 THEN 1 ELSE 0 END) AS new_builds_2011,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2012 THEN 1 ELSE 0 END) AS new_builds_2012,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2013 THEN 1 ELSE 0 END) AS new_builds_2013,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2014 THEN 1 ELSE 0 END) AS new_builds_2014,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2015 THEN 1 ELSE 0 END) AS new_builds_2015,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2016 THEN 1 ELSE 0 END) AS new_builds_2016,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2017 THEN 1 ELSE 0 END) AS new_builds_2017,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2018 THEN 1 ELSE 0 END) AS new_builds_2018,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2019 THEN 1 ELSE 0 END) AS new_builds_2019,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2020 THEN 1 ELSE 0 END) AS new_builds_2020,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2021 THEN 1 ELSE 0 END) AS new_builds_2021,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2022 THEN 1 ELSE 0 END) AS new_builds_2022,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2023 THEN 1 ELSE 0 END) AS new_builds_2023
FROM 
    dob_data
WHERE 
    (job_type = 'NB' OR job_type = 'A1') AND
    work_type = '' AND 
    residential = 'YES' AND 
    permit_status = 'ISSUED' AND 
    filing_status = 'INITIAL'
GROUP BY 
    zip_code
ORDER BY last_3_years DESC;

--- get horizontal per-year view of zip codes for ongoing/active work
WITH latest_issuance_dates AS (
    SELECT job_num, MAX(issuance_date) AS latest_issuance
    FROM dob_data
    GROUP BY job_num
)
SELECT zip_code,
    SUM(CASE WHEN YEAR(l.latest_issuance) = 2023 THEN 1 ELSE 0 END) as ongoing_2023
    ---SUM(CASE WHEN YEAR(issuance_date) = 2010 THEN 1 ELSE 0 END) AS new_builds_2010,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2011 THEN 1 ELSE 0 END) AS new_builds_2011,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2012 THEN 1 ELSE 0 END) AS new_builds_2012,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2013 THEN 1 ELSE 0 END) AS new_builds_2013,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2014 THEN 1 ELSE 0 END) AS new_builds_2014,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2015 THEN 1 ELSE 0 END) AS new_builds_2015,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2016 THEN 1 ELSE 0 END) AS new_builds_2016,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2017 THEN 1 ELSE 0 END) AS new_builds_2017,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2018 THEN 1 ELSE 0 END) AS new_builds_2018,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2019 THEN 1 ELSE 0 END) AS new_builds_2019,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2020 THEN 1 ELSE 0 END) AS new_builds_2020,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2021 THEN 1 ELSE 0 END) AS new_builds_2021,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2022 THEN 1 ELSE 0 END) AS new_builds_2022,
    ---SUM(CASE WHEN YEAR(issuance_date) = 2023 THEN 1 ELSE 0 END) AS new_builds_2023
FROM 
    dob_data d
JOIN latest_issuance_dates l ON d.job_num = l.job_num
WHERE 
    (d.job_type = 'NB' OR d.job_type = 'A1') AND
    d.work_type = '' AND 
    d.residential = 'YES' AND 
    d.permit_status = 'ISSUED' AND 
    d.filing_status = 'INITIAL'
GROUP BY 
    d.zip_code
ORDER BY ongoing_2023 DESC;

