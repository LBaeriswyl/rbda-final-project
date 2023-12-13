--- demolitions by neighborhood
--- seems demolitions don't include the residential flag
--- look at dm issuances by neighborhood

WITH residential_flag AS (SELECT 
    a.bin,
    a.residential
FROM 
    dob_data a
INNER JOIN (
    SELECT 
        bin, 
        MAX(filing_date) as max_filing_date
    FROM 
        dob_data
    GROUP BY 
        bin
) b ON a.bin = b.bin
     AND a.filing_date = b.max_filing_date
), latest_issuance_dates AS (
    SELECT job_num, MAX(issuance_date) AS latest_issuance
    FROM dob_data
    GROUP BY job_num
)
SELECT 
    YEAR(l.latest_issuance) as year,
    COUNT(CASE WHEN d.borough = 'MANHATTAN' THEN 1 END) as demolitions_manhattan,
    COUNT(CASE WHEN d.borough = 'QUEENS' THEN 1 END) as demolitions_queens,
    COUNT(CASE WHEN d.borough = 'BROOKLYN' THEN 1 END) as demolitions_brooklyn,
    COUNT(CASE WHEN d.borough = 'BRONX' THEN 1 END) as demolitions_bronx,
    COUNT(CASE WHEN d.borough = 'STATEN ISLAND' THEN 1 END) as demolitions_staten_island
FROM 
    dob_data d
JOIN residential_flag r on d.bin = r.bin
JOIN latest_issuance_dates l ON d.job_num = l.job_num
WHERE 
    d.job_type = 'DM'
    AND d.work_type = '' 
    AND d.permit_status = 'ISSUED' 
    AND d.filing_status = 'INITIAL'
    AND r.residential = 'YES'
GROUP BY 
    YEAR(l.latest_issuance)
ORDER BY 
    year DESC;

--- demolitions by zip code

WITH latest_issuance_dates AS (
    SELECT job_num, MAX(issuance_date) AS latest_issuance
    FROM dob_data
    GROUP BY job_num
), residential_flag AS (SELECT 
    a.bin,
    a.residential
FROM 
    dob_data a
INNER JOIN (
    SELECT 
        bin, 
        MAX(filing_date) as max_filing_date
    FROM 
        dob_data
    GROUP BY 
        bin
) b ON a.bin = b.bin
     AND a.filing_date = b.max_filing_date
)
SELECT zip_code,
    SUM(CASE WHEN YEAR(l.latest_issuance) = 2023 THEN 1 ELSE 0 END) as ongoing_demolitions_2023
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
JOIN residential_flag r on d.bin = r.bin
WHERE 
    (d.job_type = 'DM') AND
    d.work_type = '' AND 
    r.residential = 'YES' AND 
    d.permit_status = 'ISSUED' AND 
    d.filing_status = 'INITIAL'
GROUP BY 
    d.zip_code
ORDER BY ongoing_demolitions_2023 DESC;