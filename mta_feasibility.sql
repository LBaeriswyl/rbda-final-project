WITH all_line_station_pairs AS (
  SELECT DISTINCT complex_id, line_name
  FROM avg_station_usage_by_hour,
    UNNEST(regexp_extract_all(line_names, '.')) AS t (line_name)
),
source_stations AS (
  SELECT * FROM avg_station_usage_by_hour WHERE complex_id = 429 --testing
),
destination_stations AS (
  SELECT * FROM avg_station_usage_by_hour WHERE complex_id = 429
),
source_lines AS (
  SELECT DISTINCT line_name
  FROM all_line_station_pairs
  WHERE complex_id IN (SELECT complex_id FROM source_stations)
),
destination_lines AS (
  SELECT DISTINCT line_name
  FROM all_line_station_pairs
  WHERE complex_id IN (SELECT complex_id FROM destination_stations)
),
direct_routes AS (
  SELECT s.line_name
  FROM source_lines s
  INNER JOIN destination_lines d ON s.line_name = d.line_name
),
all_reachable_stations_from_source AS (
  SELECT complex_id
  FROM all_line_station_pairs
  WHERE line_name IN (SELECT line_name FROM source_lines)
),
all_reachable_lines_from_source AS (
  SELECT DISTINCT line_name
  FROM all_line_station_pairs
  WHERE complex_id IN (SELECT complex_id FROM all_reachable_stations_from_source)
),
one_transfer_routes AS (
  SELECT t.line_name
  FROM all_reachable_lines_from_source t
  INNER JOIN destination_lines d ON t.line_name = d.line_name
)
SELECT * FROM all_reachable_lines_from_source;


--- find all stations in the taxi zone/zip code
SELECT *
FROM mta_stations
WHERE zip_code = 10001; -- or taxi_zone = 230


SELECT
    UNNEST()
FROM 
    avg_station_usage_by_hour
WHERE complex_id = 429;

SELECT
    remote_id,
    station_name,
    division_owner,
    line_name,
    hour,
    num_entries,
    num_exits,
    latitude,
    longitude
FROM
    avg_station_usage_by_hour
WHERE complex_id = 429
CROSS JOIN
    UNNEST(split(line_names, '')) AS t (line_name);

SELECT
    complex_id,
    station_name,
    division_owner,
    line_name,
    hour,
    num_entries,
    num_exits,
    latitude,
    longitude
FROM
    (
        SELECT *
        FROM avg_station_usage_by_hour
        WHERE complex_id = 429
    ) 
CROSS JOIN
    UNNEST(split(line_names, '')) AS t (line_name);


--- Test queries
SELECT *
FROM avg_station_usage_by_hour
limit 10;

SELECT regexp_extract_all(line_names,'.') as line_names
FROM avg_station_usage_by_hour
WHERE complex_id = 429;

SELECT DISTINCT line_name
FROM avg_station_usage_by_hour,
     UNNEST(regexp_extract_all(line_names, '.')) AS t (line_name)
WHERE complex_id = 429 OR complex_id = 187;


SELECT DISTINCT complex_id, line_name
FROM avg_station_usage_by_hour,
    UNNEST(regexp_extract_all(line_names, '.')) AS t (line_name)
WHERE complex_id = 429 OR complex_id = 187 or complex_id = 373 or complex_id = 208;


WITH all_line_station_pairs AS (
  SELECT DISTINCT complex_id, line_name
  FROM avg_station_usage_by_hour,
    UNNEST(regexp_extract_all(line_names, '.')) AS t (line_name)
), all_complex_ids AS (
  SELECT * FROM all_line_station_pairs
  WHERE line_name='6'
)
SELECT * FROM avg_station_usage_by_hour
WHERE complex_id IN (SELECT complex_id FROM all_complex_ids)