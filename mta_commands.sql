-- Open Hive with:
beeline -u jdbc:hive2://localhost:10000
-- To set up the execution environment, run:
set hive.execution.engine=mr; set hive.fetch.task.conversion=minimal; use <username>
-- Created external table cleaned_mta_turnstile_data in Hive with:
create external table if not exists cleaned_mta_turnstile_data (remote_id string, device_address string, datetime timestamp, station_name string, division_owner string, line_names string, num_entries int, num_exits int) row format delimited fields terminated by ',' location '/user/bc2611_nyu_edu/Final-Project/cleaned-mta-turnstile-data/';
-- Created external table raw_mta_geo_station_data in Hive with:
create external table if not exists raw_mta_geo_station_data (GTFS_stop_ID string, remote_id int, complex_id int, division_owner string, line_names string, station_name string, borough string, daytime_routes string, station_structure string, GTFS_latitude double, GTFS_longitude double, north_dir_label string, south_dir_label string, ADA_accessibility int, ADA_north_accessibility int, ADA_South_accessibility int, ADA_notes string, georef string) row format delimited fields terminated by ',' location '/user/bc2611_nyu_edu/Final-Project/raw-mta-geo-station-data/';


-- Access data via Trino; run
presto
-- In presto, run
use hive.user_ID_nyu_edu;

-- Queries:

-- Create table for use with actual turnstile queries that drops all rows relating to non-subway stations
	create table mta_turnstile_data as
		select * from cleaned_mta_turnstile_data
		where regexp_like(division_owner, 'BMT|IND|IRT');

-- Create table for use with actual station queries that drops all rows relating to non-subway stations
	create table mta_geo_station_data as
		select * from raw_mta_geo_station_data
		where regexp_like(division_owner, 'BMT|IND|IRT')
		and remote_id is not null;

-- To find information by station about the average number of entries at a given station at a given time of day:

	-- Created intermediate table of station usage binned by hour; number of hours may be unevenly distributed because new counter values are only pushed to database once every 4 hours
		-- Arbitrary() function calls are used to allow grouping by one column's values and aggregation of all other columns; for remote_id, does not affect results because subqueries already guarantee unique values of hour and num_entries for each remote_id, while station_name, division_owner and line_names are tied to each remote_id already
		-- NB: Adding "order by remote_id" in selection subquery had no effect
	create table avg_station_usage_by_hour as select remote_id, arbitrary(station_name) as station_name, arbitrary(division_owner) as division_owner, arbitrary(line_names) as line_names, hour(datetime) as hour, avg(num_entries) as num_entries, avg(num_exits) as num_exits
		from mta_turnstile_data
		where num_entries >=0 and num_exits >=0
		group by remote_id, hour(datetime);

	-- To find information by station about the busiest time of day
		-- By number of entries:
			select * from avg_station_usage_by_hour
			inner join (select remote_id, max(num_entries) as num_entries
				from avg_station_usage_by_hour
				group by remote_id)
			using (remote_id, num_entries)
			order by remote_id;

		-- By number of exits:
			select * from avg_station_usage_by_hour
			inner join (select remote_id, max(num_exits) as num_exits
				from avg_station_usage_by_hour
				group by remote_id)
			using (remote_id, num_exits)
			order by remote_id;


-- To find lines with high usage:
	-- Aggregate the average number of entries and exits for all stations served by a given line
	-- unnest expands each element of an array into its own row, and cross join returns the Cartesian product of the two tables
	-- unnest_table (line_name) aliases the unnest statement as a table with name unnest_table containing a column with name (line_name)
	select line_name, avg(num_entries) as num_entries, avg(num_exits) as num_exits
		from mta_turnstile_data
		cross join unnest(regexp_extract_all(line_names, '.')) as unnest_table (line_name)
		where num_entries >=0 and num_exits >=0
		group by line_name
		order by line_name;


-- To attach geographic coordinates (latitude, longitude) to station names:
	-- Because station IDs differ between the two datasets and station names sometimes differ in their formatting, we use the Levenshtein distance to judge whether two stations are identical (alternatives are soundex() to compare phonetic differences in strings (unfortunately not available on Dataproc's installation of Trino) and difference() to compare soundex results (also not available on Dataproc))
	-- Given the ease with which short names agree (e.g. 7 AV vs. 30 AV), the edit distance for which a station can be considered to agree must vary with the length of the station name
	select *
		from mta_turnstile_data turn
		join (select station_name, gtfs_latitude, gtfs_longitude
			from mta_geo_station_data) sta
		on levenshtein_distance(turn.station_name, sta.station_name) < 2;
