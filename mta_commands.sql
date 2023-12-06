-- Open Hive with:
beeline -u jdbc:hive2://localhost:10000
-- Created external table mta_turnstile_data in Hive with:
create external table if not exists mta_turnstile_data (station_id string, device_address string, datetime timestamp, station_name string, division_owner string, line_names string, num_entries int, num_exits int) row format delimited fields terminated by ',' location '/user/bc2611_nyu_edu/Final-Project/cleaned-mta-turnstile-data/';
-- Created external table mta_station_data in Hive with:
create external table if not exists mta_turnstile_data (station_id int, complex_id int, GTFS_stop_ID string, division_owner string, line_names string, station_name string, borough string, daytime_routes string, station_structure string, GTFS_latitude double, GTFS_longitude double, north_dir_label string, south_dir_label string, ADA_accessibility int, ADA_notes string) row format delimited fields terminated by ',' location '/user/bc2611_nyu_edu/Final-Project/MTA_Subway_Stations.csv';


-- Access data via Trino; run
presto
-- In presto, run
use hive.user_ID_nyu_edu;

-- Queries:

-- To find information by station about the total number of entries at a given station at a given time of day:

	-- Created intermediate table of station usage binned by hour; number of hours may be unevenly distributed because new counter values are only pushed to database once every 4 hours
		-- NB: Adding "order by station_id" in selection subquery had no effect
	create table total_station_usage_by_hour as select station_id, hour(datetime) as hour, sum(num_entries) as num_entries, sum(num_exits) as num_exits
		from mta_turnstile_data
		group by station_id, hour(datetime);

	-- To find information by station about the busiest time of day
		-- Arbitrary() function calls are used to allow grouping by station_id and aggregation of all other columns; does not affect results because subqueries already guarantee unique values of hour and num_entries for each station_id, while station_name, division_owner and line_names are tied to each station_id already
		-- By number of entries:
		select station_id, arbitrary(hour) as hour, arbitrary(station_name) as station_name, arbitrary(division_owner) as division_owner, arbitrary(line_names) as line_names, arbitrary(tab1.num_entries) as num_entries
			from mta_turnstile_data
			inner join (select * from total_station_usage_by_hour
				inner join (select station_id, max(num_entries) as num_entries
					from total_station_usage_by_hour
					group by station_id)
				using (station_id, num_entries)) tab1
			using (station_id)
			group by station_id
			order by station_id;
		-- By number of exits:
		select station_id, arbitrary(hour) as hour, arbitrary(station_name) as station_name, arbitrary(division_owner) as division_owner, arbitrary(line_names) as line_names, arbitrary(tab1.num_exits) as num_exits
			from mta_turnstile_data
			inner join (select * from total_station_usage_by_hour
				inner join (select station_id, max(num_exits) as num_exits
					from total_station_usage_by_hour
					group by station_id)
				using (station_id, num_exits)) tab1
			using (station_id)
			group by station_id
			order by station_id;

-- To find total usage of stations:
	-- Created intermediate table of total usage by station ID (note: some stations of the same name have multiple associated station IDs):
	create table total_station_usage_by_id as
		select station_id, arbitrary(station_name) as station_name, arbitrary(division_owner) as division_owner, arbitrary(line_names) as line_names, arbitrary(tab1.num_entries) as num_entries, arbitrary(tab1.num_exits) as num_exits
			from mta_turnstile_data turn
			inner join (select station_id, sum(num_entries) as num_entries, sum(num_exits) as num_exits
				from mta_turnstile_data
				group by station_id) tab1
			using (station_id)
			group by station_id;

	-- Created intermediate table of total usage by station name (note: some stations have multiple formattings of what is effectively the same name):
	create table total_station_usage_by_name as
		select arbitrary(station_id) as station_id, station_name, arbitrary(division_owner) as division_owner, arbitrary(line_names) as line_names, arbitrary(tab1.num_entries) as num_entries, arbitrary(tab1.num_exits) as num_exits
			from mta_turnstile_data turn
			inner join (select station_id, sum(num_entries) as num_entries, sum(num_exits) as num_exits
				from mta_turnstile_data
				group by station_id) tab1
			using (station_id)
			group by station_name;

-- To find lines with high usage:
	-- Created intermediate table of all lines in system with which to make references in further queries
	-- unnest expands each element of an array into its own row, and cross join returns the Cartesian product of the two tables
	-- unnest_table (line_name) aliases the unnest statement as a table with name unnest_table containing a column with name (line_name)
	create table subway_lines as select distinct line_name from mta_turnstile_data cross join unnest(regexp_extract_all(line_names, '.')) as unnest_table (line_name);

	-- 

-- To attach geographic coordinates (latitude, longitude) to station names:
	-- Because station IDs differ between the two datasets and station names sometimes differ in their formatting, we use the Levenshtein distance to judge whether two stations are identical (alternatives are soundex() to compare phonetic differences in strings (unfortunately not available on Dataproc's installation of Trino) and difference() to compare soundex results (also not available on Dataproc))
	select * from mta_turnstile_data turn join (select station_name, gtfs_latitude, gtfs_longitude from mta_station_data) sta on levenshtein_distance(turn.station_name, sta.station_name) < 2;
