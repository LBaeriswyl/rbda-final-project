-- Open Hive with:
	beeline -u jdbc:hive2://localhost:10000

-- To set up the execution environment, run:
	set hive.execution.engine=mr; set hive.fetch.task.conversion=minimal; use <username>

-- Create external table cleaned_mta_turnstile_data from cleaned MTA turnstile data:
	create external table if not exists cleaned_mta_turnstile_data (remote_id string, device_address string, datetime timestamp, station_name string, division_owner string, line_names string, num_entries int, num_exits int) row format delimited fields terminated by ',' location '/user/bc2611_nyu_edu/Final-Project/cleaned-mta-turnstile-data/';

-- Create external table raw_mta_remote_complex_lookup that links remote station unit IDs to station complex IDs
	create external table if not exists raw_mta_remote_complex_lookup (remote_id string, booth_id string, complex_id int, station_name string, line_names string, division_owner string) row format delimited fields terminated by ',' location '/user/bc2611_nyu_edu/Final-Project/raw-mta-remote-complex-lookup/';

-- Create external table cleaned_mta_geo_station_data from cleaned station data to match stationstations to latitude and longitude coordinates, pretty-printed station names and correct subway lines in sorted order; must be run af
	create external table if not exists cleaned_mta_geo_station_data (complex_id int, division_owner string, station_name string, line_names string, GTFS_latitude double, GTFS_longitude double) row format delimited fields terminated by ',' location '/user/bc2611_nyu_edu/Final-Project/cleaned-mta-geo-station-data/';


-- Access data via Trino; run
	presto
-- In presto, run
	use hive.user_ID_nyu_edu;

-- Queries:

-- Create table of station geographic locations that excludes all rows relating to non-subway stations
	create table if not exists mta_geo_station_data as
		select * from cleaned_mta_geo_station_data
		where regexp_like(division_owner, 'BMT|IND|IRT');

-- Create lookup table of remote unit IDs to station complex IDs
	create table if not exists mta_remote_complex_lookup as
		select * from raw_mta_remote_complex_lookup
		where regexp_like(division_owner, 'BMT|IND|IRT')
		and complex_id is not null;

-- Create table for use with actual turnstile queries that drops all rows relating to non-subway stations and that has for every station geographic coordinates (latitude, longitude); nicely formatted station names (from mta_geo_station_data); and a correct, sorted list of servicing lines (from mta_geo_station_data)
	-- Inner join mta_turnstile_data to mta_remote_complex_lookup on remote unit IDs, which joins to mta_geo_station_data on station complex IDs; as statements after the second argument of type table of an inner join provide an alias for said second argument
	create table if not exists mta_turnstile_data as
		select remote_id, device_address, datetime, geo_lookup.station_name, complex_id, geo_lookup.division_owner, geo_lookup.line_names, num_entries, num_exits, gtfs_latitude, gtfs_longitude from cleaned_mta_turnstile_data
			inner join (select remote_id, complex_id, geo.division_owner, geo.line_names, geo.station_name, gtfs_latitude, gtfs_longitude
				from mta_remote_complex_lookup
				inner join (select complex_id, division_owner, line_names, station_name, gtfs_latitude, gtfs_longitude
					from cleaned_mta_geo_station_data) as geo
				using (complex_id)) as geo_lookup
			using (remote_id);

-- To find information by station about the average number of entries at a given station at a given time of day:

	-- Created intermediate table of station usage binned by hour; number of hours may be unevenly distributed because new counter values are only pushed to database once every 4 hours
		-- Arbitrary() function calls are used to allow grouping by one column's values and aggregation of all other columns; for complex_id, does not affect results because aggregations already guarantee unique values of hour and num_entries for each complex_id, while station_name, division_owner and line_names are tied to each complex_id already
		-- NB: Adding "order by complex_id" in selection subquery had no effect
	create table if not exists avg_station_usage_by_hour as select complex_id, arbitrary(station_name) as station_name, arbitrary(division_owner) as division_owner, arbitrary(line_names) as line_names, hour(datetime) as hour, avg(num_entries) as num_entries, avg(num_exits) as num_exits, arbitrary(gtfs_latitude) as latitude, arbitrary(gtfs_longitude) as longitude
		from mta_turnstile_data
		where num_entries >=0 and num_exits >=0
		group by complex_id, hour(datetime);

	-- To find information by station about the busiest time of day
		-- By number of entries:
			select * from avg_station_usage_by_hour
			inner join (select complex_id, max(num_entries) as num_entries
				from avg_station_usage_by_hour
				group by complex_id)
			using (complex_id, num_entries)
			order by complex_id;

		-- By number of exits:
			select * from avg_station_usage_by_hour
			inner join (select complex_id, max(num_exits) as num_exits
				from avg_station_usage_by_hour
				group by complex_id)
			using (complex_id, num_exits)
			order by complex_id;


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
