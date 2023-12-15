import java.io.IOException;
import java.time.format.DateTimeFormatter;	// Immutable and thread-safe
import java.time.format.DateTimeParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MTATurnstileDataCleaningMapper
	extends Mapper<LongWritable, Text, RemoteIDDevAddrTimestampTuple, TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple>
{
	// final variables cannot have their value changed
	// static variables are shared by all class instances
	private final static int remote_id_ind = 1;
	private final static int device_address_ind = 2;
	private final static int station_name_ind = 3;
	private final static int line_names_ind = 4;
	private final static int division_owner_ind = 5;
	private final static int date_ind = 6;
	private final static int time_of_day_ind = 7;
	private final static int record_push_type_ind = 8;
	private final static int num_entries_ind = 9;
	private final static int num_exits_ind = 10;

	private final static String bad_record_push_type = "RECOVR AUD";

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
		// split uses regex matches as delimiters; empty strings are produced if two instances of the delimiters are adjacent to each other or if the delimiter is at the beginning or end of a line
		// \s is any whitespace character
		String[] input_values = value.toString().split("\\s*,\\s*");

		boolean bad_record = false;

		// Check for empty lines
		if (input_values[remote_id_ind].equals(""))
		{
			bad_record = true;
			System.err.println("Station ID missing in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.RemoteID.EMPTY).increment(1);
		}
		if (input_values[device_address_ind].equals(""))
		{
			bad_record = true;
			System.err.println("Device address missing in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.DeviceAddress.EMPTY).increment(1);
		}
		if (input_values[station_name_ind].equals(""))
		{
			bad_record = true;
			System.err.println("Station name missing in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.StationName.EMPTY).increment(1);
		}
		if (input_values[line_names_ind].equals(""))
		{
			bad_record = true;
			System.err.println("Line names missing in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.LineNames.EMPTY).increment(1);
		}
		if (input_values[division_owner_ind].equals(""))
		{
			bad_record = true;
			System.err.println("Division owner missing in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.DivOwner.EMPTY).increment(1);
		}
		if (input_values[record_push_type_ind].equals(bad_record_push_type))
		{
			bad_record = true;
			System.err.println("Data record of type " + bad_record_push_type + " in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.PushType.RECOV_AUD).increment(1);
		}

		// Parse date and time information and combine into a Hive-compatible Unix timestamp
		// Sentinel values to placate the compiler; will never reach a point where this code is put into data, as properly formatted dates and times will overwrite these values and improperly formatted dates and/or times that do not overwrite these times will cause the function to return before they are ever emitted
		LocalDate date = LocalDate.parse("0000-01-01");
		LocalTime time = LocalTime.parse("00:00:00");
		try
		{
			date = LocalDate.parse(input_values[date_ind], DateTimeFormatter.ofPattern("MM/dd/yyyy"));
		}
		catch (DateTimeParseException dtpe)
		{
			bad_record = true;
			System.err.println("Date improperly formatted for input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.Date.NOT_A_DATE).increment(1);
		}
		try
		{
			time = LocalTime.parse(input_values[time_of_day_ind], DateTimeFormatter.ofPattern("HH:mm:ss"));
		}
		catch (DateTimeParseException dtpe1)
		{
			try
			{
				time = LocalTime.parse(input_values[time_of_day_ind], DateTimeFormatter.ofPattern("h:mm:ss a"));
				context.getCounter(MTATurnstileDataCleaning.Time.AM_PM).increment(1);
			}
			catch (DateTimeParseException dtpe2)
			{
				bad_record = true;
				System.err.println("Time improperly formatted for input: " + value);
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(MTATurnstileDataCleaning.Time.NOT_A_TIME).increment(1);
			}
		}

		long num_entries = -1;
		try
		{
			num_entries = Long.parseLong(input_values[num_entries_ind]);
		}
		catch (NumberFormatException nfe)
		{
			bad_record = true;
			System.err.println("Number of entries improperly formatted for input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.NumEntries.NAN).increment(1);
		}

		long num_exits = -1;
		try
		{
			num_exits = Long.parseLong(input_values[num_exits_ind]);
		}
		catch (NumberFormatException nfe)
		{
			bad_record = true;
			System.err.println("Number of exits improperly formatted for input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTATurnstileDataCleaning.NumExits.NAN).increment(1);
		}

		if (bad_record)
		{
			context.getCounter(MTATurnstileDataCleaning.Record.BAD_RECORD).increment(1);
			return;
		}


		// All data is valid; process

		// Sort characters (lines) in line_names column for uniform formatting; Java uses dual-pivot quicksort for primitive types and stable, adaptive, iterative mergesort for Object types
		char[] line_name_arr = input_values[line_names_ind].toCharArray();
		Arrays.sort(line_name_arr);		// In-place sort, hence returns void
		String sorted_line_names = new String(line_name_arr);

		LocalDateTime datetime = LocalDateTime.of(date, time);
		String timestamp = datetime.format(TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple.DT_FMT);

		RemoteIDDevAddrTimestampTuple output_key = new RemoteIDDevAddrTimestampTuple(input_values[remote_id_ind],
				input_values[device_address_ind],
				timestamp
			);

		// Timestamp will be needed in each line of output, but the grouping of keys will neglect distinct timestamps for any entries with identical remote unit ID and device address, so timestamp must be emitted as part of the value as well
		TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple output_value = new TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple(timestamp,
											   input_values[station_name_ind],
											   input_values[division_owner_ind],
											   sorted_line_names,
											   num_entries,
											   num_exits
											  );

		context.write(output_key, output_value);
	}
}
