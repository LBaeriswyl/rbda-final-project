import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MTAGeoStationCleaningMapper
	extends Mapper<LongWritable, Text, IntWritable, Text>
{
	// final variables cannot have their value changed
	// static variables are shared by all class instances
	private final static int complex_id_ind = 2;
	private final static int division_owner_ind = 3;
	private final static int station_name_ind = 5;
	private final static int line_names_ind = 7;	// Technically named daytime routes in original data
	private final static int gtfs_latitude_ind = 9;
	private final static int gtfs_longitude_ind = 10;

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
		// split uses regex matches as delimiters; empty strings are produced if two instances of the delimiters are adjacent to each other or if the delimiter is at the beginning or end of a line
		// \s is any whitespace character
		String[] input_values = value.toString().split("\\s*,\\s*");

		boolean bad_record = false;

		// Check for invalid complex IDs; initialise with sentinel value so compiler doesn't complain (value won't be used anyway if the record is incorrect, as the loop simply continues to the next iteration)
		int complex_id = -1;
		try
		{
			complex_id = Integer.parseInt(input_values[complex_id_ind]);
		}
		catch (NumberFormatException nfe)
		{
			bad_record = true;
			System.err.println("Complex ID improperly formatted for input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTAGeoStationCleaning.ComplexID.NAN).increment(1);
		}

		// Check for empty lines
		if (input_values[division_owner_ind].equals(""))
		{
			bad_record = true;
			System.err.println("Division owner missing in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTAGeoStationCleaning.DivOwner.EMPTY).increment(1);
		}
		if (input_values[station_name_ind].equals(""))
		{
			bad_record = true;
			System.err.println("Station name missing in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTAGeoStationCleaning.StationName.EMPTY).increment(1);
		}
		if (input_values[line_names_ind].equals(""))
		{
			bad_record = true;
			System.err.println("Line names missing in input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTAGeoStationCleaning.LineNames.EMPTY).increment(1);
		}

		// Sentinel value
		double gtfs_latitude = 0.0;
		try
		{
			gtfs_latitude = Double.parseDouble(input_values[gtfs_latitude_ind]);
		}
		catch (NumberFormatException nfe)
		{
			bad_record = true;
			System.err.println("GTFS latitude improperly formatted for input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTAGeoStationCleaning.GTFSLatitude.NAN).increment(1);
		}

		// Sentinel value
		double gtfs_longitude = 0;
		try
		{
			gtfs_longitude = Double.parseDouble(input_values[gtfs_longitude_ind]);
		}
		catch (NumberFormatException nfe)
		{
			bad_record = true;
			System.err.println("GTFS longitude improperly formatted for input: " + value);
			context.setStatus("Detected possibly corrupt record: see logs.");
			context.getCounter(MTAGeoStationCleaning.GTFSLongitude.NAN).increment(1);
		}

		if (bad_record)
		{
			context.getCounter(MTAGeoStationCleaning.Record.BAD_RECORD).increment(1);
			return;
		}


		// All data is valid; process

		String output_value = input_values[division_owner_ind] + ","
								+ input_values[station_name_ind] + ","
								+ input_values[line_names_ind] + ","
								+ Double.toString(gtfs_latitude) + ","
								+ Double.toString(gtfs_longitude);

		context.write(new IntWritable(complex_id), new Text(output_value));
	}
}
