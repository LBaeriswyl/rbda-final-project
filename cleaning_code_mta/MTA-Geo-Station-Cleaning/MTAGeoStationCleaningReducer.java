import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MTAGeoStationCleaningReducer
	extends Reducer<IntWritable, Text, NullWritable, Text>
{
	private static final int division_owner_ind = 0;
	private static final int station_name_ind = 1;
	private static final int line_names_ind = 2;
	private static final int latitude_ind = 3;
	private static final int longitude_ind = 4;

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException
	{
		Iterator<Text> val_iter = values.iterator();
		Text init_val = val_iter.next();

		// split uses regex matches as delimiters; empty strings are produced if two instances of the delimiters are adjacent to each other or if the delimiter is at the beginning or end of a line
		// \s is any whitespace character
		String[] input_values = init_val.toString().split("\\s*,\\s*");

		String output = key.toString() + ","
						+ input_values[division_owner_ind] + ","
						+ input_values[station_name_ind] + ",";

		// Take average over latitudes and longitudes
		double lat_sum = Double.parseDouble(input_values[latitude_ind]);
		int lat_count = 1;

		double longit_sum = Double.parseDouble(input_values[longitude_ind]);
		int longit_count = 1;

		String line_names = input_values[line_names_ind];

		// Concatenate names of train lines and accumulate running sum
		while (val_iter.hasNext())
		{
			init_val = val_iter.next();

			lat_sum += Double.parseDouble(input_values[latitude_ind]);
			lat_count++;

			longit_sum += Double.parseDouble(input_values[longitude_ind]);
			longit_count++;

			line_names += input_values[line_names_ind];
		}

		// Sort characters (lines) in line_names column for uniform formatting, making sure to remove any whitespace in between; Java uses dual-pivot quicksort for primitive types and stable, adaptive, iterative mergesort for Object types
		char[] line_name_arr = line_names.replaceAll("\\s+","").toCharArray();
		Arrays.sort(line_name_arr);		// In-place sort, hence returns void

		// De-duplicate
		char prev_char = '\0';	// Sentinel value
		for (int i = 0; i < line_name_arr.length; i++)
		{
			if (line_name_arr[i] != prev_char)
				output += Character.toString(line_name_arr[i]);

			prev_char = line_name_arr[i];
		}

		output += "," + Double.toString(lat_sum/lat_count)
					+ "," + Double.toString(longit_sum/longit_count);

		context.write(NullWritable.get(), new Text(output));
	}
}
