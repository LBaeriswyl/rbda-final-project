import java.io.IOException;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumEntriesExitsStdevMapper
	extends Mapper<LongWritable, Text, NullWritable, DoubleVLongWritableTupleArrayWritable>
{
	// final variables cannot have their value changed
	// static variables are shared by all class instances
	private final static int num_entries_ind = 6;
	private final static int num_exits_ind = 7;

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
		// split uses regex matches as delimiters; empty strings are produced if two instances of the delimiters are adjacent to each other or if the delimiter is at the beginning or end of a line
		// Input data has been cleaned, so data is known to be split by commas
		String[] input_values = value.toString().split(",");

		long input_num_entries = Long.parseLong(input_values[num_entries_ind]);
		long input_num_exits = Long.parseLong(input_values[num_exits_ind]);

		// Discard any entries where at least one value is a sentinel value
		if (input_num_entries < 0 && input_num_exits < 0)
		{
			context.getCounter(NumEntriesExitsStdev.NumEntries.SENTINEL_VALUE).increment(1);
			context.getCounter(NumEntriesExitsStdev.NumExits.SENTINEL_VALUE).increment(1);
			return;
		}
		else if (input_num_entries < 0)
		{
			context.getCounter(NumEntriesExitsStdev.NumEntries.SENTINEL_VALUE).increment(1);
			return;
		}
		else if (input_num_exits < 0)
		{
			context.getCounter(NumEntriesExitsStdev.NumExits.SENTINEL_VALUE).increment(1);
			return;
		}

		// Get average number of entries and exits from job's Configuration object
		Configuration conf = context.getConfiguration();

		// Default value used if the provided string has no associated property
		double avg_num_entries = conf.getDouble("Average number of entries", -1);
		double avg_num_exits = conf.getDouble("Average number of exits", -1);

		if (avg_num_entries < 0 && avg_num_exits < 0)
		{
			System.err.println("Average numbers of entries and exits not provided; terminating...");
			System.exit(1);
		}
		else if (avg_num_entries < 0)
		{
			System.err.println("Average number of entries not provided; terminating...");
			System.exit(1);
		}
		else if (avg_num_exits < 0)
		{
			System.err.println("Average number of exits not provided; terminating...");
			System.exit(1);
		}

		DoubleVLongWritableTuple num_entries_tuple = new DoubleVLongWritableTuple(Math.pow(input_num_entries - avg_num_entries, 2),
																				  1);
		DoubleVLongWritableTuple num_exits_tuple = new DoubleVLongWritableTuple(Math.pow(input_num_exits - avg_num_exits, 2),
																				1);

		// Put the (sum, counts) tuple for num_entries before the (min, max) tuple for num_exits
		// Array literals are declared with new type[], followed by curly braces enclosing their elements
		context.write(NullWritable.get(),
					  new DoubleVLongWritableTupleArrayWritable(new DoubleVLongWritableTuple[]{num_entries_tuple, num_exits_tuple})
					 );
	}
}
