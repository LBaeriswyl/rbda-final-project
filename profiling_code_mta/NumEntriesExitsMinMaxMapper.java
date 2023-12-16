import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumEntriesExitsMinMaxMapper
	extends Mapper<LongWritable, Text, NullWritable, VLongWritableTupleArrayWritable>
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
			context.getCounter(NumEntriesExitsMinMax.NumEntries.SENTINEL_VALUE).increment(1);
			context.getCounter(NumEntriesExitsMinMax.NumExits.SENTINEL_VALUE).increment(1);
			return;
		}
		else if (input_num_entries < 0)
		{
			context.getCounter(NumEntriesExitsMinMax.NumEntries.SENTINEL_VALUE).increment(1);
			return;
		}
		else if (input_num_exits < 0)
		{
			context.getCounter(NumEntriesExitsMinMax.NumExits.SENTINEL_VALUE).increment(1);
			return;
		}

		VLongWritableTuple num_entries_tuple = new VLongWritableTuple(input_num_entries,
																	  input_num_entries
																	 );
		VLongWritableTuple num_exits_tuple = new VLongWritableTuple(input_num_exits,
																	input_num_exits
																   );

		// Put the (min, max) tuple for num_entries before the (min, max) tuple for num_exits
		// Array literals are declared with new type[], followed by curly braces enclosing their elements
		context.write(NullWritable.get(),
					  new VLongWritableTupleArrayWritable(new VLongWritableTuple[]{num_entries_tuple, num_exits_tuple})
					 );
	}
}
