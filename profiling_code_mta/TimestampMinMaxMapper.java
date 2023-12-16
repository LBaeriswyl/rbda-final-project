import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimestampMinMaxMapper
	extends Mapper<LongWritable, Text, NullWritable, TextTuple>
{
	// final variables cannot have their value changed
	// static variables are shared by all class instances
	private final static int timestamp_ind = 2;

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
		// split uses regex matches as delimiters; empty strings are produced if two instances of the delimiters are adjacent to each other or if the delimiter is at the beginning or end of a line
		// Input data has been cleaned, so data is known to be split by commas
		String[] input_values = value.toString().split(",");

		context.write(NullWritable.get(),
					  new TextTuple(input_values[timestamp_ind], input_values[timestamp_ind])
					 );
	}
}
