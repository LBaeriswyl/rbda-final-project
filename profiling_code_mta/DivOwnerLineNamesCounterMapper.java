import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DivOwnerLineNamesCounterMapper
	extends Mapper<LongWritable, Text, Text, TextTuple>
{
	// final variables cannot have their value changed
	// static variables are shared by all class instances
	private final static int station_id_ind = 0;
	private final static int division_owner_ind = 4;
	private final static int line_names_ind = 5;

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
		// split uses regex matches as delimiters; empty strings are produced if two instances of the delimiters are adjacent to each other or if the delimiter is at the beginning or end of a line
		// Input data has been cleaned, so data is known to be split by commas
		String[] input_values = value.toString().split(",");

		context.write(new Text(input_values[station_id_ind]),
					  new TextTuple(input_values[division_owner_ind], input_values[line_names_ind])
					 );
	}
}
