import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DivOwnerLineNamesCounterReducer
	extends Reducer<Text, TextTuple, NullWritable, NullWritable>
{
	@Override
	public void reduce(Text key, Iterable<TextTuple> values, Context context)
		throws IOException, InterruptedException
	{
		// Create one-use Iterator in order to get first element; since the lines servicing a station generally stay the same over time, only the first value is needed
		Iterator<TextTuple> val_iter = values.iterator();
		TextTuple init_val = val_iter.next();

		// Dynamic counter
		context.getCounter("Division Owner", init_val.getT1().toString()).increment(1);

		char[] lines = init_val.getT2().toString().toCharArray();

		for (char line : lines)
			context.getCounter("Lines", Character.toString(line)).increment(1);
	}
}
