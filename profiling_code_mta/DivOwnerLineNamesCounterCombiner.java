import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DivOwnerLineNamesCounterCombiner
	extends Reducer<Text, TextTuple, Text, TextTuple>
{
	@Override
	public void reduce(Text key, Iterable<TextTuple> values, Context context)
		throws IOException, InterruptedException
	{
		// Create one-use Iterator in order to get first element; since the lines servicing a station generally stay the same over time, only the first value is needed
		Iterator<TextTuple> val_iter = values.iterator();

		context.write(key, val_iter.next());
	}
}
