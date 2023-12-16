import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TimestampMinMaxReducer
	extends Reducer<NullWritable, TextTuple, NullWritable, TextTuple>
{
	@Override
	public void reduce(NullWritable key, Iterable<TextTuple> values, Context context)
		throws IOException, InterruptedException
	{
		// Create one-use Iterator in order to get first element; does not remove the underlying element from the Iterable, so a for-each loop will still process all elements that were initially present
		Iterator<TextTuple> val_iter = values.iterator();
		TextTuple init_val = val_iter.next();
		String min_timestamp = init_val.getT1().toString();
		String max_timestamp = init_val.getT2().toString();

		while (val_iter.hasNext())
		{
			TextTuple value = val_iter.next();

			if (value.getT1().toString().compareTo(min_timestamp) < 0)
				min_timestamp = value.getT1().toString();
			if (value.getT2().toString().compareTo(max_timestamp) > 0)
				max_timestamp = value.getT2().toString();
		}

		context.write(NullWritable.get(), new TextTuple(min_timestamp, max_timestamp));
	}
}
