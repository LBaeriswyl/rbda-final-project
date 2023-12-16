import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NumEntriesExitsStdevCombiner
	extends Reducer<NullWritable, DoubleVLongWritableTupleArrayWritable, NullWritable, DoubleVLongWritableTupleArrayWritable>
{
	@Override
	public void reduce(NullWritable key, Iterable<DoubleVLongWritableTupleArrayWritable> values, Context context)
		throws IOException, InterruptedException
	{
		// Create one-use Iterator in order to get first element
		Iterator<DoubleVLongWritableTupleArrayWritable> val_iter = values.iterator();
		DoubleVLongWritableTupleArrayWritable init_val = val_iter.next();
		DoubleVLongWritableTuple[] init_array = init_val.toArray();

		while (val_iter.hasNext())
		{
			DoubleVLongWritableTupleArrayWritable value = val_iter.next();

			DoubleVLongWritableTuple[] curr_array = value.toArray();
			// Both arrays are the same length, so no need to worry about exceeding array boundaries
			for (int i = 0; i < curr_array.length; i++)
			{
				// Add together the sum field
				init_array[i].setDW(curr_array[i].getDW().get() + init_array[i].getDW().get());
				// Add together the counts field
				init_array[i].setVLW(curr_array[i].getVLW().get() + init_array[i].getVLW().get());
			}
		}

		context.write(NullWritable.get(), new DoubleVLongWritableTupleArrayWritable(init_array));
	}
}
