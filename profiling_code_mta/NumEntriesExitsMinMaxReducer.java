import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NumEntriesExitsMinMaxReducer
	extends Reducer<NullWritable, VLongWritableTupleArrayWritable, NullWritable, VLongWritableTupleArrayWritable>
{
	@Override
	public void reduce(NullWritable key, Iterable<VLongWritableTupleArrayWritable> values, Context context)
		throws IOException, InterruptedException
	{
		// Create one-use Iterator in order to get first element
		Iterator<VLongWritableTupleArrayWritable> val_iter = values.iterator();
		VLongWritableTupleArrayWritable init_val = val_iter.next();
		VLongWritableTuple[] init_array = init_val.toArray();

		while (val_iter.hasNext())
		{
			VLongWritableTupleArrayWritable value = val_iter.next();

			VLongWritableTuple[] curr_array = value.toArray();
			// Both arrays are the same length, so no need to worry about exceeding array boundaries
			for (int i = 0; i < curr_array.length; i++)
			{
				// Compare minimum values
				if (curr_array[i].getVLW1().get() < init_array[i].getVLW1().get())
					init_array[i].setVLW1(curr_array[i].getVLW1());
				// Compare maximum values
				if (curr_array[i].getVLW2().get() > init_array[i].getVLW2().get())
					init_array[i].setVLW2(curr_array[i].getVLW2());
			}
		}

		context.write(NullWritable.get(), new VLongWritableTupleArrayWritable(init_array));
	}
}
