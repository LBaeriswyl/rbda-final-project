import org.apache.hadoop.io.ArrayWritable;

public class DoubleVLongWritableTupleArrayWritable
	extends ArrayWritable
{
	public DoubleVLongWritableTupleArrayWritable()
	{
		super(DoubleVLongWritableTuple.class);
	}

	public DoubleVLongWritableTupleArrayWritable(DoubleVLongWritableTuple[] values)
	{
		super(DoubleVLongWritableTuple.class, values);
	}

	@Override
	public DoubleVLongWritableTuple[] toArray()
	{
		return (DoubleVLongWritableTuple[]) super.toArray();
	}

	@Override
	public String toString()
	{
		// For single-threaded string manipulations
		StringBuilder sb = new StringBuilder();
		
		sb.append("[");

		for (String s : super.toStrings())
			sb.append("\t").append(s);

		sb.append("\t]");
		return sb.toString();
	}
}
