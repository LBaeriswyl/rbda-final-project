import org.apache.hadoop.io.ArrayWritable;

public class VLongWritableTupleArrayWritable
	extends ArrayWritable
{
	public VLongWritableTupleArrayWritable()
	{
		super(VLongWritableTuple.class);
	}

	public VLongWritableTupleArrayWritable(VLongWritableTuple[] values)
	{
		super(VLongWritableTuple.class, values);
	}

	@Override
	public VLongWritableTuple[] toArray()
	{
		return (VLongWritableTuple[]) super.toArray();
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
