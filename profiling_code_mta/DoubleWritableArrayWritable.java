import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleWritableArrayWritable
	extends ArrayWritable
{
	public DoubleWritableArrayWritable()
	{
		super(DoubleWritable.class);
	}

	public DoubleWritableArrayWritable(DoubleWritable[] values)
	{
		super(DoubleWritable.class, values);
	}

	@Override
	public DoubleWritable[] toArray()
	{
		return (DoubleWritable[]) super.toArray();
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
