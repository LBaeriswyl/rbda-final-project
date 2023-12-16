import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

// Just a value class, so does not need to be comparable
public class VLongWritableTuple
	implements Writable
{
	private VLongWritable vlw1;
	private VLongWritable vlw2;

	public VLongWritableTuple()
	{
		set(new VLongWritable(), new VLongWritable());
	}

	public VLongWritableTuple(long l1, long l2)
	{
		set(new VLongWritable(l1), new VLongWritable(l2));
	}

	public VLongWritableTuple(VLongWritable vlw1, VLongWritable vlw2)
	{
		set(vlw1, vlw2);
	}

	public void set(VLongWritable vlw1, VLongWritable vlw2)
	{
		setVLW1(vlw1);
		setVLW2(vlw2);
	}

	public VLongWritable getVLW1()
	{
		return vlw1;
	}

	public VLongWritable getVLW2()
	{
		return vlw2;
	}

	public void setVLW1(long l1)
	{
		setVLW1(new VLongWritable(l1));
	}

	public void setVLW1(VLongWritable vlw1)
	{
		this.vlw1 = vlw1;
	}

	public void setVLW2(long l2)
	{
		setVLW2(new VLongWritable(l2));
	}

	public void setVLW2(VLongWritable vlw2)
	{
		this.vlw2 = vlw2;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		vlw1.write(out);
		vlw2.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		vlw1.readFields(in);
		vlw2.readFields(in);
	}

	@Override
	public int hashCode()
	{
		return vlw1.hashCode() * 0xFF + vlw2.hashCode();
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof VLongWritableTuple)
		{
			VLongWritableTuple vlwt = (VLongWritableTuple) o;
			return vlw1.equals(vlwt.vlw1) && vlw2.equals(vlwt.vlw2);
		}
		return false;
	}

	@Override
	public String toString()
	{
		return vlw1.toString() + "," + vlw2.toString();
	}
}
