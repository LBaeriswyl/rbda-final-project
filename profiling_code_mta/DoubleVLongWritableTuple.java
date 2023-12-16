import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

// Just a value class, so does not need to be comparable
public class DoubleVLongWritableTuple
	implements Writable
{
	private DoubleWritable dw;
	private VLongWritable vlw;

	public DoubleVLongWritableTuple()
	{
		set(new DoubleWritable(), new VLongWritable());
	}

	public DoubleVLongWritableTuple(double d, long l)
	{
		set(new DoubleWritable(d), new VLongWritable(l));
	}

	public DoubleVLongWritableTuple(DoubleWritable dw, VLongWritable vlw)
	{
		set(dw, vlw);
	}

	public void set(DoubleWritable dw, VLongWritable vlw)
	{
		setDW(dw);
		setVLW(vlw);
	}

	public DoubleWritable getDW()
	{
		return dw;
	}

	public VLongWritable getVLW()
	{
		return vlw;
	}

	public void setDW(double d)
	{
		setDW(new DoubleWritable(d));
	}

	public void setDW(DoubleWritable dw)
	{
		this.dw = dw;
	}

	public void setVLW(long l)
	{
		setVLW(new VLongWritable(l));
	}

	public void setVLW(VLongWritable vlw)
	{
		this.vlw = vlw;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		dw.write(out);
		vlw.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		dw.readFields(in);
		vlw.readFields(in);
	}

	@Override
	public int hashCode()
	{
		return dw.hashCode() * 0xFF + vlw.hashCode();
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof DoubleVLongWritableTuple)
		{
			DoubleVLongWritableTuple dvlwt = (DoubleVLongWritableTuple) o;
			return dw.equals(dvlwt.dw) && vlw.equals(dvlwt.vlw);
		}
		return false;
	}

	@Override
	public String toString()
	{
		return dw.toString() + "," + vlw.toString();
	}
}
