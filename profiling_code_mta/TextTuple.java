import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// Just a value class, so does not need to be comparable
public class TextTuple
	implements Writable
{
	private Text t1;
	private Text t2;

	public TextTuple()
	{
		set(new Text(), new Text());
	}

	public TextTuple(String s1, String s2)
	{
		set(new Text(s1), new Text(s2));
	}

	public TextTuple(Text t1, Text t2)
	{
		set(t1, t2);
	}

	public void set(Text t1, Text t2)
	{
		setT1(t1);
		setT2(t2);
	}

	public Text getT1()
	{
		return t1;
	}

	public Text getT2()
	{
		return t2;
	}

	public void setT1(String s1)
	{
		setT1(new Text(s1));
	}

	public void setT1(Text t1)
	{
		this.t1 = t1;
	}

	public void setT2(String s2)
	{
		setT2(new Text(s2));
	}

	public void setT2(Text t2)
	{
		this.t2 = t2;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		t1.write(out);
		t2.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		t1.readFields(in);
		t2.readFields(in);
	}

	@Override
	public int hashCode()
	{
		return t1.hashCode() * 0xFF + t2.hashCode();
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof TextTuple)
		{
			TextTuple tt = (TextTuple) o;
			return t1.equals(tt.t1) && t2.equals(tt.t2);
		}
		return false;
	}

	@Override
	public String toString()
	{
		return t1.toString() + "," + t2.toString();
	}
}
