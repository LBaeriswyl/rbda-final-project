import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class RemoteIDDevAddrTimestampTuple
	implements WritableComparable<RemoteIDDevAddrTimestampTuple>
{
	private Text remote_id;
	private Text device_address;	// Original column: scp: subunit, channel, position
	private Text timestamp;			// Unix-format timestamp

	public RemoteIDDevAddrTimestampTuple()
	{
		set(new Text(), new Text(), new Text());
	}

	public RemoteIDDevAddrTimestampTuple(String remote_id, String device_address, String timestamp)
	{
		set(new Text(remote_id), new Text(device_address), new Text(timestamp));
	}

	public RemoteIDDevAddrTimestampTuple(Text remote_id, Text device_address, Text timestamp)
	{
		set(remote_id, device_address, timestamp);
	}

	public void set(Text remote_id, Text device_address, Text timestamp)
	{
		setRemoteID(remote_id);
		setDevAddr(device_address);
		setTimestamp(timestamp);
	}

	public Text getRemoteID()
	{
		return remote_id;
	}

	public Text getDevAddr()
	{
		return device_address;
	}

	public Text getTimestamp()
	{
		return timestamp;
	}
	
	public void setRemoteID(String remote_id)
	{
		setRemoteID(new Text(remote_id));
	}

	public void setRemoteID(Text remote_id)
	{
		this.remote_id = remote_id;
	}

	public void setDevAddr(String device_address)
	{
		setDevAddr(new Text(device_address));
	}

	public void setDevAddr(Text device_address)
	{
		this.device_address = device_address;
	}

	public void setTimestamp(String timestamp)
	{
		setTimestamp(new Text(timestamp));
	}

	public void setTimestamp(Text timestamp)
	{
		this.timestamp = timestamp;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		remote_id.write(out);
		device_address.write(out);
		timestamp.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		remote_id.readFields(in);
		device_address.readFields(in);
		timestamp.readFields(in);
	}

	@Override
	public int hashCode()
	{
		// Including all fields in hash code ensures that a good hash is produced in general for this object
		return (remote_id.hashCode() * 0xFF + device_address.hashCode()) * 0xFF + timestamp.hashCode();
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof RemoteIDDevAddrTimestampTuple)
		{
			RemoteIDDevAddrTimestampTuple sidatt = (RemoteIDDevAddrTimestampTuple) o;
			return remote_id.equals(sidatt.remote_id)
					&& device_address.equals(sidatt.device_address)
					&& timestamp.equals(sidatt.timestamp);
		}
		return false;
	}

	@Override
	public String toString()
	{
		return remote_id + "," + device_address + "," + timestamp;
	}

	// Because MapReduce guarantees that reducer inputs are sorted, and chronological ordering is necessary for calculation of net entries and exits, involve timestamp in sorting comparisons
	@Override
	public int compareTo(RemoteIDDevAddrTimestampTuple sidatt)
	{
		int cmp = remote_id.compareTo(sidatt.remote_id);
		if (cmp == 0)
		{
			cmp = device_address.compareTo(sidatt.device_address);
			if (cmp == 0)
				cmp = timestamp.compareTo(sidatt.timestamp);
		}
		return cmp;
	}
}
