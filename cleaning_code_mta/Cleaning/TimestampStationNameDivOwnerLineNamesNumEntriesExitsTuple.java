import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.format.DateTimeFormatter;	// Immutable and thread-safe

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

// Just a value class, so does not need to be comparable
public class TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple
	implements Writable
{
	private Text timestamp;			// Unix-format timestamp
	private Text station_name;
	private Text division_owner;
	private Text line_names;
	// Counters go up to 10^10 - 1 = 2^33.219..., so need more than 32/8 = 4 bytes to store values; VLongWritables take from 1 to 5 bytes, which is perfect for this use case
	private VLongWritable num_entries;
	private VLongWritable num_exits;

	// Counter rolls over at 10^10 - 1
	public static final long COUNTER_ROLLOVER_NUM = (long) 1E10;
	public static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple()
	{
		set(new Text(), new Text(), new Text(), new Text(), new VLongWritable(), new VLongWritable());
	}

	public TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple(String timestamp, String station_name, String division_owner, String line_names, long num_entries, long num_exits)
	{
		set(new Text(timestamp),
			new Text(station_name),
			new Text(division_owner),
			new Text(line_names),
			new VLongWritable(num_entries),
			new VLongWritable(num_exits)
			);
	}

	public TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple(Text timestamp, Text station_name, Text division_owner, Text line_names, VLongWritable num_entries, VLongWritable num_exits)
	{
		set(timestamp, station_name, division_owner, line_names, num_entries, num_exits);
	}

	public void set(Text timestamp, Text station_name, Text division_owner, Text line_names, VLongWritable num_entries, VLongWritable num_exits)
	{
		setTimestamp(timestamp);
		setStationName(station_name);
		setDivOwner(division_owner);
		setLineNames(line_names);
		setNumEntries(num_entries);
		setNumExits(num_exits);
	}

	public Text getTimestamp()
	{
		return timestamp;
	}

	public Text getStationName()
	{
		return station_name;
	}

	public Text getDivOwner()
	{
		return division_owner;
	}

	public Text getLineNames()
	{
		return line_names;
	}

	public VLongWritable getNumEntries()
	{
		return num_entries;
	}

	public VLongWritable getNumExits()
	{
		return num_exits;
	}

	public void setTimestamp(String timestamp)
	{
		setTimestamp(new Text(timestamp));
	}

	public void setTimestamp(Text timestamp)
	{
		this.timestamp = timestamp;
	}

	public void setStationName(String station_name)
	{
		setStationName(new Text(station_name));
	}

	public void setStationName(Text station_name)
	{
		this.station_name = station_name;
	}

	public void setDivOwner(String division_owner)
	{
		setDivOwner(new Text(division_owner));
	}

	public void setDivOwner(Text division_owner)
	{
		this.division_owner = division_owner;
	}

	public void setLineNames(String line_names)
	{
		setLineNames(new Text(line_names));
	}

	public void setLineNames(Text line_names)
	{
		this.line_names = line_names;
	}

	public void setNumEntries(long num_entries)
	{
		setNumEntries(new VLongWritable(num_entries));
	}

	public void setNumEntries(VLongWritable num_entries)
	{
		this.num_entries = num_entries;
	}

	public void setNumExits(long num_exits)
	{
		setNumExits(new VLongWritable(num_exits));
	}

	public void setNumExits(VLongWritable num_exits)
	{
		this.num_exits = num_exits;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		timestamp.write(out);
		station_name.write(out);
		division_owner.write(out);
		line_names.write(out);
		num_entries.write(out);
		num_exits.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		timestamp.readFields(in);
		station_name.readFields(in);
		division_owner.readFields(in);
		line_names.readFields(in);
		num_entries.readFields(in);
		num_exits.readFields(in);
	}

	@Override
	public int hashCode()
	{
		// Ensures that partitions are only determined by station_name and line_names so that all num_entriess for a given device at a given station will be in the same reducer partition
		return ((((timestamp.hashCode() * 0xFF + station_name.hashCode()) * 0xFF + division_owner.hashCode()) * 0xFF + line_names.hashCode()) * 0xFF + (int) num_entries.get()) * 0xFF + (int) num_exits.get();
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple)
		{
			TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple tsndolnneet = (TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple) o;
			return timestamp.equals(tsndolnneet.timestamp)
					&& station_name.equals(tsndolnneet.station_name)
					&& division_owner.equals(tsndolnneet.division_owner)
					&& line_names.equals(tsndolnneet.line_names)
					&& num_entries.equals(tsndolnneet.num_entries)
					&& num_exits.equals(tsndolnneet.num_exits);
		}
		return false;
	}

	@Override
	public String toString()
	{
		return timestamp + "," + station_name + "," + division_owner + "," + line_names + "," + num_entries + "," + num_exits;
	}
}
