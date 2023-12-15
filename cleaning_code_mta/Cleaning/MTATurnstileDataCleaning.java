import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MTATurnstileDataCleaning
{
	public enum Record
	{
		BAD_RECORD
	}

	public enum RemoteID
	{
		EMPTY
	}

	public enum DeviceAddress
	{
		EMPTY
	}

	public enum StationName
	{
		EMPTY
	}

	public enum DivOwner
	{
		EMPTY
	}

	public enum LineNames
	{
		EMPTY
	}

	public enum Date
	{
		NOT_A_DATE
	}

	public enum Time
	{
		AM_PM,
		NOT_A_TIME
	}

	public enum Timestamp
	{
		DUPLICATE,
		ADJ_REC_TIMEOUT_THRESHOLD_EXCEEDED
	}
	
	public enum PushType
	{
		RECOV_AUD
	}

	public enum NumEntries
	{
		DECREASING,
		NAN,
		VALUE_JUMP
	}

	public enum NumExits
	{
		DECREASING,
		NAN,
		VALUE_JUMP
	}

	public static void main(String[] argv)
		throws Exception
	{
		if (argv.length != 2)
		{
			System.err.println("Usage: MTATurnstileDataCleaning <input-path> <output-path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(MTATurnstileDataCleaning.class);
		job.setJobName("MTA turnstile data cleaning");

		FileInputFormat.addInputPath(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapperClass(MTATurnstileDataCleaningMapper.class);
		job.setReducerClass(MTATurnstileDataCleaningReducer.class);

		// Set output types for mapper
		job.setMapOutputKeyClass(RemoteIDDevAddrTimestampTuple.class);
		job.setMapOutputValueClass(TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple.class);

		// Set output types for overall job (if Mapper is specified separately for a particular output type, refers only to Reducer)
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// Set method by which the partition of given data is determined; is based on remote unit ID and device address alone
		job.setPartitionerClass(MTATurnstileDataCleaningPartitioner.class);
		// Set method by which reducer determines values to belong to the same key and concatenate such values into a list; group all entries with same remote unit ID and device address together, ignoring the timestamp part of the key
		job.setGroupingComparatorClass(MTATurnstileDataCleaningGroupingComparator.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
