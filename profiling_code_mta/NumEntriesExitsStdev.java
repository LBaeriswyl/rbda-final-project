import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NumEntriesExitsStdev		// Contains combiner
{
	public enum NumEntries
	{
		SENTINEL_VALUE
	}

	public enum NumExits
	{
		SENTINEL_VALUE
	}

	public static void main(String[] args)
		throws Exception
	{
		if (args.length != 4)
		{
			System.err.println("Usage: NumEntriesExitsStdev <input-path> <output-path> <avg-num-entries> <avg-num-exits>");
			System.exit(-1);
		}

		// Use Configuration object to pass variables to mapper and reducer
		Configuration conf = new Configuration();

		try
		{
			conf.setDouble("Average number of entries", Double.parseDouble(args[2]));
		}
		catch (NumberFormatException nfe)
		{
			System.err.println("Could not parse args[2] " + args[2] + " as a double");
			System.exit(1);
		}

		try
		{
			conf.setDouble("Average number of exits", Double.parseDouble(args[3]));
		}
		catch (NumberFormatException nfe)
		{
			System.err.println("Could not parse args[3] " + args[3] + " as a double");
			System.exit(1);
		}

		Job job = Job.getInstance(conf);
		job.setJarByClass(NumEntriesExitsStdev.class);
		job.setJobName("Average number of entries and exits");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(NumEntriesExitsStdevMapper.class);
		job.setCombinerClass(NumEntriesExitsStdevCombiner.class);
		job.setReducerClass(NumEntriesExitsStdevReducer.class);

		job.setMapOutputValueClass(DoubleVLongWritableTupleArrayWritable.class);

		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritableArrayWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
