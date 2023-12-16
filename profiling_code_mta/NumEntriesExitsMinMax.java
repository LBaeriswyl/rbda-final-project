import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NumEntriesExitsMinMax	// Contains combiner
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
		if (args.length != 2)
		{
			System.err.println("Usage: NumEntriesExitsMinMax <input-path> <output-path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(NumEntriesExitsMinMax.class);
		job.setJobName("Min and max number of entries and exits");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(NumEntriesExitsMinMaxMapper.class);
		job.setCombinerClass(NumEntriesExitsMinMaxReducer.class);
		job.setReducerClass(NumEntriesExitsMinMaxReducer.class);

		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VLongWritableTupleArrayWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
