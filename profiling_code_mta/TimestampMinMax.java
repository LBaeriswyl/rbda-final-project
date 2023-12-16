import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TimestampMinMax	// Contains combiner
{
	public static void main(String[] args)
		throws Exception
	{
		if (args.length != 2)
		{
			System.err.println("Usage: TimestampMinMax <input-path> <output-path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(TimestampMinMax.class);
		job.setJobName("Min and max timestamps");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TimestampMinMaxMapper.class);
		job.setCombinerClass(TimestampMinMaxReducer.class);
		job.setReducerClass(TimestampMinMaxReducer.class);

		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(TextTuple.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
