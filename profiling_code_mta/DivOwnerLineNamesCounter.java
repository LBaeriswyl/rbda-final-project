import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DivOwnerLineNamesCounter
{
	public static void main(String[] argv)
		throws Exception
	{
		if (argv.length != 2)
		{
			System.err.println("Usage: DivOwnerLineNamesCounter <input-path> <output-path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(DivOwnerLineNamesCounter.class);
		job.setJobName("Division owner and line names counter");

		FileInputFormat.addInputPath(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapperClass(DivOwnerLineNamesCounterMapper.class);
		job.setCombinerClass(DivOwnerLineNamesCounterCombiner.class);
		job.setReducerClass(DivOwnerLineNamesCounterReducer.class);

		// Set output types for mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextTuple.class);

		// Set output types for overall job (if Mapper is specified separately for a particular output type, refers only to Reducer)
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
