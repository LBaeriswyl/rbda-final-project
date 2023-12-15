import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MTAGeoStationCleaning
{
	public enum Record
	{
		BAD_RECORD
	}

	public enum ComplexID
	{
		NAN
	}

	public enum DivOwner
	{
		EMPTY
	}

	public enum StationName
	{
		EMPTY
	}

	public enum LineNames
	{
		EMPTY
	}

	public enum GTFSLatitude
	{
		NAN
	}

	public enum GTFSLongitude
	{
		NAN
	}

	public static void main(String[] argv)
		throws Exception
	{
		if (argv.length != 2)
		{
			System.err.println("Usage: MTAGeoStationCleaning <input-path> <output-path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(MTAGeoStationCleaning.class);
		job.setJobName("MTA station geographic location cleaning");

		FileInputFormat.addInputPath(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapperClass(MTAGeoStationCleaningMapper.class);
		job.setReducerClass(MTAGeoStationCleaningReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);

		// Set output types for overall job (if Mapper is specified separately for a particular output type, refers only to Reducer)
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
