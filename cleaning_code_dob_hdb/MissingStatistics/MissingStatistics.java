import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MissingStatistics {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MissingStatistics <input> <output>");
      System.exit(-1);
    }

    Job job = Job.getInstance();
    job.setJarByClass(MissingStatistics.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MissingStatisticsMapper.class);
    job.setNumReduceTasks(0); // Set number of reducers to zero

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}