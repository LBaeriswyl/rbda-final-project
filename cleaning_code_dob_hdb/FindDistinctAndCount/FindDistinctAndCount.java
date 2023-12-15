import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindDistinctAndCount {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 3) {
      System.err.println("Usage: FindDistinctAndCount <input path> <output path> <column number>");
      System.exit(-1);
    }
    conf.set("TargetColumn", args[2]);
    Job job = Job.getInstance(conf, "FindDistinctAndCount");
    job.setJarByClass(FindDistinctAndCount.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(FindDistinctAndCountMapper.class);
    job.setCombinerClass(FindDistinctAndCountReducer.class);
    job.setReducerClass(FindDistinctAndCountReducer.class);

    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}