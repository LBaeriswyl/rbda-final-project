import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxDateProfile {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 4) {
      System.err.println("Usage: MinMaxDateProfile <input path> <output path> <target column number> <group column number>");
      System.exit(-1);
    }
    conf.set("TargetColumn", args[2]);
    conf.set("GroupColumn", args[3]);

    Job job = Job.getInstance(conf, "MinMaxDateProfile");
    job.setJarByClass(MinMaxDateProfile.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MinMaxDateProfileMapper.class);
    job.setCombinerClass(MinMaxDateProfileReducer.class);
    job.setReducerClass(MinMaxDateProfileReducer.class);

    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MinMaxDateTuple.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}