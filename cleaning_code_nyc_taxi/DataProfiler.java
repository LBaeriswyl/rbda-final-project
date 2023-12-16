import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataProfiler {
    public static void main(String[] args) throws Exception { 
        if (args.length != 3) {
            System.err.println("Usage: DataProfiler <input path> <output path> <task>");
            System.exit(-1);
        }
        
        Job job = Job.getInstance();
        
        job.setJarByClass(DataProfiler.class); 
        job.setJobName("Data Profiling");

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(args[2].equals("minmax")){
            job.setMapperClass(MinMaxMapper.class);
            job.setReducerClass(MinMaxReducer.class);
            job.setCombinerClass(MinMaxReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
        }
        else if(args[2].equals("average")){
            job.setMapperClass(AverageMapper.class);
            job.setReducerClass(AverageReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(FloatWritable.class);
        }
        else{
            System.err.println("Usage: DataProfiler <input path> <output path> <task>");
            System.exit(-1);
        }
 
        job.setNumReduceTasks(3);
        
        boolean success = job.waitForCompletion(true); 
        if(success){
            System.exit(0);
        }

    }    
}
