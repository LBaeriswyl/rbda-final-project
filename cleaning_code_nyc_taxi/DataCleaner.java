import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;


public class DataCleaner {
    public static void main(String[] args) throws Exception { 
        if (args.length != 3) {
            System.err.println("Usage: DataProfiler <input path> <output path> <taxi type>");
            System.exit(-1);
        }
        
        Job job = Job.getInstance();
        
        job.setJarByClass(DataCleaner.class); 
        job.setJobName("Data Cleaning");

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(args[2].equals("green")){
            job.setMapperClass(GreenDataCleaningMapper.class);
        }
        else if(args[2].equals("yellow")){
            job.setMapperClass(DataCleaningMapper.class);
        }
        else{
            System.err.println("Usage: DataProfiler <input path> <output path> <taxi type>");
            System.exit(-1);
        }
        // job.setMapperClass(DataCleaningMapper.class);
        // job.setReducerClass(WordCountReducer.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // job.setCombinerClass(WordCountReducer.class);
        job.setNumReduceTasks(0);
        
        boolean success = job.waitForCompletion(true); 
        if(success){
            Counters counters = job.getCounters();
            Counter invalid = counters.findCounter(RideData.INVALID);
            Counter missing = counters.findCounter(RideData.MISSING);
            Counter invalid_zone = counters.findCounter(RideData.INVALID_ZONE);
            Counter invalid_fare = counters.findCounter(RideData.INVALID_FARE);
            Counter invalid_datetime = counters.findCounter(RideData.INVALID_DATETIME);

            System.out.println("Invalid data: " + invalid.getValue());
            System.out.println("Missing data: " + missing.getValue());
            
            System.out.println("Invalid zone: " + invalid_zone.getValue());
            System.out.println("Invalid fare: " + invalid_fare.getValue());
            System.out.println("Invalid datetime: " + invalid_datetime.getValue());

            System.exit(0);
        }

    }    
}
