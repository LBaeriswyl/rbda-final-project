package com.example.project;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profile {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Profile <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Profile.class);
        job.setJobName("Profile");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ProfileMapper.class);
        job.setCombinerClass(ProfileCombiner.class);
        job.setReducerClass(ProfileReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
