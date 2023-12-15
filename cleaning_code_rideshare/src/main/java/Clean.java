package com.example.project;

import static java.lang.Thread.sleep;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.NullWritable;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

public class Clean {
    enum DataMeta {
        VALID,
        INVALID
    };

    enum EmptyDataDetailed {
        REQUEST_DATETIME, // YYYY-MM-DD HH:MM:SS
        PICKUP_DATETIME, // YYYY-MM-DD HH:MM:SS. PICKUP_DATETIME >= REQUEST_DATETIME
        DROPOFF_DATETIME, // YYYY-MM-DD HH:MM:SS. DROPOFF_DATETIES > PICKUP_DATETIME >= REQUEST_DATETIME
        PULOCATIONID, // an integer from 1 to 265
        DOLOCATIONID, // an integer from 1 to 265
        TRIP_MILES, // a positive double
        TRIP_TIME, // a positive integer
        BASE_PASSENGER_FARE, // a nonnegative double
        TOLLS, // a nonnegative double
        BCF, // a nonnegative double
        SALES_TAX, // a nonnegative double
        CONGESTION_SURCHARGE, // a nonnegative double
        TIPS, // a nonnegative double
    };

    enum InvalidDataDetailed {
        REQUEST_DATETIME, // YYYY-MM-DD HH:MM:SS
        PICKUP_DATETIME, // YYYY-MM-DD HH:MM:SS. PICKUP_DATETIME >= REQUEST_DATETIME
        DROPOFF_DATETIME, // YYYY-MM-DD HH:MM:SS. DROPOFF_DATETIES > PICKUP_DATETIME >= REQUEST_DATETIME
        PULOCATIONID, // an integer from 1 to 265
        DOLOCATIONID, // an integer from 1 to 265
        TRIP_MILES, // a positive double
        TRIP_TIME, // a positive integer
        BASE_PASSENGER_FARE, // a nonnegative double
        TOLLS, // a nonnegative double
        BCF, // a nonnegative double
        SALES_TAX, // a nonnegative double
        CONGESTION_SURCHARGE, // a nonnegative double
        TIPS, // a nonnegative double
        INVALID_LOCATION,
        INVALID_TIMEORDER,
    };

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Clean <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        String inputFile = args[0];
        String outputFile = args[1];
        String compression = (args.length > 2) ? args[2] : "none";

        Job job = Job.getInstance();
        job.setJarByClass(Clean.class);
        job.setJobName("Clean");

        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        job.setMapperClass(CleanMapper.class);
        job.setInputFormatClass(ExampleInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        Counters cn = job.getCounters();
        Counter valid = cn.findCounter(DataMeta.VALID);
        Counter invalid = cn.findCounter(DataMeta.INVALID);
        System.out.println("VALID " + valid.getValue());
        System.out.println("INVALID " + invalid.getValue());
        // print out the detailed invalid data
        for (InvalidDataDetailed invalidData : InvalidDataDetailed.values()) {
            Counter invalidDataCounter = cn.findCounter(invalidData);
            System.out.println(invalidData + " " + invalidDataCounter.getValue());
        }
        // print out the detailed empty data
        for (EmptyDataDetailed emptyData : EmptyDataDetailed.values()) {
            Counter emptyDataCounter = cn.findCounter(emptyData);
            System.out.println(emptyData + " " + emptyDataCounter.getValue());
        }

        boolean compleSucess = job.waitForCompletion(true);
        System.exit(compleSucess ? 0 : 1);

    }
}