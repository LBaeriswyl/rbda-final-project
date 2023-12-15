package com.example.project;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.util.Map;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfileMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private static HashSet<String> FIELDS = new HashSet<String>(
            Arrays.asList("trip_miles", "trip_time", "total_charges", "wait_time"));
    private static final LongWritable ONE = new LongWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Map<String, String> fieldMap = parseFields(line);
        for (String field : fieldMap.keySet()) {
            if (FIELDS.contains(field)) {
                MapWritable outCommentLength = new MapWritable();
                outCommentLength.put(new Text(String.format("%.3f", Double.parseDouble(fieldMap.get(field)))), ONE);
                // Write out the user ID with min max dates and // count
                context.write(new Text(field), outCommentLength);
            }
        }
    }

    private static Map<String, String> parseFields(String input) {
        Map<String, String> fieldMap = new HashMap<String, String>();
        String[] lines = input.split("\n");

        for (String line : lines) {
            String[] parts = line.split(": ");
            if (parts.length == 2) {
                fieldMap.put(parts[0], parts[1]);
            }
        }

        return fieldMap;
    }

}