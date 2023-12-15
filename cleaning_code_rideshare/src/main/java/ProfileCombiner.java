package com.example.project;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import java.util.Map.Entry;

public class ProfileCombiner extends
        Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        MapWritable outValue = new MapWritable();
        for (MapWritable v : values) {
            for (Entry<Writable, Writable> entry : v.entrySet()) {
                LongWritable count = (LongWritable) outValue.get(entry.getKey());
                if (count != null)
                    count.set(count.get() + ((LongWritable) entry.getValue()).get());
                else
                    outValue.put(entry.getKey(), new LongWritable(((LongWritable) entry.getValue()).get()));
            }
        }
        context.write(key, outValue);
    }
}