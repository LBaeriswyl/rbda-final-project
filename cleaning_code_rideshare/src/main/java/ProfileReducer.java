package com.example.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import java.util.Map.Entry;

public class ProfileReducer
        extends Reducer<Text, MapWritable, Text, Text> {
    private TreeMap<Long, Long> strValCounts = new TreeMap<Long, Long>();

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {

        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        float sum = 0;
        float median = 0;
        float std = 0;
        long total_count = 0;
        strValCounts.clear();

        for (MapWritable v : values) {
            for (Entry<Writable, Writable> entry : v.entrySet()) {
                long strVal = (long) Double.parseDouble(entry.getKey().toString());
                long count = ((LongWritable) entry.getValue()).get();
                total_count += count;
                sum += strVal * count;
                if (strVal < min)
                    min = strVal;
                if (strVal > max)
                    max = strVal;

                Long existVal = strValCounts.get(strVal);
                if (existVal == null)
                    strValCounts.put(strVal, count);
                else
                    strValCounts.put(strVal, existVal + count);
            }
        }

        long medianIndex = total_count / 2L;
        long prevCounts = 0;
        long curCounts = 0;
        long prevKey = 0;

        for (Entry<Long, Long> entry : strValCounts.entrySet()) {
            curCounts = prevCounts + entry.getValue();
            if (prevCounts <= medianIndex && medianIndex < curCounts) {
                if (curCounts % 2 == 0 && prevCounts == medianIndex)
                    median = (float) (entry.getKey() + prevKey) / 2.0f;
                else
                    median = entry.getKey();
                break;
            }
            prevCounts = curCounts;
            prevKey = entry.getKey();
        }

        float mean = sum / total_count;
        float sumOfSquares = 0.0f;
        for (Entry<Long, Long> entry : strValCounts.entrySet()) {
            sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean)
                    * entry.getValue();
        }
        std = (float) Math.sqrt(sumOfSquares / (total_count - 1));
        // convert all results to string and write to context
        context.write(key,
                new Text("\nmin: " + String.format("%.3f", min) + "\nmax: " + String.format("%.3f", max) + "\nmean: "
                        + String.format("%.3f", mean) + "\nmedian: " + String.format("%.3f", median) + "\nstd: "
                        + String.format("%.3f", std)));
    }
}
