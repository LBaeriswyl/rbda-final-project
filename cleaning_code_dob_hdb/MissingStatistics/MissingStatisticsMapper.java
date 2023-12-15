import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class MissingStatisticsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    
    //this slows the MR down by a lot
    String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

    context.getCounter("TotalRows", "All").increment(1);

    for (int i = 0; i < 60; i++) {
      if (i >= columns.length || columns[i].isEmpty()) {
        context.getCounter("MissingValues", "Column" + (i+1)).increment(1);
      }
    }
    if (columns.length > 60) {
      context.getCounter("MissingValues", "MalformedRow" + columns.length).increment(1);
    }
  }
}
