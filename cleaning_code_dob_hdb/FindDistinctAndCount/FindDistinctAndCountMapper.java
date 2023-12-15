import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FindDistinctAndCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private int targetColumn;

  @Override
  protected void setup(Context context) {
    Configuration conf = context.getConfiguration();
    targetColumn = Integer.parseInt(conf.get("TargetColumn"));
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");


    if (targetColumn < columns.length) {
      String category = columns[targetColumn];
      context.write(new Text(category), new IntWritable(1));
    }
  }
}