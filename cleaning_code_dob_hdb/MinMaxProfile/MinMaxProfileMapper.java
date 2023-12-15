import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxProfileMapper extends Mapper<LongWritable,Text,Text,MinMaxTuple> {

  private Text outCategory = new Text();
  private MinMaxTuple outTuple = new MinMaxTuple();

  private int groupColumn;
  private int targetColumn;

  @Override
  protected void setup(Context context) {
    Configuration conf = context.getConfiguration();
    groupColumn = Integer.parseInt(conf.get("GroupColumn"));
    targetColumn = Integer.parseInt(conf.get("TargetColumn"));
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

    double columnValue = Double.parseDouble(columns[targetColumn]);

    outTuple.setMin(columnValue);
    outTuple.setMax(columnValue);

    if (groupColumn != -1) {
      outCategory.set(columns[groupColumn]);
    }
    else {
      outCategory.set("Overall");
    }

    context.write(outCategory, outTuple);
  }
}