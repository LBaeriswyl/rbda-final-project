import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxDateProfileMapper extends Mapper<LongWritable,Text,Text,MinMaxDateTuple> {

  private Text outCategory = new Text();
  private MinMaxDateTuple outTuple = new MinMaxDateTuple();
  DateTimeFormatter dashes = DateTimeFormatter.ofPattern("yyyy-MM-dd");

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

    if (!columns[targetColumn].isEmpty()) {
      LocalDate columnValue = LocalDate.parse(columns[targetColumn], dashes);

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
}