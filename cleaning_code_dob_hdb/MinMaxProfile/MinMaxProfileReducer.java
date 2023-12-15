import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxProfileReducer extends Reducer<Text,MinMaxTuple,Text,MinMaxTuple> {

  private MinMaxTuple result = new MinMaxTuple();

  @Override
  public void reduce(Text key, Iterable<MinMaxTuple> values, Context context) throws IOException, InterruptedException {

    result.setMin(Double.MAX_VALUE);
    result.setMax(Double.MIN_VALUE);

    for (MinMaxTuple val : values) {
      if (result.getMin() == Double.MAX_VALUE || val.getMin() < result.getMin()) {
        result.setMin(val.getMin());
      }
      if (result.getMax() == Double.MIN_VALUE || val.getMax() > result.getMax()) {
        result.setMax(val.getMax());
      }
    }
    context.write(key, result);
  }
}