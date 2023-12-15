import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxDateProfileReducer extends Reducer<Text,MinMaxDateTuple,Text,MinMaxDateTuple> {

  private MinMaxDateTuple result = new MinMaxDateTuple();

  @Override
  public void reduce(Text key, Iterable<MinMaxDateTuple> values, Context context) throws IOException, InterruptedException {

    result.setMin(null);
    result.setMax(null);

    for (MinMaxDateTuple val : values) {
      if (result.getMin() == null || val.getMin().isBefore(result.getMin())) {
        result.setMin(val.getMin());
      }
      if (result.getMax() == null || val.getMax().isAfter(result.getMax())) {
        result.setMax(val.getMax());
      }
    }
    context.write(key, result);
  }
}