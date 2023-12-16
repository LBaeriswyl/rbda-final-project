import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MinMaxReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;

        for (Text value : values) {
            float currentMin = Float.parseFloat(value.toString().split(",")[0]);
            float currentMax = Float.parseFloat(value.toString().split(",")[1]);

            if (currentMax > max) {
                max = currentMax;
            }
            
            if (currentMin < min) {
                min = currentMin;
            }
        }

        context.write(key, new Text(Float.toString(min)+","+Float.toString(max)));
    }
}
