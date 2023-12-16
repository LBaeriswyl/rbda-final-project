import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AverageReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

    private FloatWritable result = new FloatWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }

        double average = count > 0 ? (sum / count) : 0;
        result.set((float)average);
        context.write(key, result);
    }
}
