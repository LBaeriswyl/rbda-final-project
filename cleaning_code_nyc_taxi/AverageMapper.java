import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AverageMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {

    private IntWritable columnKey = new IntWritable();
    private FloatWritable columnValue = new FloatWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(","); // Assuming comma-separated values

        int[] columnInds = {2,3,6};

        for (int i = 0; i < columnInds.length ; i++) {
            float columnData = Float.parseFloat(columns[columnInds[i]]);
            columnKey.set(columnInds[i]);
            columnValue.set(columnData);
            context.write(columnKey, columnValue);

        }
    }
}