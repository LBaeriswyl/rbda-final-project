import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MinMaxMapper extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable columnInd = new IntWritable();
    private Text columnValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(","); // Assuming comma-separated values

        int[] columns_inds = {2, 3, 6};

        for(int i = 0; i < columns_inds.length; i++){
            float columnData = Float.parseFloat(columns[columns_inds[i]]); // Replace 0 with your column index
            
            columnInd.set(columns_inds[i]);
            columnValue.set(Float.toString(columnData)+","+Float.toString(columnData));

            context.write(columnInd, columnValue);
        }
    }
}
