import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import javax.naming.Context;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class GreenDataCleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text > {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String dataString = value.toString();
        String[] data = dataString.split(",");

        String pickup_datetime_str = data[1];
        String dropoff_datetime_str = data[2];

        DateTimeFormatter original_format = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a");
        DateTimeFormatter new_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


        if(data[5].matches(".*[a-zA-Z]+.*") || data[6].matches(".*[a-zA-Z]+.*")){
            return;
        }

        Integer pickup_zone = Integer.parseInt(data[5]);
        Integer dropoff_zone = Integer.parseInt(data[6]);
        

        // Check if the data is valid
        if(data.length < 19 || data.length > 20){
            context.getCounter(RideData.MISSING).increment(1);
        }

        // new york taxi zones
       else if(pickup_zone < 1 || pickup_zone > 263 || dropoff_zone < 1 || dropoff_zone > 263){
            context.getCounter(RideData.INVALID_ZONE).increment(1);
            context.getCounter(RideData.INVALID).increment(1);
        }

        else if(data[16].matches(".*[a-zA-Z]+.*") || data[12].matches(".*[a-zA-Z]+.*") || (data.length > 19 && data[19].matches(".*[a-zA-Z]+.*"))){
            context.getCounter(RideData.INVALID_FARE).increment(1);
            context.getCounter(RideData.INVALID).increment(1);
        }

        else {

            if(Float.parseFloat(data[8]) < 0 || Float.parseFloat(data[8]) > 100){
                context.getCounter(RideData.INVALID).increment(1);
                return;
            }

            if(Float.parseFloat(data[16]) < 0 || Float.parseFloat(data[12]) < 0 || (data.length > 19 && Float.parseFloat(data[19]) < 0) || Float.parseFloat(data[16]) > 1000 || Float.parseFloat(data[12]) > 1000 || (data.length > 19 && Float.parseFloat(data[19]) > 1000)){
                context.getCounter(RideData.INVALID).increment(1);
                context.getCounter(RideData.INVALID_FARE).increment(1);
                return;
            }

            if(Float.parseFloat(data[7]) > 8 || Float.parseFloat(data[7]) < 1){
                context.getCounter(RideData.INVALID).increment(1);
                return;
            }



            try {
                LocalDateTime pickup_datetime = LocalDateTime.parse(pickup_datetime_str, original_format);
                LocalDateTime dropoff_datetime = LocalDateTime.parse(dropoff_datetime_str, original_format);
                
                String formatted_pickup_datetime = pickup_datetime.format(new_format);
                String formatted_dropoff_datetime = dropoff_datetime.format(new_format);

                String total_charge = Float.toString(Float.parseFloat(data[16])-Float.parseFloat(data[12]));
                String congestion_surcharge = data.length > 19 ? data[19] : "0";

                String filtered_data = String.join(",", formatted_pickup_datetime, formatted_dropoff_datetime, data[7], data[8], data[5], data[6], total_charge, congestion_surcharge);
                context.write(NullWritable.get(), new Text(filtered_data));
        
            } catch (Exception e) {
                context.getCounter(RideData.INVALID_DATETIME).increment(1);
                context.getCounter(RideData.INVALID).increment(1);
            }
            
        }





    }
}
