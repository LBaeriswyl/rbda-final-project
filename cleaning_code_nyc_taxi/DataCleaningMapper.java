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

enum RideData{
    INVALID,
    MISSING,
    INVALID_ZONE,
    INVALID_DATETIME,
    INVALID_FARE
}

public class DataCleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text > {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String dataString = value.toString();
        String[] data = dataString.split(",");

        String pickup_datetime_str = data[1];
        String dropoff_datetime_str = data[2];

        DateTimeFormatter original_format = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a");
        DateTimeFormatter new_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


        if(data[7].matches(".*[a-zA-Z]+.*") || data[8].matches(".*[a-zA-Z]+.*")){
            return;
        }

        Integer pickup_zone = Integer.parseInt(data[7]);
        Integer dropoff_zone = Integer.parseInt(data[8]);
        
        

        // Check if the data is valid
        if(data.length < 17 || data.length > 18){
            context.getCounter(RideData.MISSING).increment(1);
            return;
        }

        else if(data[3].length() < 1 || data[4].length() < 1 || data[7].length() < 1 || data[8].length() < 1 || data[16].length() < 1 || data[13].length() < 1){
                context.getCounter(RideData.MISSING).increment(1);
                return;
        }


        // new york taxi zones
       else if(pickup_zone < 1 || pickup_zone > 263 || dropoff_zone < 1 || dropoff_zone > 263){
            context.getCounter(RideData.INVALID_ZONE).increment(1);
            context.getCounter(RideData.INVALID).increment(1);
            return;
        }

        else if(data[16].matches(".*[a-zA-Z]+.*") || data[13].matches(".*[a-zA-Z]+.*") || (data.length > 17 && data[17].matches(".*[a-zA-Z]+.*"))){
            context.getCounter(RideData.INVALID_FARE).increment(1);
            context.getCounter(RideData.INVALID).increment(1);
            return;
        }

        else {

            if(Float.parseFloat(data[4]) < 0 || Float.parseFloat(data[4]) > 100){
                context.getCounter(RideData.INVALID).increment(1);
                return;
            }

            if(Float.parseFloat(data[16]) < 0 || Float.parseFloat(data[13]) < 0 || (data.length > 17 && Float.parseFloat(data[17]) < 0) || Float.parseFloat(data[16]) > 1000 || Float.parseFloat(data[13]) > 1000 || (data.length > 17 && Float.parseFloat(data[17]) > 1000)){
                context.getCounter(RideData.INVALID).increment(1);
                context.getCounter(RideData.INVALID_FARE).increment(1);
                return;
            }

            if(Float.parseFloat(data[3]) > 8 || Float.parseFloat(data[3]) < 1){
                context.getCounter(RideData.INVALID).increment(1);
                return;
            }

            try {
                LocalDateTime pickup_datetime = LocalDateTime.parse(pickup_datetime_str, original_format);
                LocalDateTime dropoff_datetime = LocalDateTime.parse(dropoff_datetime_str, original_format);
                
                String formatted_pickup_datetime = pickup_datetime.format(new_format);
                String formatted_dropoff_datetime = dropoff_datetime.format(new_format);

                String total_charge = Float.toString(Float.parseFloat(data[16])-Float.parseFloat(data[13]));
                String congestion_surcharge = data.length > 17 ? data[17] : "0";

                if(Float.parseFloat(total_charge) < 0 || Float.parseFloat(congestion_surcharge) < 0){
                    context.getCounter(RideData.INVALID).increment(1);
                    return;
                }

                String filtered_data = String.join(",", formatted_pickup_datetime, formatted_dropoff_datetime, data[3], data[4], data[7], data[8], total_charge, congestion_surcharge);
                context.write(NullWritable.get(), new Text(filtered_data));
        
            } catch (Exception e) {
                context.getCounter(RideData.INVALID_DATETIME).increment(1);
                context.getCounter(RideData.INVALID).increment(1);
                return;
            }
            
        }





    }
}