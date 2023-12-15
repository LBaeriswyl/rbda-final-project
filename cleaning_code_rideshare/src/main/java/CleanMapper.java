package com.example.project;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.util.Map;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.util.TimeZone;
import java.time.Duration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;
import org.apache.hadoop.io.NullWritable;

public class CleanMapper extends Mapper<LongWritable, Group, NullWritable, Text> {
    private static HashSet<String> VALID_FIELDS = new HashSet<String>(
            Arrays.asList("request_datetime", "hvfhs_license_num",
                    "pickup_datetime", "dropoff_datetime",
                    "PULocationID", "DOLocationID", "trip_miles", "trip_time",
                    "congestion_surcharge", "tips"));
    private static HashSet<String> CHARGE_FIELDS = new HashSet<String>(
            Arrays.asList("base_passenger_fare", "tolls", "bcf", "sales_tax", "congestion_surcharge", "tips",
                    "airport_fee"));

    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Map<String, String> fieldMap = parseFields(line);
        String output = "";
        boolean isInvalid = false;
        Double total_charges = 0.0;
        String request_datetime = "";
        String pickup_datetime = "";
        String dropoff_datetime = "";

        // only keep the columns we are interested in and record the empty fields
        for (String field : fieldMap.keySet()) {
            if (!VALID_FIELDS.contains(field) && !CHARGE_FIELDS.contains(field))
                continue;

            if (fieldMap.get(field) != null && !fieldMap.get(field).isEmpty()) {
                String parseVal = parseField(field, fieldMap.get(field), context);
                if (parseVal.equals("-1")) {
                    isInvalid = true;
                } else {
                    if (CHARGE_FIELDS.contains(field)) {
                        total_charges += Double.parseDouble(parseVal);
                    }
                    if (field.equals("request_datetime")) {
                        request_datetime = parseVal;
                    }
                    if (field.equals("pickup_datetime")) {
                        pickup_datetime = parseVal;
                    }
                    if (field.equals("dropoff_datetime")) {
                        dropoff_datetime = parseVal;
                    }
                    if (field.equals("PULocationID") || field.equals("DOLocationID")) {
                        if (Integer.parseInt(parseVal) == 264 || Integer.parseInt(parseVal) == 265) {
                            isInvalid = true;
                            context.getCounter(Clean.InvalidDataDetailed.INVALID_LOCATION).increment(1);
                        }
                    }
                    output += field + ": " + parseVal + "\n";
                }
            } else {
                isInvalid = true;
                context.getCounter(Clean.EmptyDataDetailed.valueOf(field.toUpperCase())).increment(1);
            }
        }
        if (!checkTimeOrder(request_datetime, pickup_datetime, dropoff_datetime)) {
            isInvalid = true;
            context.getCounter(Clean.InvalidDataDetailed.INVALID_TIMEORDER).increment(1);
        }

        if (isInvalid) {
            context.getCounter(Clean.DataMeta.INVALID).increment(1);
            return;
        }
        output += "total_charges" + ": " + String.format("%.2f", total_charges) + "\n";
        try {
            output += "wait_time" + ": " + waitTimeCalcuate(request_datetime, pickup_datetime) + "\n";
        } catch (ParseException e) {
            context.getCounter(Clean.DataMeta.INVALID).increment(1);
            return;
        }
        context.getCounter(Clean.DataMeta.VALID).increment(1);
        context.write(NullWritable.get(), new Text(output));
    }

    private static boolean checkTimeOrder(String request_datetime, String pickup_datetime, String dropoff_datetime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
        try {
            Date request = sdf.parse(request_datetime);
            Date pickup = sdf.parse(pickup_datetime);
            Date dropoff = sdf.parse(dropoff_datetime);
            if (!request.after(pickup) && !dropoff.before(pickup)) {
                return true;
            }
        } catch (ParseException e) {
            return false;
        }
        return false;
    }

    private static String waitTimeCalcuate(String request_datetime, String pickup_datetime) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
        Date pickup = sdf.parse(pickup_datetime);
        Date request = sdf.parse(request_datetime);
        Long wait_time = (pickup.getTime() - request.getTime()) / 1000;

        return Long.toString(wait_time);
    }

    // write a data parser function that takes in field and value and return
    // verified and converted value
    private static String parseField(String field, String value, Context context) {
        try {
            switch (field) {
                case "request_datetime":
                    value = convertEpochTime(value);
                    break;
                case "pickup_datetime":
                    value = convertEpochTime(value);
                    break;
                case "dropoff_datetime":
                    value = convertEpochTime(value);
                    break;
                case "PULocationID":
                    int PULocationID = Integer.parseInt(value);
                    if (PULocationID < 1 || PULocationID > 265) {
                        throw new Exception();
                    }
                    break;
                case "DOLocationID":
                    int DOLocationID = Integer.parseInt(value);
                    if (DOLocationID < 1 || DOLocationID > 265) {
                        throw new Exception();
                    }
                    break;
                case "trip_miles":
                    double trip_miles = Double.parseDouble(value);
                    if (trip_miles <= 0) {
                        throw new Exception();
                    }
                    break;
                case "trip_time":
                    int trip_time = Integer.parseInt(value);
                    if (trip_time <= 0) {
                        throw new Exception();
                    }
                    break;
                case "base_passenger_fare":
                    double base_passenger_fare = Double.parseDouble(value);
                    if (base_passenger_fare < 0) {
                        throw new Exception();
                    }
                    break;
                case "tolls":
                    double tolls = Double.parseDouble(value);
                    if (tolls < 0) {
                        throw new Exception();
                    }
                    break;
                case "bcf":
                    double bcf = Double.parseDouble(value);
                    if (bcf < 0) {
                        throw new Exception();
                    }
                    break;
                case "sales_tax":
                    double sales_tax = Double.parseDouble(value);
                    if (sales_tax < 0) {
                        throw new Exception();
                    }
                    break;
                case "congestion_surcharge":
                    double congestion_surcharge = Double.parseDouble(value);
                    if (congestion_surcharge < 0) {
                        throw new Exception();
                    }
                    break;
                case "tips":
                    double tips = Double.parseDouble(value);
                    if (tips < 0) {
                        throw new Exception();
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            context.getCounter(Clean.InvalidDataDetailed.valueOf(field.toUpperCase())).increment(1);
            return "-1";
        }
        return value;
    }

    private static String convertEpochTime(String epochTime) {
        long unix_seconds = Long.parseLong(epochTime) / 1000000;
        Date date = new Date(unix_seconds * 1000);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
        return sdf.format(date);
    }

    private static Map<String, String> parseFields(String input) {
        Map<String, String> fieldMap = new HashMap<String, String>();
        String[] lines = input.split("\n");

        for (String line : lines) {
            String[] parts = line.split(": ");
            if (parts.length == 2) {
                fieldMap.put(parts[0], parts[1]);
            }
        }

        return fieldMap;
    }

}