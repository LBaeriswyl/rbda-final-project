import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindMalformedDatesMapper extends Mapper<LongWritable, Text, Text, Text> {

  //Define all columns we want to keep
  private static final int[] validColumns = {1,2,3,4,5,6,7,9,10,11,12,13,14,17,18,19,20,21,22,25,26,27,28,55,56,57,58,59,60};

  DateTimeFormatter slashes = DateTimeFormatter.ofPattern("MM/dd/yyyy");
  DateTimeFormatter dashes = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  //Define columns that are in a special category
  private static final Set<Integer> datesSet = new HashSet<>(Arrays.asList(25,26,27,28));

  enum LINE_COUNTER {
    Invalid,
    Valid
  }

  public String dateParse(String inputDateString) {
    try {
      LocalDate date = LocalDate.parse(inputDateString, slashes);
      return date.format(dashes);
    } catch (DateTimeParseException e1) {
      try {
        LocalDate.parse(inputDateString, dashes);
        return inputDateString;
      } catch (DateTimeParseException e2) {
        return null;
      }
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

    List<String> outputColumns = new ArrayList<>();

    boolean valid = true;

    if(columns[0].equals("BOROUGH")) {
      context.getCounter("HeaderCounter", "DiscardedHeader").increment(1);
      //do nothing
    } else {
      String dateString = columns[24];
      if (!dateString.isEmpty()) {
        String datesValue = dateParse(dateString);
        if (datesValue == null) {
          context.write(new Text(dateString), null);
          context.getCounter(LINE_COUNTER.Invalid).increment(1);
        }
      }
    }
  }
}