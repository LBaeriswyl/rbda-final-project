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

public class ColumnFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

  //Define all columns we want to keep
  private static final int[] validColumns = {1,2,3,4,5,6,7,9,10,11,12,13,14,17,18,19,20,21,22,25,26,27,28,55,56,57,58,59,60};


  //Define columns that are in a special category
  private static final Set<Integer> allowEmptyColumnsSet = new HashSet<>(Arrays.asList(13,14,17,22,26,27));
  private static final Set<Integer> datesSet = new HashSet<>(Arrays.asList(25,26,27,28));
  private static final Map<Integer, String> defaultValuesMap = new HashMap<>();


  //Define zip code column and min and max Zip Codes
  private static final int zipCodeColumn = 12;
  private static final int minZipCode = 10001;
  private static final int maxZipCode = 12000;

  //Define date formates and min and max Dates
  //we want to allow potential future job start dates or expiration dates
  DateTimeFormatter slashes = DateTimeFormatter.ofPattern("MM/dd/yyyy");
  DateTimeFormatter dashes = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  LocalDate minDate = LocalDate.of(1950,1,1);
  LocalDate maxDate = LocalDate.of(2030,1,1); 


  static {
    defaultValuesMap.put(13, "OTHER");
    defaultValuesMap.put(14, "NO");
  }

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

  public boolean validDate(String dateString) {
    LocalDate date = LocalDate.parse(dateString, dashes);
    if (date.isBefore(minDate) || date.isAfter(maxDate)) {
      return false;
    }

    return true;
  }

  //jobStartDate is not included because a permit may be filed for a job that has already started (renewals etc)
  public boolean validDateOrder(String filingDateString, String issuanceDateString, String expirationDateString) {
    LocalDate filingDate = filingDateString.isEmpty() ? null : LocalDate.parse(filingDateString, dashes);
    LocalDate issuanceDate = issuanceDateString.isEmpty() ? null : LocalDate.parse(issuanceDateString, dashes);
    LocalDate expirationDate = expirationDateString.isEmpty() ? null : LocalDate.parse(expirationDateString, dashes);

    if ((issuanceDate != null && (issuanceDate.isBefore(filingDate)))) {
      return false;
    } else if (expirationDate != null && (expirationDate.isBefore(filingDate) || (issuanceDate != null && expirationDate.isBefore(issuanceDate)))) {
      return false;
    }

    return true;
  }

  public boolean validZipCode(int zipCode, String boroughString) {
    if (boroughString.equals("MANHATTAN") && (zipCode < 10001 || zipCode > 10282)) {
      return false;
    } else if (boroughString.equals("BROOKLYN") && (zipCode < 11201 || zipCode > 11256)) {
      return false;
    } else if (boroughString.equals("QUEENS") && (zipCode < 11004 || zipCode > 11697)) {
      return false;
    } else if (boroughString.equals("BRONX") && (zipCode < 10451 || zipCode > 10475)) {
      return false;
    } else if (boroughString.equals("STATEN ISLAND") && (zipCode < 10301 || zipCode > 10314)) {
      return false;
    }

    return true;
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

    List<String> outputColumns = new ArrayList<>();

    boolean valid = true;

    if(columns[0].equals("BOROUGH")) {
      context.getCounter("HeaderCounter", "DiscardedHeader").increment(1);
      valid = false;
    } else {
      for (int i : validColumns) {
        int index = i-1;
        if (index >= columns.length || (columns[index].isEmpty() && !allowEmptyColumnsSet.contains(i))) {
          context.getCounter("MissingDiscardCounter", "MissingValue" + i).increment(1);
          valid = false;
          break;
        }

        String columnValue = columns[index].isEmpty() && defaultValuesMap.containsKey(i) ? defaultValuesMap.get(i) : columns[index];

        if (datesSet.contains(i) && !columnValue.isEmpty()) {
          columnValue = dateParse(columnValue.trim());
          if (columnValue == null) {
            context.getCounter("MalformedDiscardCounter", "MalformedDate" + i).increment(1);
            valid = false;
            break;
          }
        }

        if (i == zipCodeColumn && !columnValue.isEmpty() && !validZipCode(Integer.parseInt(columnValue), columns[0])) {
          context.getCounter("InvalidValueDiscardCounter", "InvalidZipCode").increment(1);
          valid = false;
          break;
        }

        if (datesSet.contains(i) && !columnValue.isEmpty() && !validDate(columnValue)) {
          context.getCounter("InvalidValueDiscardCounter", "InvalidDate").increment(1);
          valid = false;
          break;
        }

        outputColumns.add(columnValue);
      }
    }

    if (valid && !validDateOrder(outputColumns.get(19), outputColumns.get(20), outputColumns.get(21))) {
      context.getCounter("InvalidDateOrderCounter", "InvalidDateOrder").increment(1);
      valid = false;
    }

    if (valid) {
      String outputLine = String.join(",", outputColumns);
      context.write(new Text(outputLine), null);
      context.getCounter(LINE_COUNTER.Valid).increment(1);
    }
    else {
      context.getCounter(LINE_COUNTER.Invalid).increment(1);
    }
  }
}