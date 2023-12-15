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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ColumnFilterTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

  //Define all columns we want to remove
  private static final Set<Integer> removeColumns = new HashSet<>(Arrays.asList(24,30,31,32,33,38,49,50,51,52,53,54,55,56,57,58));


  //Define columns that are in a special category
  private static final Set<Integer> boolColumn = new HashSet<>(Arrays.asList(3,4,34));
  private static final Set<Integer> allowEmptyColumnsSet = new HashSet<>(Arrays.asList(3,4,6,7,8,9,10,11,12,13,14,15,21,26,28,29,34,35,36,37,61,62,63));
  private static final Set<Integer> datesSet = new HashSet<>(Arrays.asList(25,26,27,28));
  private static final Set<Integer> yearsSet = new HashSet<>(Arrays.asList(6,7));


  //Define date formates and min and max Dates
  //we want to allow potential future job start dates or expiration dates
  DateTimeFormatter slashes = DateTimeFormatter.ofPattern("MM/dd/yyyy");
  DateTimeFormatter dashes = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  LocalDate minDate = LocalDate.of(1950,1,1);
  LocalDate maxDate = LocalDate.of(2030,1,1);

  enum LINE_COUNTER {
    Invalid,
    Valid
  }

  public boolean validYear(String yearString, int columnNum) {
    try {
      int yearInt = Integer.parseInt(yearString);
      if (columnNum == 6 && (yearInt < 2010 || yearInt > 2023)) {
        return false;
      } else if (columnNum == 7 && (yearInt < 1989 || yearInt > 2023)) {
        return false;
      }

      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public boolean validYearOrder(String yearStringCompletion, String yearStringPermit) {
    if(yearStringCompletion.isEmpty() || yearStringPermit.isEmpty()) {
      return true;
    } else {
      //if either is malformed will have been caught by validYear
      int completionYear = Integer.parseInt(yearStringCompletion);
      int permitYear = Integer.parseInt(yearStringPermit);

      if (completionYear < permitYear) {
        return false;
      }

      return true;
    }
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

  public boolean validDateOrder(String filingDateString, String permitDateString, String lastUpdateDateString) {
    LocalDate filingDate = filingDateString.isEmpty() ? null : LocalDate.parse(filingDateString, dashes);
    LocalDate permitDate = permitDateString.isEmpty() ? null : LocalDate.parse(permitDateString, dashes);
    LocalDate lastUpdateDate = lastUpdateDateString.isEmpty() ? null : LocalDate.parse(lastUpdateDateString, dashes);

    if ((permitDate != null && permitDate.isBefore(filingDate)) || lastUpdateDate.isBefore(filingDate)) {
      return false;
    } else if (permitDate != null && lastUpdateDate.isBefore(permitDate)) {
      return false;
    }

    return true;
  }

  public String parseJobStatus(String jobStatusString) {
    //no need to check for empty string because already skip those lines
    char firstChar = jobStatusString.charAt(0);
    if (Character.isDigit(firstChar)) {
      return String.valueOf(firstChar);
    }
    else {
      return null;
    }
  }

  Pattern matchOccStringPattern = Pattern.compile("\\(([^)]+)\\)");

  public String parseOccCode(String OccString, Context context) {
    //no need to check for empty string
    Matcher matchedOccString = matchOccStringPattern.matcher(OccString);

    if (matchedOccString.find()) {
      return matchedOccString.group(1);
    } else if (OccString.equals("Empty Site")) { //for some reason empty site doesn't have a code
      return "ES";
    } else {
      context.getCounter("OccCodeCounter", OccString).increment(1);
      return null;
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

    List<String> outputColumns = new ArrayList<>();

    boolean valid = true;

    if(columns[0].equals("Job_Number")) {
      context.getCounter("HeaderCounter", "DiscardedHeader").increment(1);
      valid = false;
    } else {
      for (int index = 0; index < 63; index++) {
        if (removeColumns.contains(index+1)) {
          continue; //skip columns that should be removed
        }

        if (columns[index].isEmpty() && !allowEmptyColumnsSet.contains(index+1)) {
          context.getCounter("MissingDiscardCounter", "MissingValue" + (index+1)).increment(1);
          valid = false;
          break;
        }

        String columnValue = columns[index];

        if (boolColumn.contains(index+1)) {
          if(columnValue.isEmpty()) {
            columnValue = "False";
          } else {
            columnValue = "True";
          }
        }

        if (index+1 == 5) { //parse JobStatus
          columnValue = parseJobStatus(columnValue);
          if (columnValue == null) {
            context.getCounter("MalformedJobStatus", "ParseError").increment(1);
            valid = false;
            break;
          }
        }

        if (!columnValue.isEmpty() && (index+1 == 21 || index+1 == 22)) { //parse Occ Code
          columnValue = parseOccCode(columnValue, context);
          if (columnValue == null) {
            context.getCounter("MissingOccCode", "ParseError").increment(1);
            valid = false;
            break;
          }
        }

        if (datesSet.contains(index+1) && !columnValue.isEmpty()) {
          columnValue = dateParse(columnValue);
          if (columnValue == null) {
            context.getCounter("MalformedDiscardCounter", "MalformedDate" + (index+1)).increment(1);
            valid = false;
            break;
          }
        }

        if (datesSet.contains(index+1) && !columnValue.isEmpty() && !validDate(columnValue)) {
          context.getCounter("InvalidValueDiscardCounter", "InvalidDate").increment(1);
          valid = false;
          break;
        }

        if (yearsSet.contains(index+1) && !columnValue.isEmpty() && !validYear(columnValue, index+1)) {
          context.getCounter("InvalidYearDiscardCounter", "InvalidYear" + (index+1)).increment(1);
          valid = false;
          break;
        }

        outputColumns.add(columnValue);
      }
    }

    if (valid && !validYearOrder(outputColumns.get(5), outputColumns.get(6))) {
      context.getCounter("InvalidYearOrderCounter", "InvalidYearOrder").increment(1);
      valid = false;
    }

    if (valid && !validDateOrder(outputColumns.get(23), outputColumns.get(24), outputColumns.get(25))) {
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