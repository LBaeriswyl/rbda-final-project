import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;

public class MinMaxDateTuple implements Writable {

  private LocalDate min;
  private LocalDate max;

  DateTimeFormatter dashes = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  public LocalDate getMin() {
    return min;
  }
  public void setMin(LocalDate min) {
    this.min = min;
  }
  public LocalDate getMax() {
    return max;
  }
  public void setMax(LocalDate max) {
    this.max = max;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    min = LocalDate.parse(in.readUTF(), dashes);
    max = LocalDate.parse(in.readUTF(), dashes);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // Write the data out in the order it is read,
    // using the UNIX timestamp to represent the Date
    out.writeUTF(min.format(dashes));
    out.writeUTF(max.format(dashes));
  }
  
  @Override
  public String toString() {
    return min.format(dashes) + "\t" + max.format(dashes);
  }
}