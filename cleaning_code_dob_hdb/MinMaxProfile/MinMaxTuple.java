import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;

public class MinMaxTuple implements Writable {

  private double min;
  private double max;

  public double getMin() {
    return min;
  }
  public void setMin(double min) {
    this.min = min;
  }
  public double getMax() {
    return max;
  }
  public void setMax(double max) {
    this.max = max;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // Read the data out in the order it is written,
    // creating new Date objects from the UNIX timestamp
    min = in.readDouble();
    max = in.readDouble();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // Write the data out in the order it is read,
    // using the UNIX timestamp to represent the Date
    out.writeDouble(min);
    out.writeDouble(max);
  }
  
  @Override
  public String toString() {
    return min + "\t" + max;
  }
}