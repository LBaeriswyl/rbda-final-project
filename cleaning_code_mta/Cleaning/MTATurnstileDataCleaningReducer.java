import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;	// For calculating entry/time or exit/time ratios
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MTATurnstileDataCleaningReducer
	extends Reducer<RemoteIDDevAddrTimestampTuple, TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple,
					NullWritable, Text>
{
	// Maximum plausible number of entries/second
	private final static double ENTRY_RATE_DISCARD_THRESHOLD = 1.0;	
	// Minimum plausible number of exits/second
	private final static double EXIT_RATE_DISCARD_THRESHOLD = 1.0;
	// Timeout in hours between adjacent records, beyond which turnstile counter should be considered as starting anew; using more precise increments because time differences truncate to nearest integers
	private final static long OFFLINE_HOURS_DISCARD_THRESHOLD = 24;
	private final static long COUNTER_ROLLOVER_NUM = TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple.COUNTER_ROLLOVER_NUM;

	@Override
	public void reduce(RemoteIDDevAddrTimestampTuple key, Iterable<TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple> values, Context context)
		throws IOException, InterruptedException
	{
		// Create one-use Iterator in order to get first element
		Iterator<TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple> val_iter = values.iterator();
		TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple init_val = val_iter.next();
		long prev_num_entries = init_val.getNumEntries().get();
		long prev_num_exits = init_val.getNumExits().get();

		String prev_timestamp = init_val.getTimestamp().toString();
		
		// Unknown preceding timeframe, so populate with sentinel values; however, don't remove so that sucessive records will have a well-defined preceding timeframe
		String output = key.getRemoteID().toString() + ","
							+ key.getDevAddr().toString() + ","
							+ prev_timestamp + ","
							+ init_val.getStationName().toString() + ","
							+ init_val.getDivOwner().toString() + ","
							+ init_val.getLineNames().toString() + ","
							+ Long.toString(-1) + ","
							+ Long.toString(-1);

		context.write(NullWritable.get(), new Text(output));

		while (val_iter.hasNext())
		{
			TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple value = val_iter.next();

			String curr_timestamp = value.getTimestamp().toString();

			// Duplicate entry (and not just the first chronological entry); skip
			if (curr_timestamp.equals(prev_timestamp))
			{
				context.getCounter(MTATurnstileDataCleaning.Record.BAD_RECORD).increment(1);
				System.err.println("Duplicate timestamp in input: " + value);
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(MTATurnstileDataCleaning.Timestamp.DUPLICATE).increment(1);
				continue;
			}

			boolean num_entries_decreasing = false;
			boolean num_exits_decreasing = false;
			
			long curr_num_entries = value.getNumEntries().get();
			long curr_num_exits = value.getNumExits().get();

			long net_entries = curr_num_entries - prev_num_entries;
			long net_exits = curr_num_exits - prev_num_exits;

			// Check for decreasing values
			if (net_entries < 0)
			{
				net_entries = -net_entries;
				num_entries_decreasing = true;
			}
			if (net_exits < 0)
			{
				net_exits = -net_exits;
				num_exits_decreasing = true;
			}

			// a mod b = (a % b + b) % b because Java's modulo operator (%) calculates remainder, which yields negative values for negative numbers
			net_entries = ((net_entries % COUNTER_ROLLOVER_NUM) + COUNTER_ROLLOVER_NUM) % COUNTER_ROLLOVER_NUM;
			net_exits = ((net_exits % COUNTER_ROLLOVER_NUM) + COUNTER_ROLLOVER_NUM) % COUNTER_ROLLOVER_NUM;

			LocalDateTime prev_datetime = LocalDateTime.parse(prev_timestamp, TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple.DT_FMT);
			LocalDateTime curr_datetime = LocalDateTime.parse(curr_timestamp, TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple.DT_FMT);

			// Calculate time elapsed; use as threshold for reasonableness of net entry/net exit values
			long hour_diff = ChronoUnit.HOURS.between(prev_datetime, curr_datetime);

			// Calculate the ratio between time elapsed and net entries/net exits; use as threshold for reasonableness of net entry/net exit values
			double entry_rate = (double) net_entries/ChronoUnit.SECONDS.between(prev_datetime, curr_datetime);
			double exit_rate = (double) net_exits/ChronoUnit.SECONDS.between(prev_datetime, curr_datetime);

			// For the sake of knowing the time period covered by the next record, need to keep an invalid record but note that it is invalid
			if (hour_diff > OFFLINE_HOURS_DISCARD_THRESHOLD)
			{
				net_entries = -1;
				net_exits = -1;
				System.err.println("Number of half-days from previous entry exceeded timeout threshold of " + Long.toString(OFFLINE_HOURS_DISCARD_THRESHOLD) + " half-days in input: " + value);
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(MTATurnstileDataCleaning.Timestamp.ADJ_REC_TIMEOUT_THRESHOLD_EXCEEDED).increment(1);
			}

			if (entry_rate > ENTRY_RATE_DISCARD_THRESHOLD)
			{
				net_entries = -1;
				System.err.println("Net entries exceeded plausibility threshold of " + Double.toString(ENTRY_RATE_DISCARD_THRESHOLD) + " entries/s in input: " + value);
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(MTATurnstileDataCleaning.NumEntries.VALUE_JUMP).increment(1);
			}				
			// Threshold not exceeded; good data, machine counter potentially miswired
			else if (num_entries_decreasing)
				context.getCounter(MTATurnstileDataCleaning.NumEntries.DECREASING).increment(1);

			if (exit_rate > EXIT_RATE_DISCARD_THRESHOLD)
			{
				net_exits = -1;
				System.err.println("Net exits exceeded plausibility threshold of " + Double.toString(EXIT_RATE_DISCARD_THRESHOLD) + " exits/s in input: " + value);
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(MTATurnstileDataCleaning.NumExits.VALUE_JUMP).increment(1);
			}
			else if (num_exits_decreasing)
				context.getCounter(MTATurnstileDataCleaning.NumExits.DECREASING).increment(1);

			if (entry_rate > ENTRY_RATE_DISCARD_THRESHOLD
				|| exit_rate > EXIT_RATE_DISCARD_THRESHOLD
				|| hour_diff > OFFLINE_HOURS_DISCARD_THRESHOLD)
				context.getCounter(MTATurnstileDataCleaning.Record.BAD_RECORD).increment(1);

			output = key.getRemoteID().toString() + ","
						+ key.getDevAddr().toString() + ","
						+ curr_timestamp + ","
						+ value.getStationName().toString() + ","
						+ value.getDivOwner().toString() + ","
						+ value.getLineNames().toString() + ","
						+ Long.toString(net_entries) + ","
						+ Long.toString(net_exits);

			context.write(NullWritable.get(), new Text(output));

			// Prepare for next iteration
			prev_timestamp = curr_timestamp;
			prev_num_entries = curr_num_entries;
			prev_num_exits = curr_num_exits;
		}
	}
}
