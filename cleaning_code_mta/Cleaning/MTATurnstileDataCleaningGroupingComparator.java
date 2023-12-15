import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// For deciding which reducer input keys to group together when compiling iterables of reducer input values
public class MTATurnstileDataCleaningGroupingComparator
	extends WritableComparator
{
	public MTATurnstileDataCleaningGroupingComparator()
	{
		// Instantiates a comparator on the provided class, with true indicating that instances of MTATurnstileDataCleaningGroupingComparator should be allowed to be created
		super(RemoteIDDevAddrTimestampTuple.class, true);
	}

	// In the reduce phase, combine into a single Iterable all values associated with any RemoteIDDevAddrTimestampTuple instances that share the same remote unit ID and device address (so that the reducer can process multiple timestamps and calculate net entries and exits between consecutive entries)
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2)
	{
		RemoteIDDevAddrTimestampTuple sidatt1 = (RemoteIDDevAddrTimestampTuple) wc1;
		RemoteIDDevAddrTimestampTuple sidatt2 = (RemoteIDDevAddrTimestampTuple) wc2;

		int cmp = sidatt1.getRemoteID().compareTo(sidatt2.getRemoteID());
		if (cmp == 0)
			cmp = sidatt1.getDevAddr().compareTo(sidatt2.getDevAddr());

		return cmp;
	}
}
