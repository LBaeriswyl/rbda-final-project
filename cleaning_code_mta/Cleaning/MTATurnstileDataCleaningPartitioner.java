import org.apache.hadoop.mapreduce.Partitioner;

public class MTATurnstileDataCleaningPartitioner
	extends Partitioner<RemoteIDDevAddrTimestampTuple, TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple>
{
	@Override
	public int getPartition(RemoteIDDevAddrTimestampTuple key, TimestampStationNameDivOwnerLineNamesNumEntriesExitsTuple value, int numPartitions)
	{
		// Partition only on remote unit ID and device address, as each reducer needs full access in its partition to all available history of a given device at a given station (i.e. remote unit)
		// a mod b = (a % b + b) % b because Java's modulo operator (%) calculates remainder, which yields negative values for negative numbers
		return ( (key.getRemoteID().hashCode() * 0xFF + key.getDevAddr().hashCode()) % numPartitions + numPartitions ) % numPartitions;
	}
}
