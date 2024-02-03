package ai.vital.prime.service.queue

import org.apache.kafka.clients.producer.*
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.utils.Utils
import java.util.Map
import java.util.Random
import org.slf4j.Logger
import org.slf4j.LoggerFactory


public class MessagingPartitioner implements Partitioner {

	private final static Logger log = LoggerFactory.getLogger(KafkaQueueInterface.class);
	
	
	private final DefaultPartitioner defaultPartitioner = new DefaultPartitioner();
	// private final RoundRobinPartitioner roundRobinPartitioner = new RoundRobinPartitioner();

	private static Random rand = new Random()
	
	@Override
	public void configure(Map<String, ?> configs) {
		defaultPartitioner.configure(configs)
		// roundRobinPartitioner.configure(configs)
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		if (key == null) {
			
			List<PartitionInfo> partitions = cluster.partitionsForTopic(topic)
			
			int numPartitions = partitions.size()
			
			List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic)
			
			int numAvailablePartitions = availablePartitions.size()
			
			if(numAvailablePartitions > 0) {
				
				int randomNum = rand.nextInt(numAvailablePartitions)
				
				PartitionInfo partInfo = availablePartitions[randomNum]
				
				int partitionNum  = partInfo.partition()
				
				log.info("Selecting random partition ${partitionNum} from available set of ${numAvailablePartitions} from total ${numPartitions}.")
				
				return partitionNum
				
			}
			else {
				
				int randomNum = rand.nextInt(numPartitions)
				
				PartitionInfo partInfo = partitions[randomNum]
				
				int partitionNum  = partInfo.partition()
				
				log.info("Selecting random partition ${partitionNum} from total set of ${numPartitions}.")
				
				return partitionNum
				
			}
			
			// return roundRobinPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
		} else {
			// Otherwise, use DefaultPartitioner
			return defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
		}
	}

	@Override
	public void close() {
		defaultPartitioner.close();
		// roundRobinPartitioner.close();
	}
}