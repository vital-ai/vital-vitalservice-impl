package ai.vital.prime.service.queue;

import java.io.Serializable;
import java.io.StringReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.vitalservice.QueueConsumer;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.java.VitalJavaSerializationUtils;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasOrganizationID;
import ai.vital.vitalsigns.model.property.IntegerProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import java.util.Collection;

public class KafkaQueueInterface implements QueueInterface {

	private final static Logger log = LoggerFactory.getLogger(KafkaQueueInterface.class);
	
	private Properties producerProperties = new Properties();
	
	private KafkaProducer<String, byte[]> producer;
	
	private Properties consumerProperties = new Properties();
	
	private Map<String, List<ConsumerWrapper>> consumers = new HashMap<String, List<ConsumerWrapper>>();
	
	private int maxMessageBytesLength = DEFAULT_maxMessageBytesLength; 
	
	public static final Integer DEFAULT_maxMessageBytesLength = 990 * 1000;
		
	static class RebalanceListener implements ConsumerRebalanceListener {

	    @Override
	    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
	       
	    	log.info("Partitions Revoked: " + partitions);
	    }

	    @Override
	    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
	    	
	    	
	    	log.info("Partitions assigned: " + partitions);
	    }
	}

	static class ConsumerWrapper {
		
		int num;
		
		KafkaConsumer<String, byte[]> kConsumer;
		
		QueueConsumer vConsumer;
		
		boolean running = false;
		
		boolean threadStopped = false;
		
		Thread thread = null;

		private List<String> topicNames;
		
		private List<String> fullTopicNames;

		private Properties properties;
		
		private String instance_id;
		
		public ConsumerWrapper(int num, String instance_id, List<String> topicNames, List<String> fullTopicNames, Properties properties, QueueConsumer vConsumer) {
			this.num = num;
			
			this.instance_id = instance_id;
			
			this.properties = properties;
			this.topicNames = topicNames;
			this.fullTopicNames = fullTopicNames;
			this.vConsumer = vConsumer;

		}
		public boolean close(long timeout) {

			running = false;
			
			int i = 0 ;
			
			long delta = 50;
			
			long steps = timeout >= delta ? timeout / delta : 0;
			
			while(thread.isAlive() && i < steps) {
				
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				i++;
				
			}
			
			
			if(thread.isAlive()) {
				log.error("Couldn't stop the consumer thread within {}ms", timeout);
				return false;
			} else {
				log.info("Consumer thread stopped successfully after {}ms", i * delta);
				return true;
				
			}
			
		}
		
		public void start () {
			
			if(running) throw new RuntimeException("Consumer already started");
			
			running = true;
			
			thread = new Thread(){
				@SuppressWarnings("unchecked")
				@Override
				public void run() {

					List<String> topics = fullTopicNames;
					
					kConsumer = new KafkaConsumer<String, byte[]>(properties, new StringDeserializer(), new ByteArrayDeserializer());
										
					RebalanceListener rebalanceListener = new RebalanceListener();

					log.info("Consumer Subscribing to topics: " + topics);
					
					kConsumer.subscribe(topics, rebalanceListener);
					
					while(running) {

				         ConsumerRecords<String, byte[]> records = null;
				         
				         // isolate from other consumers?
				        //  synchronized (ConsumerWrapper.class) {
				        	 
				        	 records = kConsumer.poll(Duration.ofMillis(100)); 
				          
				        //}
				         
				         // log.info("Consumer #{} received {} record(s)", num, records.count() );
				         
				         for (ConsumerRecord<String, byte[]> record : records) {
				        	 
				        	 long start = System.currentTimeMillis();
				        	 
				        	 log.info("Processing Message for key: " + record.key());
				        	 
				        	 String topic = record.topic();
				        	 
				        	 String partitionID = "" + record.partition();
					        	
					        	
				        	 Set<TopicPartition> assignedPartitionSet = kConsumer.assignment();
				        	 
				        	 
				        	 
				        	 String[] tc = topic.split("__");
				        	 String fullTopicName = topic;
				        	 String topicName = tc[2];
				        	 
				        	 byte[] value = record.value();
				        	 
				        	 log.info("Consumer #{}, kafka topic = {} record partition = {} offset = {}, timestamp = {} key = {}, value = {}", num, fullTopicName, record.partition(), record.offset(), record.timestamp(), record.key(), value.length);
				        	
				        	 List<GraphObject> msg = null;
				        	 
				        	 try {
				        		 
								msg = (List<GraphObject>) VitalJavaSerializationUtils.deserialize(value);				        		 
				        	 
				        	 } catch(Exception e) {
				        		 log.error("Error when deserializing kafka message: " + e.getLocalizedMessage(), e);
				        		 continue;
				        	 }

				        	 if(msg == null) {
				        		 log.error("Null graph objects list message - skipping");
				        		 continue;
				        	 }
				        	 
				        	 ResultList rl = new ResultList();
				        	 
				        	 for(GraphObject g : msg) {
				        		 rl.addResult(g);
				        	 }
				        	 
				        	 log.info("Consumer #{}, processing kafka record partition = {} offset = {}, timestamp = {} key = {}", num, record.partition(), record.offset(), record.timestamp(), record.key());	 

				        	 
				        	 List<String> partitionList = new ArrayList<String>();
				        	 	
				        	 for (TopicPartition p : assignedPartitionSet) {
				        		 
				        		 	String partitionName = p.topic() + "-" + p.partition();
				        		 
				        		 	partitionList.add(partitionName);	 	
				        	 }
				        	 
				        	 String consumerID = topics.toString() + "-" + num + "-" + instance_id;
				        	 
				        	 vConsumer.messageReceived(topicName, consumerID, partitionID, partitionList, rl);
				        	 
				        	 log.info("Consumer #{}, processed kafka record partition = {} offset = {}, timestamp = {} key = {} time {}ms", num, record.partition(), record.offset(), record.timestamp(), record.key(), System.currentTimeMillis() - start);	 
				         }
					}
					
					kConsumer.close();
				}
			};
			
			thread.setDaemon(true);
			
			thread.start();
		}
	}
	
	public KafkaQueueInterface(VITAL_GraphContainerObject cfg) throws Exception {

		StringProperty producerPropertiesString = (StringProperty) cfg.getProperty("producer");
		
		StringProperty consumerPropertiesString = (StringProperty) cfg.getProperty("consumer");
		
		if(producerPropertiesString == null) throw new Exception("No producer config property");
		
		if(consumerPropertiesString == null) throw new Exception("No consumer config property");
		
		IntegerProperty maxMessageBytesLengthProp = (IntegerProperty) cfg.getProperty("maxMessageBytesLength");
		
		if(maxMessageBytesLengthProp != null) {
			
			this.maxMessageBytesLength = maxMessageBytesLengthProp.intValue();
			
			log.info("maxMessageBytesLength: {} bytes", this.maxMessageBytesLength);
			
		} else {
			
			log.warn("No maxMessageBytesLength config, default value: " + maxMessageBytesLengthProp);
			
		}
		
		producerProperties.load(new StringReader(producerPropertiesString.asString()));
		
		consumerProperties.load(new StringReader(consumerPropertiesString.asString()));
		
		log.info("Starting kafka producer...");
		
		producer = new KafkaProducer<String, byte[]>(producerProperties, new StringSerializer(), new ByteArraySerializer());
		
		log.info("Shared producer started successfully");
		
	}

	private String getFullQueueName(VitalOrganization organization, VitalApp app, String queueName) {
		return organization.get(Property_hasOrganizationID.class).toString() + "__" + app.get(Property_hasAppID.class).toString() + "__" + queueName; 
	}
	
	@Override
	public VitalStatus queueSend(VitalOrganization organization, VitalApp app, String queueName, String partitionKey,
			List<GraphObject> objects) throws Exception {
		
		List<GraphObject> out = null;
		
		//serialize objects as java bytes
		if(!(objects instanceof Serializable)) {
			out = new ArrayList<GraphObject>();
			out.addAll(objects);
		} else {
			out = objects;
		}
		
		final String uri = objects.size() > 0 ? objects.get(0).getURI() : null;
		
		String fullQueueName = getFullQueueName(organization, app, queueName);
		
		byte[] serialized = SerializationUtils.serialize((Serializable) out);
		
		if(serialized.length > this.maxMessageBytesLength) {
			throw new Exception("Serialized message length " + serialized.length + " exceeds max allowed value: " + this.maxMessageBytesLength + " bytes");
		}
		
		final long now_ts = new Date().getTime();

		producer.send(new ProducerRecord<String, byte[]>(fullQueueName, partitionKey, serialized), new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {

				if(exception != null) {
					log.error("Error when sending kafka message URI " + uri + " : " + exception.getLocalizedMessage() + " - " + ( metadata != null ? metadata.toString() : "" ), exception);
				} else {
					
					long tm = metadata.timestamp();
					
					long delta = now_ts - tm;
				
					
					log.info("Kafka message URI " + uri + " delta: " + delta +  "sent - " + ( metadata != null ? metadata.toString() : ""));
				}
			}
		});
	
		return VitalStatus.withOKMessage("Message sent");
		
	}

	@Override
	public VitalStatus queueLog(VitalOrganization organization, VitalApp app, String queueName, String identifier) throws Exception {
		
		String fullQueueName = getFullQueueName(organization, app, queueName);

		log.info(identifier + " Listing Partitions");
		
		int consumerCount = 0;
		
		for(ConsumerWrapper k : consumers.get(fullQueueName)) {
			
			consumerCount++;
			
			KafkaConsumer<String, byte[]> kConsumer = k.kConsumer;
			
			Set<TopicPartition> assignedPartitions = kConsumer.assignment();
			
			for(TopicPartition topicPartition : assignedPartitions) {
				
				log.info( identifier + " : " + consumerCount + " : Assigned Partition: " + topicPartition.topic() + " - " + topicPartition.partition());
		
			}
		}
		
		return VitalStatus.withOKMessage("Logged");
		
	}
	
	

	@Override
	public synchronized VitalStatus queueConsumer(VitalOrganization organization, VitalApp app, String queueName,
			QueueConsumer consumer, Map<String, Object> _properties) throws Exception {
		
		if(_properties == null) _properties = new HashMap<String, Object>();
		
		List<String> topicNames = new ArrayList<String>(Arrays.asList(queueName.split(",")));
		List<String> fullTopicNames = new ArrayList<String>();
		for(String topicName : topicNames) {
			if(topicName.isEmpty()) throw new Exception("Topic name cannot be empty");
			String fullQueueName = getFullQueueName(organization, app, topicName);
			fullTopicNames.add(fullQueueName);
		}
		
		
		Collections.sort(fullTopicNames);
		String key = StringUtils.join(fullTopicNames, ",");
		List<ConsumerWrapper> list = consumers.get(key);
		
		if(list != null) throw new Exception("A consumer already registered to topic: " + queueName + " (" + key + ")");
		
		Properties targetProperties = new Properties();
		targetProperties.putAll(consumerProperties);
		targetProperties.putAll(_properties);
		
		int threadsCount = 1;
		
		Integer workersCount = (Integer) _properties.get(QueueFunctions.property_vital_workers_threads);
		
		String instanceIdentifier = (String) _properties.get(QueueFunctions.property_instance_id);

		if(instanceIdentifier == null) {
			instanceIdentifier = "instance_id";
		}
		
		
		if(workersCount == null) {
			
			log.info("Getting partitions for topic: " + fullTopicNames.get(0));
			List<PartitionInfo> partitionsFor = producer.partitionsFor(fullTopicNames.get(0));
			int partitionsCount = partitionsFor.size();
			
			log.info("Topic: {} partitions count: {}" , fullTopicNames.get(0), partitionsCount);
			
			threadsCount = partitionsCount;
			
		} else {
			
			if(workersCount < 1) throw new Exception(QueueFunctions.property_vital_workers_threads + " count must be > 0: " + workersCount);
			
			log.info("Using custom workers count: {}", workersCount);
			
			threadsCount = workersCount;
			
		}
		
		
		log.info("Effective consumer properties: {}", targetProperties);
		
		
		list = new ArrayList<ConsumerWrapper>();
		
		for(int i = 0 ; i < threadsCount; i++) {
			
			ConsumerWrapper cw = new ConsumerWrapper(i+1, instanceIdentifier, topicNames, fullTopicNames, targetProperties, consumer);
			
			cw.start();
			
			list.add(cw);
			
		}
		
		consumers.put(key, list);
		
		VitalStatus status = VitalStatus.withOKMessage("Consumer registered");
		
		status.setSuccesses(threadsCount);
		
		return status;
		
	}

	@Override
	public void close() {

		for(Entry<String, List<ConsumerWrapper>> e : consumers.entrySet() ) {
			
			for(ConsumerWrapper consumer : e.getValue()) {
				
				try {
					consumer.close(0);
				} catch(Exception ex) {
					log.error("Error when closing consumer, queue: " + e.getKey() + ": " + ex.getLocalizedMessage());
				}
				
			}
			
		}
		
		if(producer != null) {
			
			try {
				producer.close();
				log.info("Producer shut down successfully");
			} catch(Exception e) {
				log.error("Error when shutting down producer: " + e.getLocalizedMessage(), e);
			}
			producer = null;
			
		}
		
	}

	@Override
	public synchronized VitalStatus queueRemoveConsumer(VitalOrganization organization, VitalApp app, String queueName) throws Exception {
		
		try {
			
			List<String> topicNames = new ArrayList<String>(Arrays.asList(queueName.split(",")));
			List<String> fullTopicNames = new ArrayList<String>();
			for(String topicName : topicNames) {
				if(topicName.isEmpty()) throw new Exception("Topic name cannot be empty");
				String fullQueueName = getFullQueueName(organization, app, topicName);
				fullTopicNames.add(fullQueueName);
			}
			
			
			Collections.sort(fullTopicNames);
			String key = StringUtils.join(fullTopicNames, ",");
			
			List<ConsumerWrapper> list = consumers.remove(key);
			
			if(list == null) {
				throw new Exception("no consumer registered for queue: " + queueName);
			}
			
			for(ConsumerWrapper cw : list) {
				
				cw.close(500);
				
			}

			return VitalStatus.withOKMessage("Consumer removed from queue: " + queueName);
		
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
		
	}

	
}
