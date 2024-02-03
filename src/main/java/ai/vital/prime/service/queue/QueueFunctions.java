package ai.vital.prime.service.queue;

public class QueueFunctions {

	/**
	 * By default the number of workers will be equal to the number 
	 */
	public final static String property_vital_workers_threads = "vital.workers.threads";
	
	/**
	 * Use it to override default group.id read from remote server 
	 */
	public final static String property_group_id = "group.id";
	
	public final static String property_instance_id = "instance_id";

	
	public final static String queueSend = "queueSend";
	
	public final static String queueConsumer = "queueConsumer";
	
	public final static String queueRemoveConsumer = "queueRemoveConsumer";
	
	public final static String queueList = "queueList";
	
	public final static String queueCreate = "queueCreate";
	
	public final static String queueRemove = "queueRemove";
	
	public final static String queueLog = "queueLog";

	
	
	// internal function
	final static String Queue_GetConfig = "commons/scripts/Queue_GetConfig"; 
	
}
