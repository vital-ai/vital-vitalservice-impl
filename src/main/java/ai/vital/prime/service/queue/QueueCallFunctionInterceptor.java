package ai.vital.prime.service.queue;

import static ai.vital.prime.service.queue.QueueFunctions.Queue_GetConfig;
import static ai.vital.prime.service.queue.QueueFunctions.queueConsumer;
import static ai.vital.prime.service.queue.QueueFunctions.queueCreate;
import static ai.vital.prime.service.queue.QueueFunctions.queueList;
import static ai.vital.prime.service.queue.QueueFunctions.queueRemove;
import static ai.vital.prime.service.queue.QueueFunctions.queueRemoveConsumer;
import static ai.vital.prime.service.queue.QueueFunctions.queueSend;
import static ai.vital.prime.service.queue.QueueFunctions.queueLog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.prime.client.IVitalPrimeClient;
import ai.vital.vitalservice.QueueConsumer;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.property.BooleanProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import groovy.lang.GString;



public class QueueCallFunctionInterceptor {

	private QueueInterface queueInterface;
	
	private final static Logger log = LoggerFactory.getLogger(QueueCallFunctionInterceptor.class);
	
	/**
	 * Returns null if method call should be forwarded
	 * @param organization
	 * @param app
	 * @param functionName
	 * @return
	 */
	public ResultList callFunction(IVitalPrimeClient client, VitalOrganization organization, VitalApp app, String functionName, Map<String, Object> params) throws VitalServiceException {

		if(queueConsumer.equals(functionName)) {
			
			_initQueueInterface(client, organization, app);
			
			return handleQueueConsumer(organization, app, functionName, params);
			
		} else if(queueCreate.equals(functionName)) {
			
			_initQueueInterface(client, organization, app);
			
			return handleQueueCreate(organization, app, functionName, params);
			
		} else if(queueList.equals(functionName)) {
			
			_initQueueInterface(client, organization, app);
			
			return handleQueueList(organization, app, functionName, params);
			
		} else if(queueRemove.equals(functionName)) {
			
			_initQueueInterface(client, organization, app);
			
			return handleQueueRemove(organization, app, functionName, params);
			
		} else if(queueRemoveConsumer.equals(functionName)) {
			
			return handleQueueRemoveConsumer(organization, app, functionName, params);
			
		} else if(queueSend.equals(functionName)) {
			
			_initQueueInterface(client, organization, app);
			
			return handleQueueSend(organization, app, functionName, params);
			
		} else if(queueLog.equals(functionName)) {
		
			_initQueueInterface(client, organization, app);
		
			return handleQueueLog(organization, app, functionName, params);
		
	}
		
		
		return null;
		
	}
	
	private synchronized void _initQueueInterface(IVitalPrimeClient client, VitalOrganization organization, VitalApp app) throws VitalServiceException {
		
		if(queueInterface == null) {
			
			log.info("Queue interface not ready, initializing...");
			
			ResultList rl = null;
			try {
				rl = client.callFunction(organization, app, Queue_GetConfig, new HashMap<String, Object>());
			} catch (VitalServiceUnimplementedException e) {
				throw new VitalServiceException(e.getLocalizedMessage(), e);
			}
			
			if(rl.getStatus().getStatus() != VitalStatus.Status.ok) {
				
				throw new VitalServiceException("Error when intializing queue interface: " + rl.getStatus().getMessage());
				
			}
			
			GraphObject first = rl.first();
			if(!(first instanceof VITAL_GraphContainerObject)) throw new VitalServiceException("Expected an instanceof " + VITAL_GraphContainerObject.class.getCanonicalName());
			
			VITAL_GraphContainerObject cfg = (VITAL_GraphContainerObject) first;
			BooleanProperty enabled = (BooleanProperty) cfg.getProperty("enabled");
			if(enabled == null || !enabled.booleanValue()) throw new VitalServiceException("Messaging component not enabled in prime");
			StringProperty type = (StringProperty) cfg.getProperty("type");
			if(type == null) throw new VitalServiceException("No type property");
			
			String t = type.toString();
			
			if("kafka".equalsIgnoreCase(t)) {
				try {
					
					log.info("KafkaConfig: " + cfg.toJSON());

					queueInterface = new KafkaQueueInterface(cfg);
					
				} catch (Exception e) {
					throw new VitalServiceException(e.getLocalizedMessage(), e);
				}
			} else {
				throw new VitalServiceException("Unknown queue implementation: " + t);
			}
			
		}
		
	}
	
	private ResultList handleQueueConsumer(VitalOrganization organization, VitalApp app, String functionName, Map<String, Object> params) {

		ResultList rl = new ResultList();
		
		try {
			
			Object queueNameParam = params.get("queueName");
			
			if(queueNameParam == null) throw new Exception("No queueName param");
			
			if(!(queueNameParam instanceof String || queueNameParam instanceof GString)) throw new Exception("queueName param must be a string");
			
			String queueName = queueNameParam.toString();
			
			Object consumerParam = params.get("consumer");
			
			if(consumerParam == null) throw new Exception("No consumer param");
			
			if(!(consumerParam instanceof QueueConsumer)) throw new Exception("consumer param must be an instanceof " + QueueConsumer.class.getCanonicalName());
			
			@SuppressWarnings("unchecked")
			Map<String, Object> properties = (Map<String, Object>) params.get("properties");
			
			QueueConsumer consumer = (QueueConsumer) consumerParam;
			
			VitalStatus status = queueInterface.queueConsumer(organization, app, queueName, consumer, properties);
			
			rl.setStatus(status);
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
		}
		
		return rl;
		
	}
	

	private ResultList handleQueueCreate(VitalOrganization organization, VitalApp app, String functionName, Map<String, Object> params) {
		ResultList rl = new ResultList();
		rl.setStatus(VitalStatus.withError(queueCreate + " not implemented"));
		return rl;
	}
	
	private ResultList handleQueueList(VitalOrganization organization, VitalApp app, String functionName, Map<String, Object> params) {
		ResultList rl = new ResultList();
		rl.setStatus(VitalStatus.withError(queueList + " not implemented"));
		return rl; 
	}
	
	private ResultList handleQueueRemove(VitalOrganization organization, VitalApp app, String functionName, Map<String, Object> params) {
		ResultList rl = new ResultList();
		rl.setStatus(VitalStatus.withError(queueRemove + " not implemented"));
		return rl; 
	}
	
	
	private ResultList handleQueueRemoveConsumer(VitalOrganization organization, VitalApp app, String functionName, Map<String, Object> params) {

		ResultList rl = new ResultList();
		
		try {
			
			Object queueNameParam = params.get("queueName");
			if(queueNameParam == null) throw new Exception("No queueName param");
			
			if(!(queueNameParam instanceof String || queueNameParam instanceof GString)) throw new Exception("queueName param must be a string");
			
			String queueName = queueNameParam.toString();
			
			VitalStatus status = queueInterface.queueRemoveConsumer(organization, app, queueName);
			
			rl.setStatus(status);
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
		}
		
		return rl;
		
	}

	private ResultList handleQueueSend(VitalOrganization organization, VitalApp app, String functionName, Map<String, Object> params) {
		
		ResultList rl = new ResultList();
		
		try {
			
			Object queueNameParam = params.get("queueName");
			if(queueNameParam == null) throw new Exception("No queueName param");
			if(!(queueNameParam instanceof String || queueNameParam instanceof GString)) throw new Exception("queueName param must be a string");
			String queueName = queueNameParam.toString();
			
			Object partitionKeyParam = params.get("partitionKey");
			if(partitionKeyParam == null) throw new Exception("No partitionKey param");
			if(!(partitionKeyParam instanceof String || partitionKeyParam instanceof GString)) throw new Exception("partitionKey param must be a string");
			String partitionKey = partitionKeyParam.toString();
			
			Object objectsParam = params.get("objects");
			if(objectsParam == null) throw new Exception("No objects (List<GraphObject>) param");
			if(!(objectsParam instanceof List)) throw new Exception("objects param must be a list of GraphObject");
			@SuppressWarnings("rawtypes")
			List l = (List) objectsParam;
			if(l.size() == 0) throw new Exception("objects list must not be empty");
			for(Object o : l) {
				if(o == null) throw new Exception("Objects list must not contain null values");
				if(!(o instanceof GraphObject)) throw new Exception("Objects list must only contain GraphObject instances, found: " + o.getClass().getCanonicalName());
			}
			
			VitalStatus status = queueInterface.queueSend(organization, app, queueName, partitionKey, l);
			
			rl.setStatus(status);
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
		}
		
		return rl;
	}

private ResultList handleQueueLog(VitalOrganization organization, VitalApp app, String functionName, Map<String, Object> params) {
		
		ResultList rl = new ResultList();
		
		try {
			
			Object queueNameParam = params.get("queueName");
			if(queueNameParam == null) throw new Exception("No queueName param");
			if(!(queueNameParam instanceof String || queueNameParam instanceof GString)) throw new Exception("queueName param must be a string");
			String queueName = queueNameParam.toString();
			
			
			Object identifierParam = params.get("identifier");
			if(identifierParam == null) throw new Exception("No identifier param");
			if(!(identifierParam instanceof String || identifierParam instanceof GString)) throw new Exception("identifier param must be a string");
			String identifier = identifierParam.toString();
			
			VitalStatus status = queueInterface.queueLog(organization, app, queueName, identifier);
			
			rl.setStatus(status);
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
		}
		
		return rl;
	}
	
	
	
	
	public void close() {
		
		if(queueInterface != null) {
			queueInterface.close();
		}
		
	}

}
