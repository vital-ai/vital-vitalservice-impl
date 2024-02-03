package ai.vital.prime.service.queue;

import java.util.List;
import java.util.Map;

import ai.vital.vitalservice.QueueConsumer;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;

interface QueueInterface {

    // MESSAGING
    public VitalStatus queueSend(VitalOrganization organization, VitalApp app, String queueName, String partitionKey, List<GraphObject> objects) throws Exception;
    
    public VitalStatus queueConsumer(VitalOrganization organization, VitalApp app, String queueName, QueueConsumer consumer, Map<String, Object> properties) throws Exception;
    
    public VitalStatus queueRemoveConsumer(VitalOrganization organization, VitalApp app, String queueName) throws Exception;

    public VitalStatus queueLog(VitalOrganization organization, VitalApp app, String queueName, String identifier) throws Exception;
    
	public void close();
    
//    public ResultList queueList(VitalApp app) throws VitalServiceException, VitalServiceUnimplementedException;
    
//    public VitalStatus queueCreate(VitalApp app, String queueName) throws VitalServiceException, VitalServiceUnimplementedException;
    
//    public VitalStatus queueRemove(VitalApp app, String queueName) throws Exception; 
    
//    public VITAL_GraphContainerObject queueGetConfig() throws VitalServiceException, VitalServiceUnimplementedException;
	
	
}
