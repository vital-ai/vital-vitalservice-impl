package ai.vital.allegrograph.service.config

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.config.VitalServiceConfig

class VitalServiceAllegrographConfig extends VitalServiceConfig {

	private static final long serialVersionUID = 1L;
	
	String serverURL
	
	String username
	
	String password
	
	String catalogName
	
	String repositoryName
	
	// will fall back to commons default
	Integer poolMaxTotal = null
	
	public VitalServiceAllegrographConfig() {
		endpointtype = EndpointType.ALLEGROGRAPH
	}
	
}
