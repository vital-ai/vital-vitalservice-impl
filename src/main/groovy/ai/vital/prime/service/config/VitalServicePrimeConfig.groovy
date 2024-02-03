package ai.vital.prime.service.config

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.config.VitalServiceConfig

class VitalServicePrimeConfig extends VitalServiceConfig {

	private static final long serialVersionUID = 1L;
	
	String endpointURL
	
	public VitalServicePrimeConfig() {
		endpointtype = EndpointType.VITALPRIME
	}
	
}
