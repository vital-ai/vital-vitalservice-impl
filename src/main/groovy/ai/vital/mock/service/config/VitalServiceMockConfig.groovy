package ai.vital.mock.service.config

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.config.VitalServiceConfig;

class VitalServiceMockConfig extends VitalServiceConfig {

	private static final long serialVersionUID = 1L;
	
	public VitalServiceMockConfig() {
		endpointtype = EndpointType.MOCK
	}
}
