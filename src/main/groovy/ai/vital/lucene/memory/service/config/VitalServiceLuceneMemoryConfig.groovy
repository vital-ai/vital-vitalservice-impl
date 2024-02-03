package ai.vital.lucene.memory.service.config

import ai.vital.vitalservice.EndpointType
import ai.vital.vitalservice.config.VitalServiceConfig

class VitalServiceLuceneMemoryConfig extends VitalServiceConfig {
	
	private static final long serialVersionUID = 1L;
	
	// no extra properties
	public VitalServiceLuceneMemoryConfig() {
		endpointtype = EndpointType.LUCENEMEMORY
	}

}
