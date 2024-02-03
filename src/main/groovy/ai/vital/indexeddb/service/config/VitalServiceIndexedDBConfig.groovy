package ai.vital.indexeddb.service.config

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.config.VitalServiceConfig;

class VitalServiceIndexedDBConfig extends VitalServiceConfig {

	private static final long serialVersionUID = 1L;
	
	public static enum QueryTarget {
		index,
		database
	}
	
	VitalServiceConfig indexConfig
	
	VitalServiceConfig dbConfig
	
	QueryTarget selectQueries = QueryTarget.index
	
	QueryTarget graphQueries = QueryTarget.database
	
	public	VitalServiceIndexedDBConfig() {
		endpointtype = EndpointType.INDEXDB
	}
	
}
