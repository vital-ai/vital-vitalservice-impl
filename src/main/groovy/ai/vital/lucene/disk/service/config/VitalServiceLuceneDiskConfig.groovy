package ai.vital.lucene.disk.service.config

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.config.VitalServiceConfig

class VitalServiceLuceneDiskConfig extends VitalServiceConfig {

	private static final long serialVersionUID = 1L;
	
	String rootPath
	
	
	/**
	 * only for disk based segment, when enabled the operations will be committed after every N operations
	 */
	boolean bufferWrites = false;
	
	/**
	 * only for disk based segment, commit after n buffered writes/changes
	 */
	int commitAfterNWrites = 1;

	/**
	 * only for disk based segment, commit after n seconds since last commit (if there are any pending buffered changes)	
	 */
	int commitAfterNSeconds = 10;
	
	public VitalServiceLuceneDiskConfig() {
		endpointtype = EndpointType.LUCENEDISK
	}
	
}
