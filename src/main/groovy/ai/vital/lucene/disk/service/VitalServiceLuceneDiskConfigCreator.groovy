package ai.vital.lucene.disk.service

import org.slf4j.Logger
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config
import com.typesafe.config.ConfigException;

import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.factory.EndpointConfigCreator
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig

class VitalServiceLuceneDiskConfigCreator extends EndpointConfigCreator<VitalServiceLuceneDiskConfig> {

	private final static Logger log = LoggerFactory.getLogger(VitalServiceLuceneDiskConfigCreator.class)
	
	@Override
	public VitalServiceLuceneDiskConfig createConfig() {
		return new VitalServiceLuceneDiskConfig();
	}

	@Override
	public void setCustomConfigProperties(VitalServiceLuceneDiskConfig config,
			Config cfgObject) {

		Config cfg = cfgObject.getConfig("LuceneDisk")
		
		config.rootPath = cfg.getString("rootPath")
		
		try {
			config.bufferWrites = cfg.getBoolean('bufferWrites')
		} catch(ConfigException.Missing e) {
			log.warn('bufferWrites not set, using default value: ' + config.getBufferWrites())
		}
		
		try {
			config.commitAfterNWrites = cfg.getInt('commitAfterNWrites')
		} catch(ConfigException.Missing e) {
			log.warn('commitAfterNWrites not set, using default value: ' + config.getCommitAfterNWrites())
		}
		
		try {
			config.commitAfterNSeconds = cfg.getInt('commitAfterNSeconds');
		} catch(ConfigException.Missing e) {
			log.warn('commitAfterNSeconds not set, using default value: ' + config.getCommitAfterNSeconds())
		}
		
	}

	@Override
	public boolean allowMultipleInstances() {
		return false;
	}

}
