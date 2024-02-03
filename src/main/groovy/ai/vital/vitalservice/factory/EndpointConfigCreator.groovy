package ai.vital.vitalservice.factory

import ai.vital.vitalservice.config.URIGenerationStrategy;
import ai.vital.vitalservice.config.VitalServiceConfig
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.admin.VitalServiceAdmin
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.typesafe.config.Config

abstract class EndpointConfigCreator<T extends VitalServiceConfig> {

	private final static Logger log = LoggerFactory.getLogger(EndpointConfigCreator.class)
	
	public abstract T createConfig();

	public void setCommonProperties(VitalServiceConfig endpointConfig, Config cfg) {
	
		
		String defaultSegmentName = null;
		try {
			defaultSegmentName = cfg.getString("defaultSegmentName");
		} catch(Exception e) {}
		
		if(defaultSegmentName == null) {
			defaultSegmentName = "default";
			log.warn("defaultSegmentName config property not found, using default value: \"default\"");
		}
		
		URIGenerationStrategy uriGenerationStrategy = null;
		try {
			uriGenerationStrategy = URIGenerationStrategy.valueOf(cfg.getString("uriGenerationStrategy"));
		} catch(Exception e) {}
		
		if(uriGenerationStrategy == null) {
			uriGenerationStrategy = URIGenerationStrategy.local;
		}
		
		endpointConfig.setDefaultSegmentName(defaultSegmentName);
		endpointConfig.setUriGenerationStrategy(uriGenerationStrategy);
			
	}
	
	public abstract void setCustomConfigProperties(T config, Config cfgObject);
	
	/**
	 * If true factory methods may be used to create multiple instances, otherwise only single instances at a time is allowed
	 * @return
	 */
	public abstract boolean allowMultipleInstances();
	
}
