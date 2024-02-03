package ai.vital.lucene.memory.service

import com.typesafe.config.Config;

import ai.vital.lucene.memory.service.config.VitalServiceLuceneMemoryConfig
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.factory.EndpointConfigCreator

class VitalServiceLuceneMemoryConfigCreator extends EndpointConfigCreator<VitalServiceLuceneMemoryConfig> {

	@Override
	public VitalServiceLuceneMemoryConfig createConfig() {
		return new VitalServiceLuceneMemoryConfig();
	}

	@Override
	public void setCustomConfigProperties(
			VitalServiceLuceneMemoryConfig config, Config cfgObject) {

		// expect an empty object to be set ? 			
		Config cObj = cfgObject.getConfig("LuceneMemory")
			
	}

	@Override
	public boolean allowMultipleInstances() {
		return false;
	}

}
