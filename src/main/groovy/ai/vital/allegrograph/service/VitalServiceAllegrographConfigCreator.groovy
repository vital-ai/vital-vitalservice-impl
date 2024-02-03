package ai.vital.allegrograph.service

import com.typesafe.config.Config
import com.typesafe.config.ConfigException;

import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.factory.EndpointConfigCreator;

class VitalServiceAllegrographConfigCreator extends
		EndpointConfigCreator<VitalServiceAllegrographConfig> {

	@Override
	public VitalServiceAllegrographConfig createConfig() {
		return new VitalServiceAllegrographConfig();
	}

	@Override
	public void setCustomConfigProperties(
			VitalServiceAllegrographConfig config, Config cfgObject) {
		Config cfg = cfgObject.getConfig("Allegrograph")
		cfg.resolve()
		
		config.serverURL      = cfg.getString("serverURL")
		config.username       = cfg.getString("username")
		config.password       = cfg.getString("password")
		config.catalogName    = cfg.getString("catalogName") 
		config.repositoryName = cfg.getString("repositoryName")
		try { config.poolMaxTotal  = cfg.getInt("poolMaxTotal") } catch (ConfigException.Missing ex){}
	}

	@Override
	public boolean allowMultipleInstances() {
		return false;
	}

}
