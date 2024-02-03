package ai.vital.prime.service

import com.typesafe.config.Config

import ai.vital.prime.service.admin.VitalServiceAdminPrime
import ai.vital.prime.service.config.VitalServicePrimeConfig;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.factory.EndpointConfigCreator;

class VitalServicePrimeConfigCreator extends
		EndpointConfigCreator<VitalServicePrimeConfig> {

	@Override
	public VitalServicePrimeConfig createConfig() {
		return new VitalServicePrimeConfig();
	}

	@Override
	public void setCustomConfigProperties(VitalServicePrimeConfig config,
			Config cfgObject) {
		Config cfg = cfgObject.getConfig("VitalPrime")
		config.endpointURL = cfg.getString("endpointURL")
	}

	@Override
	public boolean allowMultipleInstances() {
		return true;
	}

}
