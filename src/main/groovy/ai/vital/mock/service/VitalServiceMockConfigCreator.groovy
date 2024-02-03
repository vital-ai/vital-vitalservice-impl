package ai.vital.mock.service

import com.typesafe.config.Config;

import ai.vital.mock.service.admin.VitalServiceAdminMock
import ai.vital.mock.service.config.VitalServiceMockConfig
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.factory.EndpointConfigCreator;

class VitalServiceMockConfigCreator extends EndpointConfigCreator<VitalServiceMockConfig>{

	@Override
	public VitalServiceMockConfig createConfig() {
		return new VitalServiceMockConfig();
	}

	@Override
	public void setCustomConfigProperties(VitalServiceMockConfig config,
			Config cfgObject) {
			
		// expect an empty object to be set ?
		Config cfg = cfgObject.getConfig("Mock")
		
	}

	@Override
	public boolean allowMultipleInstances() {
		return false;
	}

}
