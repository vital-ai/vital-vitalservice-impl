package ai.vital.mock.service

import ai.vital.lucene.memory.service.VitalServiceLuceneMemory
import ai.vital.mock.service.config.VitalServiceMockConfig
import ai.vital.vitalservice.exception.VitalServiceException
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalOrganization

/**
 * Wrap the lucene service memory implementation
 * Adds canned results list for function calls for easier testing
 */
public class VitalServiceMock extends VitalServiceLuceneMemory {

	ResultList callFunctionResults
	
	@Override
	public ResultList callFunction(String function,
			Map<String, Object> arguments) throws VitalServiceException,
			VitalServiceUnimplementedException {
		if(this.callFunctionResults == null) throw new VitalServiceException("Mock service canned results list not set")
		return this.callFunctionResults
	}

	private VitalServiceMock(VitalOrganization organization, VitalApp app) {
		super(organization, app);
	}

	public static VitalServiceMock create(VitalServiceMockConfig cfg,
			VitalOrganization organization, VitalApp app) {

		VitalServiceMock lm = new VitalServiceMock(organization, app);
		try {
			lm.systemSegment.addOrganization(VitalSigns.get().getOrganization());
			
			VitalApp _app = cfg.getApp();
			if(_app == null) throw new VitalServiceException("No app set in the config, cannot use lucene memory service instance");
			
			lm.systemSegment.addApp(organization, _app);
			
		} catch (VitalServiceException e) {
			throw new RuntimeException(e);
		}
		
		return lm;
	}
	
}
