package ai.vital.mock.service.admin

import ai.vital.lucene.memory.service.admin.VitalServiceAdminLuceneMemory
import ai.vital.mock.service.config.VitalServiceMockConfig
import ai.vital.vitalservice.exception.VitalServiceException
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalOrganization

class VitalServiceAdminMock extends VitalServiceAdminLuceneMemory {

	Map<String, ResultList> app2ResultList = [:]
	
	private VitalServiceAdminMock(VitalOrganization organization) {
		super(organization);
	}
			
	public static VitalServiceAdminMock create(VitalServiceMockConfig cfg,
			VitalOrganization organization) {

		VitalServiceAdminMock lm = new VitalServiceAdminMock(organization);
		
		try {
			lm.systemSegment.addOrganization(VitalSigns.get().getOrganization());
		} catch (VitalServiceException e) {
			throw new RuntimeException(e);
		}
		return lm;
		
	}

	@Override
	public ResultList callFunction(VitalApp app, String function,
			Map<String, Object> arguments) throws VitalServiceException,
			VitalServiceUnimplementedException {

		ResultList rl = app2ResultList.get(app.appID.toString())
		if(rl == null) throw new VitalServiceException("No canned results list set for app ${app.ID}")				
		return rl
		
	}

}
