package ai.vital.mock.service.superadmin;

import java.util.HashMap;
import java.util.Map;

import ai.vital.lucene.memory.service.superadmin.VitalServiceSuperAdminLuceneMemory;
import ai.vital.mock.service.config.VitalServiceMockConfig;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasOrganizationID;

public class VitalServiceSuperAdminMock extends VitalServiceSuperAdminLuceneMemory {

	Map<String, Map<String, ResultList>> organization2App2ResultList = new HashMap<String, Map<String, ResultList>>();
	

	private VitalServiceSuperAdminMock() {
		super();
	}

	public static VitalServiceSuperAdminMock create(VitalServiceMockConfig config) {
		

		return new VitalServiceSuperAdminMock();
		
	}

	@Override
	public ResultList callFunction(VitalOrganization organization, VitalApp app, String function,
			Map<String, Object> arguments) throws VitalServiceException,
			VitalServiceUnimplementedException {

		Map<String, ResultList> m = organization2App2ResultList.get(organization.getRaw(Property_hasOrganizationID.class));
		
		ResultList rl = null;
		
		if(m != null) {
			rl = m.get(app.getRaw(Property_hasAppID.class));
		}
				
		if(rl == null) throw new VitalServiceException("Mock service canned results list not set for organization: " + organization.getRaw(Property_hasOrganizationID.class) + ", app: " + app.getRaw(Property_hasAppID.class));
		
		return rl;
		
	}

}
