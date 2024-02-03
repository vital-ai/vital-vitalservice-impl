package ai.vital.vitalservice.superadmin.impl;

import java.util.List;

import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.query.PathQueryImplementation.PathQueryExecutor;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.superadmin.VitalServiceSuperAdmin;
import ai.vital.vitalsigns.meta.GraphContext;

public class VitalServiceSuperAdminPathQueryExecutor extends PathQueryExecutor {

	private VitalServiceSuperAdmin vitalServiceSuperAdmin;

	public VitalServiceSuperAdminPathQueryExecutor(VitalServiceSuperAdmin vitalServiceSuperAdmin, VitalOrganization organization,
			VitalApp app) {
		super(organization, app);
		this.vitalServiceSuperAdmin = vitalServiceSuperAdmin;
	}

	public VitalServiceSuperAdmin getVitalServiceSuperAdmin() {
		return vitalServiceSuperAdmin;
	}

	public void setVitalServiceSuperAdmin(
			VitalServiceSuperAdmin vitalServiceSuperAdmin) {
		this.vitalServiceSuperAdmin = vitalServiceSuperAdmin;
	}

	@Override
	public ResultList get(List<URIProperty> rootURIs)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return vitalServiceSuperAdmin.get(organization, app, GraphContext.ServiceWide, rootURIs);
	}

	@Override
	public ResultList query(VitalSelectQuery rootSelect)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return vitalServiceSuperAdmin.query(organization, app, rootSelect);
	}

	
	
}
