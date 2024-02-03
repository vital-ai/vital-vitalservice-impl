package ai.vital.vitalservice.superadmin.impl;

import java.util.List;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.CommonQueryImplementation;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.impl.query.PathQueryImplementation.PathQueryExecutor;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;

public class SuperAdminServiceQueryImplementation extends CommonQueryImplementation {

	
	private AbstractVitalServiceSuperAdminImplementation superAdminService;

	public SuperAdminServiceQueryImplementation(SystemSegment systemSegment, VitalQuery query,
			AbstractVitalServiceSuperAdminImplementation superAdminService, 
			VitalOrganization organization, VitalApp app) {
		super(systemSegment, query, organization, app);
		this.superAdminService = superAdminService;
	}

	@Override
	protected EndpointType getEndpointType() {
		return superAdminService.getEndpointType();
	}

	@Override
	protected ResultList _graphQuery(VitalGraphQuery graphQuery)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return superAdminService._graphQuery(organization, app, graphQuery);
	}

	@Override
	protected ResultList _selectQuery(VitalSelectQuery query) throws VitalServiceException, VitalServiceUnimplementedException {
		return superAdminService._selectQuery(organization, app, query);
	}

	@Override
	protected ResultList _sparqlQuery(VitalSparqlQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		return superAdminService._sparqlQuery(organization, app, query);
	}

	@Override
	protected void addToCache(ResultList rl) {
		superAdminService.addToCache(rl);
	}

	@Override
	protected PathQueryExecutor createExecutor() {
		return new VitalServiceSuperAdminPathQueryExecutor(superAdminService, organization, app);
	}

	@Override
	protected List<VitalSegment> listSegments() throws VitalServiceUnimplementedException, VitalServiceException {
		return superAdminService.listSegments(organization, app);
	}

}
