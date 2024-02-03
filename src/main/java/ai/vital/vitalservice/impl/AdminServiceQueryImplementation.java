
package ai.vital.vitalservice.impl;

import java.util.List;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.query.PathQueryImplementation.PathQueryExecutor;
import ai.vital.vitalservice.impl.query.PathQueryImplementation.VitalServiceAdminPathQueryExector;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalSegment;

public class AdminServiceQueryImplementation extends CommonQueryImplementation {

	AbstractVitalServiceAdminImplementation adminService;

	
	
	public AdminServiceQueryImplementation(SystemSegment systemSegment, VitalQuery query,
			AbstractVitalServiceAdminImplementation adminService, VitalApp app) {
		super(systemSegment, query, adminService.getOrganization(), app);
		this.adminService = adminService;
	}

	@Override
	protected EndpointType getEndpointType() {
		return this.adminService.getEndpointType();
	}

	@Override
	protected ResultList _graphQuery(VitalGraphQuery graphQuery)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.adminService._graphQuery(app, graphQuery);
	}

	@Override
	protected ResultList _selectQuery(VitalSelectQuery query) throws VitalServiceException, VitalServiceUnimplementedException {
		return this.adminService._selectQuery(app, query);
	}

	@Override
	protected ResultList _sparqlQuery(VitalSparqlQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.adminService._sparqlQuery(organization, app, query);
	}

	@Override
	protected void addToCache(ResultList rl) {
		adminService.addToCache(rl);
	}

	@Override
	protected PathQueryExecutor createExecutor() {
		return new VitalServiceAdminPathQueryExector(adminService, app);
	}

	@Override
	protected List<VitalSegment> listSegments() throws VitalServiceUnimplementedException, VitalServiceException {
		return adminService.listSegments(app);
	}

}
