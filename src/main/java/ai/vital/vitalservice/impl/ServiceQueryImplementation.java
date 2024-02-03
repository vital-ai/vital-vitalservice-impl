package ai.vital.vitalservice.impl;

import java.util.List;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.query.PathQueryImplementation.PathQueryExecutor;
import ai.vital.vitalservice.impl.query.PathQueryImplementation.VitalServicePathQueryExecutor;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.VitalSegment;

class ServiceQueryImplementation extends CommonQueryImplementation {

	AbstractVitalServiceImplementation service;

	

	public ServiceQueryImplementation(SystemSegment systemSegment, VitalQuery query, AbstractVitalServiceImplementation service) {
		super(systemSegment, query, service.getOrganization(), service.getApp());
		this.service = service;
	}

	@Override
	protected EndpointType getEndpointType() {
		return service.getEndpointType();
	}

	@Override
	protected ResultList _graphQuery(VitalGraphQuery graphQuery) throws VitalServiceUnimplementedException, VitalServiceException {
		return service._graphQuery(graphQuery);
	}

	@Override
	protected ResultList _selectQuery(VitalSelectQuery query) throws VitalServiceException, VitalServiceUnimplementedException {
		return service._selectQuery(query);
	}

	@Override
	protected ResultList _sparqlQuery(VitalSparqlQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		return service._sparqlQuery(query);
	}

	@Override
	protected void addToCache(ResultList rl) {
		service.addToCache(rl);
	}

	@Override
	protected PathQueryExecutor createExecutor() {
		return new VitalServicePathQueryExecutor(service);
	}

	@Override
	protected List<VitalSegment> listSegments() throws VitalServiceUnimplementedException, VitalServiceException {
		return service.listSegments();
	}

}
