package ai.vital.vitalservice.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.query.PathQueryImplementation;
import ai.vital.vitalservice.impl.query.PathQueryImplementation.PathQueryExecutor;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;

public abstract class CommonQueryImplementation implements Callable<ResultList> {

	private final static Logger log = LoggerFactory.getLogger(CommonQueryImplementation.class);
	
	protected SystemSegment systemSegment;
	
	protected VitalQuery query;
	
	protected VitalOrganization organization;
	
	protected VitalApp app;
	
	protected abstract EndpointType getEndpointType();
	
	protected abstract ResultList _graphQuery(VitalGraphQuery graphQuery) throws VitalServiceUnimplementedException, VitalServiceException;
	
	protected abstract ResultList _selectQuery(VitalSelectQuery query) throws VitalServiceException, VitalServiceUnimplementedException;
	
	protected abstract ResultList _sparqlQuery(VitalSparqlQuery query) throws VitalServiceUnimplementedException, VitalServiceException;
	
	protected abstract void addToCache(ResultList rl);
	
	protected abstract PathQueryExecutor createExecutor();
	
	protected abstract List<VitalSegment> listSegments() throws VitalServiceUnimplementedException, VitalServiceException;
	
	public CommonQueryImplementation(SystemSegment systemSegment, VitalQuery query, VitalOrganization organization,
			VitalApp app) {
		super();
		this.systemSegment = systemSegment;
		this.query = query;
		this.organization = organization;
		this.app = app;
	}

	public ResultList handleQuery() {
		
		
		ResultList response = null;

		Integer timeout = query.getTimeout();
		
		if(timeout != null && timeout.intValue() > 0) {
			
			ExecutorService pool = Executors.newSingleThreadExecutor();
			
			Future<ResultList> future = null;
			
			try {
				
				future = pool.submit(this);
				response = future.get(timeout, TimeUnit.SECONDS);
				
			} catch(TimeoutException e) {

				log.warn("Query timed out: {} seconds, query: {}", timeout, query.debugString());
				
			} catch(Exception e) {
				
				response = new ResultList();
				response.setStatus(VitalStatus.withError("Query exception: " + e.getLocalizedMessage()));
				
				if(future != null) {
					try { future.cancel(true); } catch(Exception ex) {}
				}
				
			} finally {
				
				pool.shutdownNow();
				
				this.shutdown();
				
			}
			
			if(response == null) {
				response = new ResultList();
				response.setStatus(VitalStatus.withError("Query timed out - " + timeout + " second" + (timeout != 1 ? "s" : "")));
			}
			
		} else {
			
			try {
				response = call();
			} catch (Exception e) {
				ResultList rl = new ResultList();
				rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
				return rl;
			}
			
		}
		
		return response;

	}

	private void shutdown() {
		
	}

	@Override
	public ResultList call() throws Exception {
		
		if( VitalSegment.isAllSegments(query.getSegments()) ) {
			query.setSegments(listSegments());
			if(query.getSegments().size() == 0) throw new VitalServiceException("No segments available");
		} else {
			
			if(query instanceof VitalGraphQuery || query instanceof VitalSelectQuery || query instanceof VitalPathQuery) {
				
				Map<String, VitalSegment> segmentsMap = systemSegment.listSegmentID2Map(organization, app);
				
				List<VitalSegment> lookedUpList = new ArrayList<VitalSegment>();
				for(VitalSegment s : query.getSegments()) {
					String sid = (String) s.getRaw(Property_hasSegmentID.class);
					VitalSegment x = segmentsMap.get(sid);
					if(x == null) throw new RuntimeException("Segment not found: " + sid);
					lookedUpList.add(x);
				}
				
				if(lookedUpList.size() == 0) throw new RuntimeException("Segments list is empty");
				
				query = (VitalQuery) query.clone();
				
				query.setSegments(lookedUpList);
				
			}
			
		}
	
		if(query instanceof VitalGraphQuery) {
			//check which enpoint type may return sparql
			if(query.isReturnSparqlString()) {
				if(this.getEndpointType() == EndpointType.ALLEGROGRAPH || this.getEndpointType() == EndpointType.INDEXDB || this.getEndpointType() == EndpointType.VITALPRIME) {
				} else {
					throw new VitalServiceException("Endpoint type: " + this.getEndpointType() + " does not return sparql query strings");
				}
			}

//			String localSegment = LocalSegmentsQueryFilter.checkIfLocalQuery(query.getSegments());
			ResultList rl = null;
			//filter removed
//			if(localSegment != null) {
//				if(query.isReturnSparqlString()) {
//					throw new VitalServiceException("Local segment queries do not return sparl query strings");
//				}
//				rl = VitalSigns.get().doGraphQuery(localSegment, (VitalGraphQuery) query);
//			} else {
				rl = _graphQuery((VitalGraphQuery) query);
//			}
			addToCache(rl);
			return rl;
			
		} else if(query instanceof VitalSelectQuery) {
			
			//		ExportQueryFilter.checkExportQuery(query)

			//check which enpoint type may return sparql
			if(query.isReturnSparqlString()) {
				if(this.getEndpointType() == EndpointType.ALLEGROGRAPH || this.getEndpointType() == EndpointType.INDEXDB || this.getEndpointType() == EndpointType.VITALPRIME) {
				} else {
					throw new VitalServiceException("Endpoint type: " + this.getEndpointType() + " does not return sparql query strings");
				}
			}


			//single segment at a time ?
//			String localSegment = LocalSegmentsQueryFilter.checkIfLocalQuery(query.getSegments());
			ResultList rl = null;
			//filter removed
//			if(localSegment != null) {
//				if(query.isReturnSparqlString()) {
//					throw new VitalServiceException("Local segment queries do not return sparl query strings");
//				}
//				rl = VitalSigns.get().doSelectQuery(localSegment, (VitalSelectQuery) query);
//			} else {
				rl = _selectQuery((VitalSelectQuery) query);
//			}
			addToCache(rl);
			return rl;
		} else if(query instanceof VitalSparqlQuery) {
			
			return _sparqlQuery((VitalSparqlQuery)query);
			
		} else if(query instanceof VitalPathQuery) {
			
			return new PathQueryImplementation((VitalPathQuery) query, createExecutor()).execute();
			
		} else {
			throw new VitalServiceUnimplementedException("Unhandled query type: " + query.getClass().getCanonicalName());
		}
		
	}
	
}
