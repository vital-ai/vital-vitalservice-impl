package ai.vital.indexeddb.service.impl;

import static ai.vital.vitalservice.VitalServiceConstants.NO_TRANSACTION;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import ai.vital.lucene.common.service.LuceneSystemSegmentExecutor;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.service.lucene.impl.LuceneServiceDiskImpl;
import ai.vital.vitalservice.DeleteOperation;
import ai.vital.vitalservice.InsertOperation;
import ai.vital.vitalservice.TransactionOperation;
import ai.vital.vitalservice.UpdateOperation;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.RDFStatement;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.property.URIProperty;

public class LuceneDiskImpl implements IndexInterface {

	private LuceneServiceDiskImpl impl;
	
	@Override
	public VitalStatus initialize(VitalServiceConfig config) throws IOException {

		if(!(config instanceof VitalServiceLuceneDiskConfig)) throw new IOException("Expected instance of " + VitalServiceLuceneDiskConfig.class.getCanonicalName());
		
		VitalServiceLuceneDiskConfig cfg = (VitalServiceLuceneDiskConfig) config;
		
		impl = LuceneServiceDiskImpl.create(new File(cfg.getRootPath()), cfg.getBufferWrites(), cfg.getCommitAfterNWrites(), cfg.getCommitAfterNSeconds());
		
		try {
			impl.open();
		} catch (LuceneException e) {
			throw new IOException(e);
		}
		
		return VitalStatus.OK;
	}

	@Override
	public VitalStatus ping() throws Exception {
		return VitalStatus.OK;
	}

	List<VitalSegment> extractSegments(ResultList segmentsRL) {
		List<VitalSegment> r = new ArrayList<VitalSegment>();
		for( Iterator<VitalSegment> iterator = segmentsRL.iterator(VitalSegment.class, true); iterator.hasNext(); ) {
			r.add(iterator.next());
		}
		return r;
	}
	
	@Override
	public GraphObject save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, GraphObject graphObject, ResultList segmentsRL)
			throws Exception {

		if(graphObject instanceof RDFStatement)	{
			//rdf statements not indexed
			return graphObject;
		}
		
		return impl.save(targetSegment, graphObject, extractSegments(segmentsRL));
	}

	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, List<GraphObject> graphObjectsList, ResultList segmentsRL)
			throws Exception {
		for(GraphObject g : graphObjectsList) {
			if(g instanceof RDFStatement) {
				ResultList rl = new ResultList();
				rl.setStatus(VitalStatus.withOKMessage("rdf statements not indexed"));
				return rl;
			}
		}
		return impl.save(targetSegment, graphObjectsList, extractSegments(segmentsRL));
	}

	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri, ResultList segmentsRL)
			throws Exception {
		return impl.delete(extractSegments(segmentsRL), uri, true);
	}

	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris, ResultList segmentsRL)
			throws Exception {
		return impl.delete(extractSegments(segmentsRL), uris);
	}

	@Override
	public void close() throws Exception {
		impl.close();
	}

	@Override
	public ResultList selectQuery(VitalOrganization organization, VitalApp app, VitalSelectQuery query, ResultList segmentsRL)
			throws Exception {
		return impl.selectQuery(query);
	}

	@Override
	public ResultList graphQuery(VitalOrganization organization, VitalApp app,
			VitalGraphQuery query, ResultList segmentsRL) throws Exception {
		return impl.graphQuery(organization, app, query);
	}


	@Override
	public void deleteAll(VitalOrganization organization, VitalApp app, VitalSegment segment, ResultList segmentsRL)
			throws Exception {
		impl.deleteAll(segment);
	}

	@Override
	public int getSegmentSize(VitalOrganization organization, VitalApp app, VitalSegment segment, ResultList segmentsRL) throws Exception {
		return impl.getSegmentSize(segment);
	}

	@Override
	public boolean supportsSelectQuery(VitalOrganization organization, VitalApp app, VitalSelectQuery sq) throws Exception {

		//don't forward export queries!
		if(sq instanceof VitalExportQuery) {
			return false;
		}
		
		if(sq.getReturnSparqlString()) {
//			throw new Exception("Lucene does not return sparql query strings")
			//don't forward it if we're just asking for sparql string
			return false;
		}
		return impl.supportsSelectQuery(organization, app, sq);
		
	}		

	@Override
	public boolean supportsGraphQuery(VitalOrganization organization, VitalApp app, VitalGraphQuery gq) throws Exception {
		
		//XXX never!
		return  false;
//		return impl.supportsGraphQuery(organization, app, gq)
		
	}

	@Override
	public void synchronizeTransaction(TransactionWrapper transaction, ResultList segmentsRL)
			throws Exception {

		Set<String> segmentsToForceCommit = impl.isBufferWrites() ? new HashSet<String>() : null;

//		Set<String> segmentsToForce
		for( TransactionOperation op : transaction.operations ) {
		
			if(op.getOrganization() == null) throw new RuntimeException("No organization set in an operation");
			if(op.getApp() == null) throw new RuntimeException("No app set in an operation");
				
			GraphObject toSave = null;
			
			VitalSegment segment = null;
			
			URIProperty toDelete = null;
			
			if(op instanceof UpdateOperation) {
				
				toSave = ((UpdateOperation)op).getGraphObject();
				
				segment = ((UpdateOperation)op).getSegment();
				
			} else if(op instanceof InsertOperation) {
			
				toSave = ((InsertOperation)op).getGraphObject();
				
				segment = ((InsertOperation)op).getSegment();
			
			} else if(op instanceof DeleteOperation) {
			
				toDelete = URIProperty.withString( ((DeleteOperation)op).getGraphObjectURI().get() );
				
			} else throw new RuntimeException("Unknown operation: " + op.getClass().getCanonicalName());
			
			
			if(toSave != null) {
				
				if(segment == null) throw new RuntimeException("No target segment to save an object: " + toSave);
				
				save(NO_TRANSACTION, op.getOrganization(), op.getApp(), segment, toSave, segmentsRL);
				
			} else if(toDelete != null) {

				delete(NO_TRANSACTION, op.getOrganization(), op.getApp(), toDelete, segmentsRL);
			
			} else {
				throw new RuntimeException("Not enough data in a transaction operation");
			}
			
			if(segment != null && segmentsToForceCommit != null) segmentsToForceCommit.add(segment.getURI());
			
		}
		
		if(segmentsToForceCommit != null) {
			for(String segmentURI: segmentsToForceCommit) {
				impl.forceCommit(segmentURI);
			}
		}
		
	}

	@Override
	public SystemSegmentOperationsExecutor getSystemSegmentExecutor() {
		return new LuceneSystemSegmentExecutor(impl);
	}

	@Override
	public void insertNoCheck(VitalOrganization organization, VitalApp app,
			VitalSegment segment, List<GraphObject> objects,
			ResultList listSegments) {
		try {
			impl.save(segment, objects, Arrays.asList(segment));
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		
	}

}
