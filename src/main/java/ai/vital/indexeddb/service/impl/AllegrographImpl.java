package ai.vital.indexeddb.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import ai.vital.allegrograph.service.AllegrographSystemSegmentExecutor;
import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig;
import ai.vital.triplestore.allegrograph.AllegrographWrapper;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionID;
import ai.vital.vitalsigns.model.property.URIProperty;

public class AllegrographImpl implements DBInterface {

	VitalServiceAllegrographConfig config;
	
	AllegrographWrapper wrapper;

	@Override
	public VitalStatus initialize(VitalServiceConfig _config) throws IOException {

		if( ! ( _config instanceof VitalServiceAllegrographConfig ) ) throw new IOException("Expected config of class: " + VitalServiceAllegrographConfig.class.getCanonicalName());
		
		this.config = (VitalServiceAllegrographConfig) _config;
		
		wrapper = AllegrographWrapper.create(config.getServerURL(), config.getUsername(), config.getPassword(), config.getCatalogName(), config.getRepositoryName(), config.getPoolMaxTotal());
		try {
			wrapper.open();
		} catch(Exception e) {
			throw new IOException(e);
		}
		
		return VitalStatus.withOK();
		
	}

	protected List<VitalSegment> extractSegments(ResultList segmentsRL) {
		List<VitalSegment> r = new ArrayList<VitalSegment>();
		for(Iterator<VitalSegment> iterator = segmentsRL.iterator(VitalSegment.class, true); iterator.hasNext(); ) {
			r.add(iterator.next());
		}
		return r;
	}
	
	@Override
	public GraphObject save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, GraphObject graphObject, ResultList segmentsRL)
			throws Exception {
		return wrapper.save(transaction, targetSegment, graphObject, extractSegments(segmentsRL));
	}

	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris, ResultList segmentsRL)
			throws Exception {
		return wrapper.delete(transaction, extractSegments(segmentsRL), uris);
	}

	@Override
	public VitalStatus ping() throws Exception {
		return wrapper.ping();
	}

	@Override
	public void close() throws Exception {
		wrapper.close();
	}

	@Override
	public GraphObject get(VitalOrganization organization, VitalApp app, URIProperty uri, ResultList segmentsRL)
			throws Exception {
		return wrapper.get(extractSegments(segmentsRL), uri);
	}

	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, List<GraphObject> graphObjectsList, ResultList segmentsRL)
			throws Exception {
		return wrapper.save(transaction, targetSegment, graphObjectsList, extractSegments(segmentsRL));
	}

	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri, ResultList segmentsRL)
			throws Exception {
		return wrapper.delete(transaction, extractSegments(segmentsRL), uri);
	}

	@Override
	public List<GraphObject> getBatch(VitalOrganization organization, VitalApp app,
			Collection<String> uris, ResultList segmentsRL) throws Exception {
		return wrapper.getBatch(extractSegments(segmentsRL), uris);
	}


	@Override
	public void scanSegment(VitalOrganization organization, VitalApp app, VitalSegment segment,
			int limit, final ScanListener handler, ResultList segmentsRL) throws Exception {
		
		wrapper.scanSegment(segment, limit, new ai.vital.triplestore.allegrograph.AllegrographWrapper.ScanListener(){
			
			public void onBatchReady(List<GraphObject> part){
				handler.onBatchReady(part);
			}
			
			public void onScanComplete() {
				handler.onScanComplete();
			}
			
		});
		
	}

	@Override
	public int getSegmentSize(VitalOrganization organization, VitalApp app, VitalSegment segment, ResultList segmentsRL) throws Exception {
		return wrapper.getSegmentSize(segment);
	}

	@Override
	public boolean supportsSelectQuery(VitalOrganization organization, VitalApp app, VitalSelectQuery sq) throws Exception {
		//export, aggregation functions and regular
		return true;
	}

	@Override
	public boolean supportsGraphQuery(VitalOrganization organization, VitalApp app, VitalGraphQuery gq) throws Exception {
		//sparql and regular
		return true;
	}

	@Override
	public ResultList selectQuery(VitalOrganization organization, VitalApp app,
			VitalSelectQuery query, ResultList segmentsRL) throws Exception {
		return wrapper.selectQuery(query);
	}

	@Override
	public ResultList graphQuery(VitalOrganization organization, VitalApp app,
			VitalGraphQuery query, ResultList segmentsRL) throws Exception {
		return wrapper.graphQuery(query);
	}

	public ResultList insertRDFStatements(VitalTransaction transaction, List<GraphObject> stmts) throws Exception {
		return wrapper.insertRDFStatements(transaction, stmts);
	}
		
			
	public VitalStatus deleteRDFStatements(VitalTransaction transaction, List<GraphObject> stmts) throws Exception {
		return wrapper.deleteRDFStatements(transaction, stmts);
	}

	@Override
	public void commitTransaction(VitalTransaction transaction, ResultList segmentsRL) throws Exception {
		wrapper.commitTransation((String) transaction.get(Property_hasTransactionID.class));
	}

	@Override
	public String createTransaction() throws Exception {
		return wrapper.createTransaction();
	}

	@Override
	public void rollbackTransaction(VitalTransaction transaction) throws Exception {
		wrapper.rollbackTransaction((String) transaction.get(Property_hasTransactionID.class));		
	}

	@Override
	public ResultList sparqlQuery(VitalOrganization organization, VitalApp app,
			VitalSparqlQuery query, ResultList segmentsRL) throws Exception {
		return wrapper.sparqlQuery(query);
	}

	@Override
	public VITAL_GraphContainerObject getExistingObjects(
			VitalOrganization organization, VitalApp app, List<String> uris, ResultList segmentsRL) throws Exception {
		return wrapper.getExistingObjects(extractSegments(segmentsRL), uris);
	}

	@Override
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app,
			VitalSegment segment, OutputStream outputStream, ResultList segmentsRL, String datasetURI) throws Exception {
		return wrapper.bulkExport(segment, outputStream, datasetURI);
	}

	@Override
	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app,
			VitalSegment segment, InputStream inputStream, ResultList segmentsRL, String datasetURI) throws Exception {
		return wrapper.bulkImportBlockCompact(segment, inputStream, datasetURI);
	}

	@Override
	public SystemSegmentOperationsExecutor getSystemSegmentExecutor() {
		return new AllegrographSystemSegmentExecutor(wrapper);
	}

	@Override
	public void deleteAll(VitalOrganization organization, VitalApp app,
			VitalSegment segment, ResultList segmentsRL) throws Exception {
		wrapper.deleteAll(segment);
	}

	@Override
	public void transactionsCheck() throws VitalServiceUnimplementedException {}

}
