package ai.vital.indexeddb.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import ai.vital.sql.VitalSqlImplementation;
import ai.vital.sql.VitalSqlImplementation.ScanHandler;
import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.service.VitalServiceSql;
import ai.vital.sql.service.VitalSqlSystemSegmentExecutor;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
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

public class SqlImpl implements DBInterface {

	VitalSqlImplementation impl;
	
	@Override
	public GraphObject save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, GraphObject graphObject, ResultList segmentsRL)
			throws Exception {
		return impl.save(transaction, targetSegment, targetSegment, extractSegments(segmentsRL));
	}

	private List<VitalSegment> extractSegments(ResultList segmentsRL) {
		List<VitalSegment> r = new ArrayList<VitalSegment>();
		for( Iterator<VitalSegment> iterator = segmentsRL.iterator(VitalSegment.class, true); iterator.hasNext(); ) {
			r.add(iterator.next());
		}
		return r;
	}

	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, List<GraphObject> graphObjectsList, ResultList segmentsRL)
			throws Exception {
		return impl.save(transaction, targetSegment, graphObjectsList, extractSegments(segmentsRL));
	}

	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			URIProperty uri, ResultList segmentsRL) throws Exception {
		return impl.delete(transaction, extractSegments(segmentsRL), uri);
	}

	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			List<URIProperty> uris, ResultList segmentsRL) throws Exception {
		return impl.delete(transaction, extractSegments(segmentsRL), uris);
	}

	@Override
	public VitalStatus ping() throws Exception {
		impl.ping();
		return VitalStatus.withOK();
	}

	@Override
	public void close() throws Exception {
		impl.close();
	}

	@Override
	public int getSegmentSize(VitalOrganization organization, VitalApp app, VitalSegment segment, ResultList segmentsRL) throws Exception {
		return impl.getSegmentSize(segment);
	}

	@Override
	public boolean supportsSelectQuery(VitalOrganization organization, VitalApp app,
			VitalSelectQuery sq) throws Exception {
		return true;
	}

	@Override
	public boolean supportsGraphQuery(VitalOrganization organization, VitalApp app,
			VitalGraphQuery gq) throws Exception {
		return true;
	}

	@Override
	public ResultList selectQuery(VitalOrganization organization, VitalApp app,
			VitalSelectQuery query, ResultList segmentsRL) throws Exception {
		return impl.selectQuery(query);
	}

	@Override
	public ResultList graphQuery(VitalOrganization organization, VitalApp app,
			VitalGraphQuery query, ResultList segmentsRL) throws Exception {
		return impl.graphQuery(query);
	}

	@Override
	public VitalStatus initialize(VitalServiceConfig config) throws IOException {
		if(!(config instanceof VitalServiceSqlConfig)) throw new IOException("Expected config of class: " + VitalServiceSqlConfig.class.getCanonicalName());
		try {
			VitalSqlDataSource dataSource = new VitalSqlDataSource(VitalServiceSql.toInnerConfig((VitalServiceSqlConfig) config));
			impl = new VitalSqlImplementation(dataSource);
			impl.open();
		} catch (Exception e) {
			throw new IOException(e);
		}
		return VitalStatus.withOK();
	}

	@Override
	public GraphObject get(VitalOrganization organization, VitalApp app, URIProperty uri, ResultList segmentsRL)
			throws Exception {
		return impl.get(extractSegments(segmentsRL), uri);
	}

	@Override
	public List<GraphObject> getBatch(VitalOrganization organization, VitalApp app,
			Collection<String> uris, ResultList segmentsRL) throws Exception {
		return impl.getBatch(extractSegments(segmentsRL), uris);
	}

	@Override
	public void scanSegment(VitalOrganization organization, VitalApp app,
			VitalSegment segment, int limit, final ScanListener handler, ResultList segmentsRL)
			throws Exception {
		ScanHandler scanHandler = new ScanHandler() {
			
			@Override
			public void onResultsPage(List<GraphObject> objects) {
				handler.onBatchReady(objects);
			}
			
			@Override
			public void onComplete() {
				handler.onScanComplete();
			}
		};
		impl.scan(segment, limit, scanHandler);
	}

	@Override
	public void commitTransaction(VitalTransaction transaction, ResultList segmentsRL) throws Exception {
		impl.commitTransaction((String) transaction.getRaw(Property_hasTransactionID.class));
	}

	@Override
	public String createTransaction() {
		return impl.createTransaction();
	}

	@Override
	public void rollbackTransaction(VitalTransaction transaction) throws Exception {
		impl.rollbackTransaction((String) transaction.getRaw(Property_hasTransactionID.class));
	}

	@Override
	public ResultList sparqlQuery(VitalOrganization organization, VitalApp app,
			VitalSparqlQuery query, ResultList segmentsRL) throws Exception {
		throw new Exception("Sparql queries unsupported.");
	}

	@Override
	public VITAL_GraphContainerObject getExistingObjects(
			VitalOrganization organization, VitalApp app, List<String> uris, ResultList segmentsRL)
			throws Exception {
		return impl.getExistingObjects(extractSegments(segmentsRL), uris);
	}

	@Override
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app,
			VitalSegment segment, OutputStream outputStream, ResultList segmentsRL, String datasetURI) throws Exception {
		return impl.bulkExport(segment, outputStream, datasetURI);
	}

	@Override
	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app,
			VitalSegment segment, InputStream inputStream, ResultList segmentsRL, String datasetURI) throws Exception {
		return impl.bulkImport(segment, inputStream, datasetURI);
	}

	@Override
	public void deleteAll(VitalOrganization organization, VitalApp app,
			VitalSegment segment, ResultList segmentsRL) throws Exception {
		impl.deleteAll(segment);
	}

	@Override
	public SystemSegmentOperationsExecutor getSystemSegmentExecutor() {
		return new VitalSqlSystemSegmentExecutor(impl);
	}

	@Override
	public void transactionsCheck() throws VitalServiceUnimplementedException {}

}
