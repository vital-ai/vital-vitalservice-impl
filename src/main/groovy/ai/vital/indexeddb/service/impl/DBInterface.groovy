package ai.vital.indexeddb.service.impl

import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalOrganization
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalTransaction


// interface for db implementations

interface DBInterface extends CommonInterface {

	// do setup
	VitalStatus initialize(VitalServiceConfig config) throws IOException
	
	public GraphObject get(VitalOrganization organization, VitalApp app, URIProperty uri, ResultList segmentsRL) throws Exception
	
	public List<GraphObject> getBatch(VitalOrganization organization, VitalApp app, Collection<String> uris, ResultList segmentsRL) throws Exception 

	public void scanSegment(VitalOrganization organization, VitalApp app, VitalSegment segment, int limit, ScanListener handler, ResultList segmentsRL) throws Exception 
	
	public static interface ScanListener {
		
		public void onBatchReady(List<GraphObject> part);
		
		public void onScanComplete();
		
	}
	
	
	/**
	 * Should throw an exception if transactions not supported
	 * @param transaction
	 */
	public void commitTransaction(VitalTransaction transaction, ResultList segmentsRL) throws Exception;
	

	/**
	 * Creates a new transaction, may throw errors
	 * @return
	 * @throws Exception 
	 */
	public String createTransaction() throws Exception;
	
	public void rollbackTransaction(VitalTransaction transaction) throws Exception;
	
	public ResultList sparqlQuery(VitalOrganization organization, VitalApp app, VitalSparqlQuery query, ResultList segmentsRL) throws Exception;

	public VITAL_GraphContainerObject getExistingObjects(VitalOrganization organization, VitalApp app, List<String> uris, ResultList segmentsRL) throws Exception ;
	
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream, ResultList segmentsRL, String datasetURI) throws Exception;

	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream, ResultList segmentsRL, String datasetURI) throws Exception;

	void transactionsCheck() throws VitalServiceUnimplementedException;
	
}
