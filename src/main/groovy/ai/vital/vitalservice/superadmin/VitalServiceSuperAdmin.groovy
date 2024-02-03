package ai.vital.vitalservice.superadmin

import java.io.InputStream
import java.io.OutputStream
import java.util.List
import java.util.Map

import ai.vital.vitalservice.EndpointType
//import ai.vital.vitalservice.Transaction
import ai.vital.vitalsigns.model.property.URIProperty
import ai.vital.vitalservice.ServiceOperations
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalsigns.meta.GraphContext
import ai.vital.vitalservice.exception.VitalServiceException
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalservice.query.VitalPathQuery
import ai.vital.vitalservice.query.VitalQuery
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalsigns.model.DatabaseConnection
import ai.vital.vitalsigns.model.VITAL_Event
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalOrganization
import ai.vital.vitalsigns.model.VitalProvisioning
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey
import ai.vital.vitalsigns.model.VitalTransaction
import ai.vital.vitalsigns.model.container.GraphObjectsIterable;

interface VitalServiceSuperAdmin {

	// use call function for this...
	//public VitalStatus registerOntology(InputStream owl, String namespace, String _package)
	
	// info about service connection
	
	public EndpointType getEndpointType()
	
	// connection status
	
	public VitalStatus validate() throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus ping() throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus close() throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// service containers: segments, apps, organizations
	
	public List<VitalSegment> listSegments(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException
	
	public ResultList listSegmentsWithConfig(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalSegment addSegment(VitalOrganization organization, VitalApp app, VitalSegment config, boolean createIfNotExists) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalSegment addSegment(VitalOrganization organization, VitalApp app, VitalSegment config, VitalProvisioning provisioning, boolean createIfNotExists) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus removeSegment(VitalOrganization organization, VitalApp app, VitalSegment segment, boolean deleteData) throws VitalServiceUnimplementedException, VitalServiceException

	public List<VitalApp> listApps(VitalOrganization organization) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus addApp(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus removeApp(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// URIs
	
	public URIProperty generateURI(VitalOrganization organization, VitalApp app, Class<? extends GraphObject> clazz) throws VitalServiceUnimplementedException, VitalServiceException
		
	// Transactions
	
	public VitalTransaction createTransaction() throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus commitTransaction(VitalTransaction transaction) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus rollbackTransaction(VitalTransaction transaction) throws VitalServiceUnimplementedException, VitalServiceException
	
	public List<VitalTransaction> getTransactions() throws VitalServiceUnimplementedException, VitalServiceException
	
	
	
	public VitalStatus addOrganization(VitalOrganization organization) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus removeOrganization(VitalOrganization organization) throws VitalServiceUnimplementedException, VitalServiceException
	
	public List<VitalOrganization> listOrganizations() throws VitalServiceUnimplementedException, VitalServiceException
	
	
	
	// crud operations
	
	// get
	
	// default cache=true, for consistency always include GraphContext
	
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException
	
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, URIProperty uri, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException
	
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, List<URIProperty> uris) throws VitalServiceUnimplementedException, VitalServiceException
	
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, List<URIProperty> uris, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// GraphContext is redundant here as this call only used for containers, but kept for consistency
	
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, URIProperty uri, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException
	
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, List<URIProperty> uris, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// delete
	public VitalStatus delete(VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus delete(VitalOrganization organization, VitalApp app, List<URIProperty> uris) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris) throws VitalServiceUnimplementedException, VitalServiceException

	public VitalStatus deleteObject(VitalOrganization organization, VitalApp app, GraphObject object) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus deleteObject(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject object) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus deleteObjects(VitalOrganization organization, VitalApp app, List<GraphObject> objects) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus deleteObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> objects) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// insert, must be a new object
	
	public ResultList insert(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject) throws VitalServiceUnimplementedException, VitalServiceException
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject) throws VitalServiceUnimplementedException, VitalServiceException
	
	public ResultList insert(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// save, if create=true, create a new object if it doesn't exist
	
	public ResultList save(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject, boolean create) throws VitalServiceUnimplementedException, VitalServiceException
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject, boolean create) throws VitalServiceUnimplementedException, VitalServiceException

	public ResultList save(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList, boolean create) throws VitalServiceUnimplementedException, VitalServiceException
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList, boolean create) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// object(s) must already exist, determine current segment & update
	
	public ResultList save(VitalOrganization organization, VitalApp app, GraphObject graphObject) throws VitalServiceUnimplementedException, VitalServiceException
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject graphObject) throws VitalServiceUnimplementedException, VitalServiceException
	
	public ResultList save(VitalOrganization organization, VitalApp app, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException
		
	
	public VitalStatus doOperations(VitalOrganization organization, VitalApp app, ServiceOperations operations) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// Functions
	
	public ResultList callFunction(VitalOrganization organization, VitalApp app, String function, Map<String, Object> arguments) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// Query + Expand
	
	// ServiceWide
	public ResultList query(VitalOrganization organization, VitalApp app, VitalQuery query) throws VitalServiceUnimplementedException, VitalServiceException
	
	// Local
	public ResultList queryLocal(VitalOrganization organization, VitalApp app, VitalQuery query) throws VitalServiceUnimplementedException, VitalServiceException
	
	// Containers
	public ResultList queryContainers(VitalOrganization organization, VitalApp app, VitalQuery query, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException
	
	// ServiceWide
	public ResultList getExpanded(VitalOrganization organization, VitalApp app, URIProperty uri, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException

	
	// path query determines segments list
	// there can be a prepared default path query helper that does the same as expand
	// i.e. VitalPathQuery.getDefaultExpandQuery(List<VitalSegment> segments, VitalSelectQuery rootselector) --> VitalPathQuery
	// for the case to query against containers or local, no segments specified:
	// VitalPathQuery.getDefaultExpandQuery(VitalSelectQuery rootselector) --> VitalPathQuery
	
	public ResultList getExpanded(VitalOrganization organization, VitalApp app, URIProperty uri, VitalPathQuery query, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus deleteExpandedObject(VitalOrganization organization, VitalApp app, GraphObject object) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus deleteExpandedObject(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject object) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, URIProperty uri, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, List<URIProperty> uris, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus deleteExpandedObjects(VitalOrganization organization, VitalApp app, List<GraphObject> objects, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException
	public VitalStatus deleteExpandedObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> objects, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException
	
	// Import and Export
	
	/**
	 * Imports the data in bulk mode.
	 * In case of indexeddb endpoint it only writes data to database, index is untouched (needs refresh).
	 * Import fails if there's any active transaction.
	 * @param organization
	 * @param app
	 * @param segment
	 * @param inputStream
	 * @return operation status
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream) throws VitalServiceUnimplementedException, VitalServiceException
	
	/**
	 * Imports the data in bulk mode.
	 * In case of indexeddb endpoint it only writes data to database, index is untouched (needs refresh).
	 * Import fails if there's any active transaction.
	 * @param organization
	 * @param app
	 * @param segment
	 * @param inputStream
	 * @param datasetURI, null - no dataset, empty string - don't set provenance, other value set it
	 * @return operation status
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream, String datasetURI) throws VitalServiceUnimplementedException, VitalServiceException
	
	/**
	 * Exports the data in bulk mode.
	 * Export fails if there's any active transaction.
	 * @param organization
	 * @param app
	 * @param segment
	 * @param outputStream
	 * @return operation status
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream) throws VitalServiceUnimplementedException, VitalServiceException
	
	/**
	 * Exports the data in bulk mode.
	 * Export fails if there's any active transaction.
	 * @param organization
	 * @param app
	 * @param segment
	 * @param outputStream
	 * @param datasetURI, null - no dataset
	 * @return operation status
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream, String datasetURI) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// Events
	
	public VitalStatus sendEvent(VitalOrganization organization, VitalApp app, VITAL_Event event, boolean waitForDelivery) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus sendEvents(VitalOrganization organization, VitalApp app, List<VITAL_Event> events, boolean waitForDelivery) throws VitalServiceUnimplementedException, VitalServiceException
			
	
	// Files
	
	public VitalStatus uploadFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName, InputStream inputStream, boolean overwrite) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus downloadFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName, OutputStream outputStream, boolean closeOutputStream) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus fileExists(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus deleteFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName) throws VitalServiceUnimplementedException, VitalServiceException
	
	public ResultList listFiles(VitalOrganization organization, VitalApp app, String filepath) throws VitalServiceUnimplementedException, VitalServiceException
	

	//named databases
	public ResultList listDatabaseConnections(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus addDatabaseConnection(VitalOrganization organization, VitalApp app, DatabaseConnection connection) throws VitalServiceUnimplementedException, VitalServiceException
	
	public VitalStatus removeDatabaseConnection(VitalOrganization organization, VitalApp app, String databaseName) throws VitalServiceUnimplementedException, VitalServiceException

	

	
    /**
     * @return name of this service instance
     */
    public String getName()

    /**
     * Returns a segment with given ID or <code>null</code> if not found
     * @param organization
     * @param app
     * @param segmentID
     * @return
     * @throws VitalServiceUnimplementedException
     * @throws VitalServiceException
     */
    public VitalSegment getSegment(VitalOrganization organization, VitalApp app, String segmentID) throws VitalServiceUnimplementedException, VitalServiceException
    
    /**
     * Returns an app with given ID or <code>null</code> if not found
     * @param organization
     * @param appID
     * @return
     * @throws VitalServiceUnimplementedException
     * @throws VitalServiceException
     */
    public VitalApp getApp(VitalOrganization organization, String appID) throws VitalServiceUnimplementedException, VitalServiceException
	
    /**
     * Returns an organization with given ID or <code>null</code> if not found
     * @param organizationID
     * @return
     * @throws VitalServiceUnimplementedException
     * @throws VitalServiceException
     */
    public VitalOrganization getOrganization(String organizationID) throws VitalServiceUnimplementedException, VitalServiceException

	
	
	/**
	 * Adds a new vitalservice key
	 * @param organization
	 * @param app
	 * @param serviceKey
	 * @return
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public VitalStatus addVitalServiceKey(VitalOrganization organization, VitalApp app, VitalServiceKey serviceKey) throws VitalServiceUnimplementedException, VitalServiceException
  
	/**
	 * Removes a vitalservice key
	 * @param organization
	 * @param app
	 * @param serviceKey
	 * @return
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public VitalStatus removeVitalServiceKey(VitalOrganization organization, VitalApp app, VitalServiceKey serviceKey) throws VitalServiceUnimplementedException, VitalServiceException

	/**
	 * Lists vitalservice keys
	 * @param organization
	 * @param app
	 * @return
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public List<VitalServiceKey> listVitalServiceKeys(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException

	
	
	/**
	 * Adds a new vitalservice admin key 
	 * @param organization
	 * @param serviceAdminKey
	 * @return
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public VitalStatus addVitalServiceAdminKey(VitalOrganization organization, VitalServiceAdminKey serviceAdminKey) throws VitalServiceUnimplementedException, VitalServiceException
	
	/**
	 * Removes a vitalservice admin key
	 * @param organization
	 * @param serviceAdminKey
	 * @return
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public VitalStatus removeVitalServiceAdminKey(VitalOrganization organization, VitalServiceAdminKey serviceAdminKey) throws VitalServiceUnimplementedException, VitalServiceException
	
	/**
	 * Lists vitalservice admin keys
	 * @param organization
	 * @return
	 * @throws VitalServiceUnimplementedException
	 * @throws VitalServiceException
	 */
	public List<VitalServiceAdminKey> listVitalServiceAdminKeys(VitalOrganization organization) throws VitalServiceUnimplementedException, VitalServiceException
}


