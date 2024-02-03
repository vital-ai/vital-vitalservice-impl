package ai.vital.prime.client

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.ServiceOperations;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalQuery
import ai.vital.vitalsigns.model.DatabaseConnection;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Event;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalAuthKey;
import ai.vital.vitalsigns.model.VitalOrganization
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.VitalTransaction

interface IVitalPrimeClient {

	/**
	 * main entry point method, it should be only called once as at the beginning in order to obtain VitalSession object with organizations and apps info etc
	 * It maintains the sessionID internally
	 * @param authKey
	 * @return VitalSession with VitalOrganization (always) and VitalApp (if regular service)
	 * @throws VitalServiceException
	 * @throws VitalServiceUnimplementedException
	 */
	public ResultList authenticate(VitalAuthKey authKey) throws VitalServiceException, VitalServiceUnimplementedException;
	
	/**
	 * It should be called 
	 * @return
	 * @throws VitalServiceException
	 * @throws VitalServiceUnimplementedException
	 */
	public VitalStatus closeSession() throws VitalServiceException, VitalServiceUnimplementedException;
	
	
	/**
	 * Closes the session on remote side - only for tests;
	 */
	public VitalStatus testBrokenSession() throws VitalServiceException, VitalServiceUnimplementedException;
	
	// methods specific to Super Admin of the Vital Service
	public ResultList callFunction(VitalOrganization organization, VitalApp app, String function, Map<String, Object> arguments) throws VitalServiceException, VitalServiceUnimplementedException;
	
	public URIProperty generateURI(VitalOrganization organization, VitalApp app, Class<? extends GraphObject> clazz) throws VitalServiceException, VitalServiceUnimplementedException;
	
	public GraphObject get(VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceException,  VitalServiceUnimplementedException
	
	public List<GraphObject> getBatch(VitalOrganization organization, VitalApp app, List<URIProperty> uris) throws VitalServiceException,  VitalServiceUnimplementedException
	
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject, boolean create) throws VitalServiceException, VitalServiceUnimplementedException
	
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList, boolean create) throws VitalServiceException, VitalServiceUnimplementedException
	
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject) throws VitalServiceException, VitalServiceUnimplementedException
	
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList) throws VitalServiceException, VitalServiceUnimplementedException
	
	
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus deleteObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> objects) throws VitalServiceException, VitalServiceUnimplementedException
	
	public ResultList query(VitalOrganization organization, VitalApp app, VitalQuery query) throws VitalServiceException, VitalServiceUnimplementedException
	
	public List<VitalSegment> listSegments(VitalOrganization organization, VitalApp app) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus sendFlumeEvent(VitalOrganization organization, VitalApp app, VITAL_Event event, boolean waitForDelivery) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus sendFlumeEvents(VitalOrganization organization, VitalApp app, List<VITAL_Event> event, boolean waitForDelivery) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus uploadFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName, InputStream inputStream, boolean overwrite) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus downloadFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName, OutputStream outputStream, boolean closeOutputStream) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus fileExists(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus deleteFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus ping() throws VitalServiceException, VitalServiceUnimplementedException
	
	
	public VitalSegment addSegment(VitalOrganization organization, VitalApp app, VitalSegment config, VitalProvisioning provisioning, boolean createIfNotExists) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus removeSegment(VitalOrganization organization, VitalApp app, VitalSegment segment, boolean deleteData) throws VitalServiceException, VitalServiceUnimplementedException
	
	
	
	public List<VitalApp> listApps(VitalOrganization organization) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus addApp(VitalOrganization organization, VitalApp app) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus removeApp(VitalOrganization organization, VitalApp app) throws VitalServiceException, VitalServiceUnimplementedException
	
	
	
	// create/remove/update/list organizations
	
	public VitalOrganization addOrganization(VitalOrganization organization) throws VitalServiceException, VitalServiceUnimplementedException
	
	public VitalStatus removeOrganization(VitalOrganization organization) throws VitalServiceException, VitalServiceUnimplementedException
	
	public List<VitalOrganization> listOrganizations() throws VitalServiceException, VitalServiceUnimplementedException

	//remote shutdown, vitalprime may ignore such requests
	public VitalStatus shutdown();
	
	public List<GraphObject> listDatascripts(VitalOrganization organization, VitalApp app, String path, boolean getBodies) throws VitalServiceException;
	
	public GraphObject addDatascript(VitalOrganization organization, VitalApp app, String path, String body) throws VitalServiceException;

	public VitalStatus removeDatascript(VitalOrganization organization, VitalApp app, String path) throws VitalServiceException;
	
	
	//transactions
	public VitalStatus commitTransaction(VitalTransaction transaction) throws VitalServiceException;
	
	public VitalTransaction createTransaction() throws VitalServiceException;
	
	public List<VitalTransaction> getTransactions() throws VitalServiceException;

	public VitalStatus rollbackTransaction(VitalTransaction transaction);
	
	
	public VitalOrganization getOrganization(String organizationID) throws VitalServiceException, VitalServiceUnimplementedException;
	
	public VitalApp getApp(VitalOrganization organization, String appID) throws VitalServiceException, VitalServiceUnimplementedException;
	
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream, String datasetURI) throws VitalServiceException, VitalServiceUnimplementedException;
	
	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream, String datasetURI) throws VitalServiceException, VitalServiceUnimplementedException;

	
	//named databases
    public ResultList listDatabaseConnections(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException;
	
	public VitalStatus addDatabaseConnection(VitalOrganization organization, VitalApp app, DatabaseConnection connection) throws VitalServiceUnimplementedException, VitalServiceException;
	
	public VitalStatus removeDatabaseConnection(VitalOrganization organization, VitalApp app, String databaseName) throws VitalServiceUnimplementedException, VitalServiceException

	
	
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty arg0)

	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty arg0, VitalPathQuery arg1)

	public VitalStatus deleteExpandedObject(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject arg0)

	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, List<URIProperty> arg0, VitalPathQuery arg1)

	public VitalStatus deleteExpandedObjects(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, List<GraphObject> arg0)

	public VitalStatus deleteObject(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, GraphObject arg0)

	public VitalStatus doOperations(VitalOrganization organization,
			VitalApp app, ServiceOperations arg0)

	public ResultList getExpanded(VitalOrganization organization, VitalApp app, URIProperty arg0, boolean arg1)

	public ResultList getExpanded(VitalOrganization organization, VitalApp app, URIProperty arg0, VitalPathQuery arg1)

	public VitalSegment getSegment(VitalOrganization organization,
			VitalApp app, String arg0)

	public ResultList listFiles(VitalOrganization organization, VitalApp app, String arg0)

	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			GraphObject arg0)

	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			List<GraphObject> arg0)

	public VitalStatus isInitialized()

	//optional key is either admin or superdmin key
	public VitalStatus initialize(VitalServiceRootKey rootKey, VitalAuthKey optionalKey)

	
	//normal vital admin /superadmin access is enough
	public VitalStatus addVitalServiceKey(VitalOrganization organization, VitalApp app, VitalServiceKey serviceKey)

	//normal vital admin /superadmin access is enough
	public List<VitalServiceKey> listVitalServiceKeys(VitalOrganization organization, VitalApp app)

	//normal vital admin /superadmin access is enough
	public VitalStatus removeVitalServiceKey(VitalOrganization organization, VitalApp app, VitalServiceKey serviceKey)

	//for superadmin access / or access with root keys
	public VitalStatus addVitalServiceAdminKey(VitalOrganization organization, VitalServiceRootKey rootKey, VitalServiceAdminKey serviceAdminKey)

	//for superadmin access / or access with root keys
	public VitalStatus removeVitalServiceAdminKey(VitalOrganization organization, VitalServiceRootKey rootKey, VitalServiceAdminKey serviceAdminKey)

	//for superadmin access / or access with root keys
	public List<VitalServiceAdminKey> listVitalServiceAdminKeys(VitalOrganization organization, VitalServiceRootKey rootKey);

	//for root key access only
	public VitalStatus addVitalServiceSuperAdminKey(VitalServiceRootKey rootKey, VitalAuthKey serviceSuperAdminKey)
	
	//for root key access only
	public VitalStatus removeVitalServiceSuperAdminKey(VitalServiceRootKey rootKey, VitalAuthKey serviceSuperAdminKey)
	
	//for root key access only
	public List<VitalAuthKey> listVitalServiceSuperAdminKeys(VitalServiceRootKey rootKey);
	
	
	//only if prime is in testing mode
	public VitalStatus destroy(VitalServiceRootKey rootKey)

	public ResultList listSegmentsWithConfig(VitalOrganization organization, VitalApp app);
}
