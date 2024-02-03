package ai.vital.prime.client.common


import java.util.List

import org.apache.commons.httpclient.HostConfiguration
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.HttpMethod
import org.apache.commons.httpclient.HttpMethodBase
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager
import org.apache.commons.httpclient.methods.DeleteMethod
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.methods.InputStreamRequestEntity
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.commons.httpclient.methods.RequestEntity
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.io.IOUtils

import ai.vital.prime.client.IVitalPrimeClient
import ai.vital.prime.client.java.BlockCompactStreamRequestEntity
import ai.vital.vitalservice.ServiceOperations
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalServiceConstants;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.exception.VitalServiceException
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.query.VitalPathQuery
import ai.vital.vitalservice.query.VitalQuery
import ai.vital.vitalsigns.java.VitalMap;
import ai.vital.vitalsigns.model.DatabaseConnection
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VITAL_Event
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalAuthKey
import ai.vital.vitalsigns.model.VitalOrganization
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.VitalSession
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.property.URIProperty

abstract class VitalPrimeClientBaseImpl implements IVitalPrimeClient {
	
	
	private static final String VITAL_PRIME_ORGANIZATION_ID = "vital-prime-organization-id";
	
	private static final String VITAL_PRIME_ORGANIZATION_URI = "vital-prime-organization-uri";
	
	private static final String VITAL_PRIME_APP_ID = "vital-prime-app-id";
	
	private static final String VITAL_PRIME_APP_URI = "vital-prime-app-uri";
	
	private static final String VITAL_PRIME_SEGMENT_ID = "vital-prime-segment-id";
	
	private static final String VITAL_PRIME_SEGMENT_URI = "vital-prime-segment-uri";
	
	private static final String VITAL_PRIME_DATASET_URI = "vital-prime-dataset-uri";
	
	private static final String VITAL_PRIME_SESSION_ID = "vital-session-id";
	
	private static final String VITAL_PRIME_FILE_URI = "vital-file-uri";
	
	private static final String VITAL_PRIME_FILE_NAME = "vital-file-name";
	
	private static final String VITAL_PRIME_OVERWRITE = "vital-file-overwrite";
	
	private static final String VITAL_PRIME_EXISTS = "vital-file-exists";
	
	
	private HttpClient client
	
	protected String endpointURL
	
	private String filesURL
	
	private String bulkURL

	//this is called by authenticate method
	private String sessionID
	
	private VitalAuthKey authKey
	
	private int authRetrialsCounter = 0;	
	
	public VitalPrimeClientBaseImpl(String endpointURL) {
		
		this.endpointURL = endpointURL
		
		validateEndpoint()
		
		if(this.endpointURL.endsWith("/")) {
			
			this.filesURL = this.endpointURL.substring(0, this.endpointURL.length() - 1) + '_files/'
			
			this.bulkURL = this.endpointURL.substring(0, this.endpointURL.length() - 1) + '_bulk/'
			
		} else {
		
			this.filesURL = this.endpointURL + '_files/'
			
			this.bulkURL = this.endpointURL + '_bulk/'
			
		}
		
		MultiThreadedHttpConnectionManager cm = new MultiThreadedHttpConnectionManager();
		HttpConnectionManagerParams params = new HttpConnectionManagerParams()
		params.setDefaultMaxConnectionsPerHost(100)
		params.setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION, 100)
		params.setMaxTotalConnections(200)
		cm.setParams(params)
		
		client = new HttpClient(cm);
		
		
		
	}
	
	protected abstract void validateEndpoint()
	
	protected abstract RequestEntity createRequestEntity(Map request)
	
	protected abstract Object processResponse(InputStream stream)
	
	static Set<String> sessionlessMethods = new HashSet<String>([
		'authenticate', 
		'destroy',
		'isInitialized', 
		'initialize', 
		'addVitalServiceAdminKey', 
		'removeVitalServiceAdminKey', 
		'listVitalServiceAdminKeys',
		'addVitalServiceSuperAdminKey',
		'removeVitalServiceSuperAdminKey',
		'listVitalServiceSuperAdminKeys'
	]);
	
	
	private Object innerImpl(Class responseClass, String method, VitalOrganization organization, VitalApp app, Map paramsMap) throws VitalServiceException {
		
		PostMethod post = new PostMethod(this.endpointURL)
		
		if(!this.sessionID && ! sessionlessMethods.contains(method)) {
			throw new VitalServiceException("Not authenticated, call authenticate first to obtain a session")
		}
		
		Map request = [
			sessionID: this.sessionID,
			method: method,
			organization: organization,
			app: app,
			params: paramsMap
		]
		
		
		RequestEntity requestEntity = createRequestEntity(request)
		
//		ByteArrayOutputStream output = new ByteArrayOutputStream();
//		            						   
//		ObjectOutputStream oos = new ObjectOutputStream(output)
//		oos.writeObject(request)
//		oos.close()
//		
//		post.setRequestEntity(new ByteArrayRequestEntity(output.toByteArray(), "application/x-java-serialized-object"));
		post.setRequestEntity(requestEntity);
		
		try {
			
			return innerGenericImpl(responseClass, post)
			
		} catch(VitalServiceException ex ){
		
			if(ex.sessionNotFound) {
				//try authenticating
				this.sessionID = null
				ResultList rl = this.authenticate(this.authKey)
				if(rl.status.status == VitalStatus.Status.ok) {
					//everything was ok repeat the request
					request.sessionID = this.sessionID
					requestEntity = createRequestEntity(request)
					post.setRequestEntity(requestEntity);
					return innerGenericImpl(responseClass, post)
				} else {
					throw new VitalServiceException("Couldn't reauthenticate after detected a broken session: " + rl.status.message)
				}
			}
			
			throw ex;
		
		}
		
	}
	
	
	private Object innerGenericImpl(Class responseClass, HttpMethod post) throws VitalServiceException {
		
//		ObjectInputStream ois = null
		
		Object responseObject = null
		
		InputStream stream = null
				
		try {
			
			int status = client.executeMethod(post)
			
			if(status < 200 || status > 299) {
				
				// try to obtain the error message
				String msg = "";
				try { msg = post.getResponseBodyAsString() } catch(Exception ex) {}
				
				throw new VitalServiceException("HTTP error: ${status} - ${msg}")
			}
			
			stream = post.getResponseBodyAsStream()
			
			responseObject = processResponse(stream)
			
			// ois = new VitalObjectInputStream( post.getResponseBodyAsStream() )

			// responseObject = ois.readObject()
						
		} catch(Exception e) {
			throw new VitalServiceException(e)
		} finally {
			IOUtils.closeQuietly(stream)
			post.releaseConnection()
		}
		
		// deserialize response from json
		
		if(responseObject == null) {
			if(responseClass == GraphObject.class || responseClass == null || responseClass == VitalApp.class || responseClass == VitalSegment.class ||
				responseClass == VitalOrganization.class) return null
			throw new VitalServiceException("Null object returned")
		}
		
		Class responseClass2 = responseObject.getClass()
		
		if(responseClass2 == VitalServiceException.class || responseClass2 == VitalServiceUnimplementedException.class) {
			throw responseObject;
		}
		
		if( ! responseClass.isAssignableFrom(responseClass2)) {
			throw new VitalServiceException("Malformed class response: ${responseClass2.canonicalName}, expected: ${responseClass.canonicalName}")
		}

		return responseObject		
		
	}
	@Override
	public ResultList callFunction(VitalOrganization organization, VitalApp app, String function,
			Map<String, Object> arguments) throws VitalServiceException,
			VitalServiceUnimplementedException {
		if(!(arguments instanceof VitalMap)) {
			arguments = new VitalMap(arguments)
		}
		return innerImpl(ResultList.class, 'callFunction', organization, app, [function: function, arguments: arguments]);
	}
			
	@Override
	public GraphObject get(VitalOrganization organization, VitalApp app, URIProperty uri)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(GraphObject.class, 'get', organization, app, [uri: uri]);
	}
		
	@Override
	public List<GraphObject> getBatch(VitalOrganization organization, VitalApp app,
			List<URIProperty> uris) throws VitalServiceException,
			VitalServiceUnimplementedException {
		return innerImpl(List.class, 'getBatch', organization, app, [uris: uris]);
	}

	private VitalTransaction tx(VitalTransaction transaction) {
		if( transaction != null && transaction != VitalServiceConstants.NO_TRANSACTION ) {
			return transaction
		}
		return null
	}
			
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, GraphObject graphObject, boolean create)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(ResultList.class, 'save', organization, app, [transaction: tx(transaction), targetSegment: targetSegment, object: graphObject, create: create])
	}

	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, List<GraphObject> graphObjectsList, boolean create)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(ResultList.class, 'save', organization, app, [transaction: tx(transaction), targetSegment: targetSegment, objects: graphObjectsList, create: create]);
	}
			
	@Override
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(ResultList.class, 'insert', organization, app, [transaction: tx(transaction), targetSegment: targetSegment, graphObject: graphObject]);
	}

	@Override
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment,
			List<GraphObject> graphObjectsList) throws VitalServiceException,
			VitalServiceUnimplementedException {
		return innerImpl(ResultList.class, 'insert', organization, app, [transaction: tx(transaction), targetSegment: targetSegment, graphObjectsList: graphObjectsList]);
	}

	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalStatus.class, 'delete', organization, app, [transaction: tx(transaction), uri: uri]);
	}

	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalStatus.class, 'delete', organization, app, [transaction: tx(transaction), uris: uris]);
	}

	@Override
	public ResultList query(VitalOrganization organization, VitalApp app,
			VitalQuery query) throws VitalServiceException,
			VitalServiceUnimplementedException {
		return innerImpl(ResultList.class, 'query', organization, app, [query: query]);
	}

	@Override
	public List<VitalSegment> listSegments(VitalOrganization organization, VitalApp app)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(List.class, 'listSegments', organization, app, [:]);
	}

	@Override
	public VitalStatus sendFlumeEvent(VitalOrganization organization, VitalApp app,
			VITAL_Event event, boolean waitForDelivery)
			throws VitalServiceException, VitalServiceUnimplementedException {
		// TODO Auto-generated method stub
		throw new VitalServiceUnimplementedException("Not implemented.");
	}

	@Override
	public VitalStatus sendFlumeEvents(VitalOrganization organization, VitalApp app,
			List<VITAL_Event> event, boolean waitForDelivery)
			throws VitalServiceException, VitalServiceUnimplementedException {
		// TODO Auto-generated method stub
		throw new VitalServiceUnimplementedException("Not implemented.");
	}

	@Override
	public VitalStatus uploadFile(VitalOrganization organization, VitalApp app,
			URIProperty uri, String fileName, InputStream inputStream,
			boolean overwrite) throws VitalServiceException,
			VitalServiceUnimplementedException {


		PostMethod postMethod = new PostMethod(this.filesURL)
		
		addContextHeaders(postMethod, organization, app)		
		
		postMethod.addRequestHeader(VITAL_PRIME_SESSION_ID, sessionID)
		postMethod.addRequestHeader(VITAL_PRIME_FILE_URI, uri.get())
		postMethod.addRequestHeader(VITAL_PRIME_FILE_NAME, fileName)
		postMethod.addRequestHeader(VITAL_PRIME_OVERWRITE, '' + overwrite)

		postMethod.setRequestEntity(new InputStreamRequestEntity(inputStream))
		
		try {
			return innerGenericImpl(VitalStatus.class, postMethod);
		} finally {
			IOUtils.closeQuietly(inputStream)
		}		
				
	}
			
	private void addContextHeaders(HttpMethodBase method, VitalOrganization organization, VitalApp app) {

		if(organization != null) {
			method.addRequestHeader(VITAL_PRIME_ORGANIZATION_URI, organization.URI)
			method.addRequestHeader(VITAL_PRIME_ORGANIZATION_ID, organization.organizationID?.toString())
		}
		
		if(app != null) {
			method.addRequestHeader(VITAL_PRIME_APP_URI, app.URI)
			method.addRequestHeader(VITAL_PRIME_APP_ID, app.appID?.toString())
		}

	}		

	@Override
	public VitalStatus downloadFile(VitalOrganization organization, VitalApp app,
			URIProperty uri, String fileName, OutputStream outputStream,
			boolean closeOutputStream) throws VitalServiceException,
			VitalServiceUnimplementedException {

		GetMethod getMethod = new GetMethod(this.filesURL)
		addContextHeaders(getMethod, organization, app)
		getMethod.addRequestHeader(VITAL_PRIME_SESSION_ID, sessionID)
		getMethod.addRequestHeader(VITAL_PRIME_FILE_URI, uri.get())
		getMethod.addRequestHeader(VITAL_PRIME_FILE_NAME, fileName)
		getMethod.addRequestHeader(VITAL_PRIME_EXISTS, "false")
//		private static final String VITAL_PRIME_OVERWRITE = "vital-file-overwrite";
		
		InputStream inputStream = null
		
		try {
			
			int status = client.executeMethod(getMethod)
			
			if(status == 404) {
				return VitalStatus.withError("File not found");
			}
			
			if(status < 200 || status > 299) {
				
				//try to obtain the error message
				String msg = "";
				try { msg = getMethod.getResponseBodyAsString() } catch(Exception ex) {}
				
				throw new VitalServiceException("HTTP error: ${status} - ${msg}")
			}
			
			inputStream = getMethod.getResponseBodyAsStream()

			//intput stream may be empty if file is empty
			if(inputStream != null) {
				IOUtils.copy(inputStream, outputStream)
			}
			
						
			if(closeOutputStream) {
				IOUtils.closeQuietly(outputStream)	
			}
			
			return VitalStatus.withOK()
			
		} catch(Exception e) {
			throw new VitalServiceException(e)
		} finally {
			IOUtils.closeQuietly(inputStream)
		}
				
	}

	@Override
	public VitalStatus fileExists(VitalOrganization organization, VitalApp app,
			URIProperty uri, String fileName) throws VitalServiceException,
			VitalServiceUnimplementedException {

		GetMethod getMethod = new GetMethod(this.filesURL)
		addContextHeaders(getMethod, organization, app)
		getMethod.addRequestHeader(VITAL_PRIME_SESSION_ID, sessionID)
		getMethod.addRequestHeader(VITAL_PRIME_FILE_URI, uri.get())
		getMethod.addRequestHeader(VITAL_PRIME_FILE_NAME, fileName)
		getMethod.addRequestHeader(VITAL_PRIME_EXISTS, "true")
//		
//		private static final String VITAL_PRIME_OVERWRITE = "vital-file-overwrite";
//		
//		
		return innerGenericImpl(VitalStatus.class, getMethod)
				

	}

	@Override
	public VitalStatus deleteFile(VitalOrganization organization, VitalApp app,
			URIProperty uri, String fileName) throws VitalServiceException,
			VitalServiceUnimplementedException {

		DeleteMethod deleteMethod = new DeleteMethod(this.filesURL)
		addContextHeaders(deleteMethod, organization, app)
		deleteMethod.addRequestHeader(VITAL_PRIME_SESSION_ID, sessionID)
		deleteMethod.addRequestHeader(VITAL_PRIME_FILE_URI, uri.get())
		deleteMethod.addRequestHeader(VITAL_PRIME_FILE_NAME, fileName)

		return innerGenericImpl(VitalStatus.class, deleteMethod)
		
	}

	@Override
	public VitalStatus ping() throws VitalServiceException,
			VitalServiceUnimplementedException {
		return innerImpl(VitalStatus.class, 'ping', null, null, [:]);
	}

	@Override
	public VitalSegment addSegment(VitalOrganization organization, VitalApp app,
			VitalSegment config, VitalProvisioning provisioning, boolean createIfNotExists)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalSegment.class, 'addSegment', organization, app, [segment: config, provisioning: provisioning, createIfNotExists: createIfNotExists]);
	}

	@Override
	public VitalStatus removeSegment(VitalOrganization organization, VitalApp app,
			VitalSegment segment, boolean deleteData)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalStatus.class, 'removeSegment', organization, app, [segment: segment, deleteData: deleteData]);
	}

	@Override
	public List<VitalApp> listApps(VitalOrganization organization) throws VitalServiceException,
			VitalServiceUnimplementedException {
		return innerImpl(List.class, 'listApps', organization, null, [:]);
	}

	@Override
	public VitalStatus addApp(VitalOrganization organization, VitalApp app)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalStatus.class, 'addApp', organization, null, [app: app]);
	}

	@Override
	public VitalStatus removeApp(VitalOrganization organization, VitalApp app)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalStatus.class, 'removeApp', organization, null, [app: app]);
	}

	@Override
	public VitalOrganization addOrganization(VitalOrganization organization)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalOrganization.class, 'addOrganization', null, null, [organization: organization]);
	}

	@Override
	public VitalStatus removeOrganization(VitalOrganization organization)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalStatus.class, 'removeOrganization', null, null, [organization: organization]);
	}

	@Override
	public List<VitalOrganization> listOrganizations() throws VitalServiceException,
			VitalServiceUnimplementedException {
		return innerImpl(List.class, 'listOrganizations', null, null, [:]);
	}

	@Override
	public VitalStatus shutdown() {
		return innerImpl(VitalStatus.class, 'shutdown', null, null, [:]);
	}

	@Override
	public List<GraphObject> listDatascripts(VitalOrganization organization, VitalApp app, String path, boolean getBodies)
			throws VitalServiceException {
		return innerImpl(List.class, 'listDatascripts', organization, app, [path: path, getBodies: getBodies]);
	}

	@Override
	public GraphObject addDatascript(VitalOrganization organization, VitalApp app, String path,
			String body) throws VitalServiceException {
		return innerImpl(GraphObject.class, 'addDatascript', organization, app, [path: path, body: body]);
	}

	@Override
	public VitalStatus removeDatascript(VitalOrganization organization, VitalApp app, String path)
			throws VitalServiceException {
		return innerImpl(VitalStatus.class, 'removeDatascript', organization, app, [path: path]);
	}

	@Override
	public VitalStatus deleteObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			List<GraphObject> objects) throws VitalServiceException,
			VitalServiceUnimplementedException {
		return innerImpl(VitalStatus.class, 'deleteObjects', organization, app, [transaction: tx(transaction), objects: objects]);
	}
			

	@Override
	public VitalStatus commitTransaction(VitalTransaction transaction)
			throws VitalServiceException {
		return innerImpl(VitalStatus.class, 'commitTransaction', null, null, [transaction: transaction]);
	}

	@Override
	public VitalTransaction createTransaction() throws VitalServiceException {
		return innerImpl(VitalTransaction.class, 'createTransaction', null, null, [:]);
	}

	@Override
	public List<VitalTransaction> getTransactions() throws VitalServiceException {
		return innerImpl(List.class, 'getTransactions', null, null, [:]);
	}

	@Override
	public VitalStatus rollbackTransaction(VitalTransaction transaction) {
		return innerImpl(VitalStatus.class, 'rollbackTransaction', null, null, [transaction: transaction]);
	}

	@Override
	public VitalOrganization getOrganization(String organizationID)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalOrganization.class, 'getOrganization', null, null, [organizationID: organizationID]);
	}

	@Override
	public VitalApp getApp(VitalOrganization organization, String appID)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return innerImpl(VitalApp.class, 'getApp', organization, null, [appID: appID]);
	}

	@Override
	public URIProperty generateURI(VitalOrganization organization, VitalApp app,
			Class<? extends GraphObject> clazz) throws VitalServiceException,
			VitalServiceUnimplementedException {
		return innerImpl(URIProperty.class, 'generateURI', organization, app, ['class': clazz]);
	}

	@Override
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app,
			VitalSegment segment, OutputStream outputStream, String datasetURI)
			throws VitalServiceException, VitalServiceUnimplementedException {

		GetMethod getMethod = new GetMethod(this.bulkURL)
				
		addContextHeaders(getMethod, organization, app)
		getMethod.addRequestHeader(VITAL_PRIME_SESSION_ID, sessionID)
		
		if(segment) {
			getMethod.addRequestHeader(VITAL_PRIME_SEGMENT_ID, segment.segmentID.toString())
			getMethod.addRequestHeader(VITAL_PRIME_SEGMENT_URI, segment.URI)
		}
		if(datasetURI) {
			getMethod.addRequestHeader(VITAL_PRIME_DATASET_URI, datasetURI)
		}

		InputStream inputStream = null
		
		try {
		
			int status = client.executeMethod(getMethod)
			
			if(status < 200 || status > 299) {
				
				//try to obtain the error message
				String msg = "";
				try { msg = getMethod.getResponseBodyAsString(8192) } catch(Exception ex) {}
				
				throw new VitalServiceException("HTTP error: ${status} - ${msg}")
			}
			
			inputStream = getMethod.getResponseBodyAsStream()
			
			IOUtils.copy(inputStream, outputStream)	
			
			return VitalStatus.withOKMessage("Export success")
			
		} catch(Exception e) {
		
			return VitalStatus.withError("Export error: " + e.getLocalizedMessage())
			
		} finally {
		
			IOUtils.closeQuietly(inputStream)
			getMethod.releaseConnection();
			
		}

	}

	@Override
	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app,
			VitalSegment segment, InputStream inputStream, String datasetURI)
			throws VitalServiceException, VitalServiceUnimplementedException {
				
		PostMethod postMethod = new PostMethod(this.bulkURL)
		addContextHeaders(postMethod, organization, app)
		postMethod.addRequestHeader(VITAL_PRIME_SESSION_ID, sessionID)
		if(segment) {
			postMethod.addRequestHeader(VITAL_PRIME_SEGMENT_ID, segment.segmentID.toString())
			postMethod.addRequestHeader(VITAL_PRIME_SEGMENT_URI, segment.URI)
		}

		if(datasetURI != null) {
			if(datasetURI.length() > 0) {
				postMethod.addRequestHeader(VITAL_PRIME_DATASET_URI, datasetURI)
			} else {
				postMethod.addRequestHeader(VITAL_PRIME_DATASET_URI, '-')
			}
		}					

		BlockCompactStreamRequestEntity reqEntity = new BlockCompactStreamRequestEntity(inputStream);
		postMethod.setRequestEntity(reqEntity)
		
		try {
			return innerGenericImpl(VitalStatus.class, postMethod);
		} finally {
			postMethod.releaseConnection();
//			IOUtils.closeQuietly(inputStream)
		}		
	}

	@Override
	public ResultList listDatabaseConnections(VitalOrganization organization, VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return innerImpl(ResultList.class, 'listDatabaseConnections', organization, app, [:]);
	}

	@Override
	public VitalStatus addDatabaseConnection(VitalOrganization organization,
			VitalApp app, DatabaseConnection connection)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return innerImpl(VitalStatus.class, 'addDatabaseConnection', organization, app, [connection: connection]);
	}

	@Override
	public VitalStatus removeDatabaseConnection(VitalOrganization organization,
			VitalApp app, String databaseName)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return innerImpl(VitalStatus.class, 'removeDatabaseConnection', organization, app, [databaseName: databaseName]);
	}

	@Override
	public ResultList authenticate(VitalAuthKey authKey)
			throws VitalServiceException, VitalServiceUnimplementedException {
				
		if(this.sessionID) throw new VitalServiceException("this client is already authenticated");
		ResultList rl = innerImpl(ResultList.class, 'authenticate', null, null, [authKey: authKey]);
		if(rl.status.status == VitalStatus.Status.ok) {
			VitalSession session = null
			for(GraphObject g : rl) {
				if(g instanceof VitalSession) {
					session = (VitalSession)g;
				}
			}
			if(session == null) throw new VitalServiceException("No session object");
			this.sessionID = session.sessionID.toString()
			this.authKey = authKey
		}
		return rl
	}

	@Override
	public VitalStatus closeSession() throws VitalServiceException,
			VitalServiceUnimplementedException {
		if( ! this.sessionID ) return VitalStatus.withError("not authenticated");
		VitalStatus status = innerImpl(VitalStatus.class, 'closeSession', null, null, [:]);
		this.sessionID = null;
		this.authKey = null;
		return status;
	}

	@Override
	public VitalStatus testBrokenSession() throws VitalServiceException,
			VitalServiceUnimplementedException {
		if( ! this.sessionID ) return VitalStatus.withError("not authenticated");
		return innerImpl(VitalStatus.class, 'closeSession', null, null, [:]);
	}

			
	//XXX other methods for 1:1 compatibility
			
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, URIProperty arg0) {
		return innerImpl(VitalStatus.class, "deleteExpanded", organization, app, [transaction: tx(transaction), uri: arg0]);
	}

	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, URIProperty arg0, VitalPathQuery arg1) {
		return innerImpl(VitalStatus.class, "deleteExpanded", organization, app, [transaction: tx(transaction), uri: arg0, pathQuery: arg1]);
	}

	@Override
	public VitalStatus deleteExpandedObject(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, GraphObject arg0) {
		return innerImpl(VitalStatus.class, "deleteExpandedObject", organization, app, [transaction: tx(transaction), object: arg0]);
	}

	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, List<URIProperty> arg0, VitalPathQuery arg1) {
		return innerImpl(VitalStatus.class, "deleteExpanded", organization, app, [transaction: tx(transaction), uris: arg0, pathQuery: arg1]);
	}
			
	@Override
	public VitalStatus deleteExpandedObjects(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, List<GraphObject> arg0) {
		return innerImpl(VitalStatus.class, "deleteExpandedObjects", organization, app, [transaction: tx(transaction), objects: arg0]);
	}

	@Override
	public VitalStatus deleteObject(VitalTransaction transaction, VitalOrganization organization,
			VitalApp app, GraphObject arg0) {
		return innerImpl(VitalStatus.class, "deleteObject", organization, app, [transaction: tx(transaction), object: arg0]);
	}

	@Override
	public VitalStatus doOperations(VitalOrganization organization,
			VitalApp app, ServiceOperations arg0) {
		return innerImpl(VitalStatus.class, "doOperations", organization, app, [operations: arg0]);
	}

	@Override
	public ResultList getExpanded(VitalOrganization organization, VitalApp app,
			URIProperty arg0, boolean arg1) {
		return innerImpl(ResultList.class, "getExpanded", organization, app, [uri: arg0]);
	}

	@Override
	public ResultList getExpanded(VitalOrganization organization, VitalApp app,
			URIProperty arg0, VitalPathQuery arg1) {
		return innerImpl(ResultList.class, "getExpanded", organization, app, [uri: arg0, pathQuery: arg1]);
	}

	@Override
	public VitalSegment getSegment(VitalOrganization organization,
			VitalApp app, String arg0) {
		return innerImpl(VitalSegment.class, "getSegment", organization, app, [segmentID: arg0]);
	}

	@Override
	public ResultList listFiles(VitalOrganization organization, VitalApp app,
			String arg0) {
		return innerImpl(ResultList.class, "listFiles", organization, app, [uri: arg0]);
	}

	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			GraphObject arg0) {
		return innerImpl(ResultList.class, "save", organization, app, [transaction: tx(transaction), object: arg0]);
	}

	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			List<GraphObject> arg0) {
		return innerImpl(ResultList.class, "save", organization, app, [transaction: tx(transaction), objects: arg0]);
	}

	@Override
	public VitalStatus isInitialized() {
		return innerImpl(VitalStatus.class, "isInitialized", null, null, [:]);
	}

	@Override
	public VitalStatus initialize(VitalServiceRootKey rootKey, VitalAuthKey optionalKey) {
		return innerImpl(VitalStatus.class, "initialize", null, null, [rootKey: rootKey, optionalKey: optionalKey]);
	}

	@Override
	public VitalStatus addVitalServiceKey(VitalOrganization organization,
			VitalApp app, VitalServiceKey serviceKey) {
		return innerImpl(VitalStatus.class, "addVitalServiceKey", organization, app, [serviceKey: serviceKey]);
	}

	@Override
	public List<VitalServiceKey> listVitalServiceKeys(
			VitalOrganization organization, VitalApp app) {
		return innerImpl(List.class, "listVitalServiceKeys", organization, app, [:]);
	}

	@Override
	public VitalStatus removeVitalServiceKey(VitalOrganization organization,
			VitalApp app, VitalServiceKey serviceKey) {
		return innerImpl(VitalStatus.class, "removeVitalServiceKey", organization, app, [serviceKey: serviceKey]);
	}

	@Override
	public VitalStatus addVitalServiceAdminKey(VitalOrganization organization, VitalServiceRootKey rootKey,
			VitalServiceAdminKey serviceAdminKey) {
		return innerImpl(VitalStatus.class, "addVitalServiceAdminKey", organization, null, [rootKey: rootKey, serviceAdminKey: serviceAdminKey]);
	}

	@Override
	public VitalStatus removeVitalServiceAdminKey(
			VitalOrganization organization, VitalServiceRootKey rootKey, VitalServiceAdminKey serviceAdminKey) {
		return innerImpl(VitalStatus.class, "removeVitalServiceAdminKey", organization, null, [rootKey: rootKey, serviceAdminKey: serviceAdminKey]);
	}

	@Override
	public List<VitalServiceAdminKey> listVitalServiceAdminKeys(
			VitalOrganization organization, VitalServiceRootKey rootKey) {
		return innerImpl(List.class, "listVitalServiceAdminKeys", organization, null, [rootKey: rootKey]);
	}

	@Override
	public VitalStatus addVitalServiceSuperAdminKey(
			VitalServiceRootKey rootKey, VitalAuthKey serviceSuperAdminKey) {
		return innerImpl(VitalStatus.class, "addVitalServiceSuperAdminKey", null, null, [rootKey: rootKey, serviceSuperAdminKey: serviceSuperAdminKey]);
	}

	@Override
	public VitalStatus removeVitalServiceSuperAdminKey(
			VitalServiceRootKey rootKey,
			VitalAuthKey serviceSuperAdminKey) {
		return innerImpl(VitalStatus.class, "removeVitalServiceSuperAdminKey", null, null, [rootKey: rootKey, serviceSuperAdminKey: serviceSuperAdminKey]);
	}

	@Override
	public List<VitalAuthKey> listVitalServiceSuperAdminKeys(
			VitalServiceRootKey rootKey) {
		return innerImpl(List.class, "listVitalServiceSuperAdminKeys", null, null, [rootKey: rootKey]);
	}

	@Override
	public VitalStatus destroy(VitalServiceRootKey rootKey) {
		return innerImpl(VitalStatus.class, "destroy", null, null, [rootKey: rootKey]);
	}

	@Override
	public ResultList listSegmentsWithConfig(VitalOrganization organization,
			VitalApp app) {
		return innerImpl(ResultList.class, "listSegmentsWithConfig", organization, app, [:]);
	}
			
			

}
