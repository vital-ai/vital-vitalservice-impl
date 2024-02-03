package ai.vital.prime.service.admin;

import static ai.vital.vitalservice.VitalServiceConstants.NO_TRANSACTION;
import groovy.lang.Closure;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.lucene.model.LuceneSegment;
import ai.vital.prime.client.IVitalPrimeClient;
import ai.vital.prime.service.config.VitalServicePrimeConfig;
import ai.vital.prime.service.queue.QueueCallFunctionInterceptor;
import ai.vital.service.lucene.impl.LuceneServiceQueriesImpl;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.ServiceOperations;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.impl.IVitalServiceConfigAware;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.DatabaseConnection;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Event;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalAuthKey;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.container.GraphObjectsIterable;
import ai.vital.vitalsigns.model.property.URIProperty;


public class VitalServiceAdminPrime implements VitalServiceAdmin, IVitalServiceConfigAware {

	IVitalPrimeClient client;

	private VitalOrganization organization;

	private String name;

	private VitalServiceAdminKey serviceAdminKey;

	private VitalServicePrimeConfig config;
	
	private final static Logger log = LoggerFactory.getLogger(VitalServiceAdminPrime.class);
	
	private QueueCallFunctionInterceptor queueCallFunctionInterceptor = new QueueCallFunctionInterceptor(); 
	
	private VitalServiceAdminPrime(IVitalPrimeClient client,
			VitalOrganization _organization) {

		this.client = client;
		this.organization = _organization;
//		organization = client.getOrganization(_organization.ID)
//		if(organization == null) throw new RuntimeException("Organization not found: ${_organization.ID}")
//		app = client.getApp(_organization.ID, _app.ID)
//		if(app == null) throw new RuntimeException("App not found: ${_app.ID}, organizationID: ${_organization.ID}")
		
	}
	
	
	private void addToCache(ResultList rl) {
		
//		if(useCache) {
			List<GraphObject> gs = new ArrayList<GraphObject>(rl.getResults().size());
			for(ResultElement re : rl.getResults()) {
				GraphObject graphObject = re.getGraphObject();
				if(graphObject instanceof VITAL_GraphContainerObject) continue;
				gs.add(graphObject);
			}

			if(gs.size() > 0) VitalSigns.get().addToCache(gs);
//		}
			
	}

	private void addToCache(List<GraphObject> gos) {

//		if(useCache && gos != null && gos.size() > 0) {
			VitalSigns.get().addToCache(gos);
//		}

	}
			
	public static VitalServiceAdminPrime create(IVitalPrimeClient client, VitalOrganization organization) {

		return new VitalServiceAdminPrime(client, organization);
	}

	@Override
	public VitalStatus bulkExport(VitalApp app, VitalSegment segment, OutputStream outputStream)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return bulkExport(app, segment, outputStream, null);
	}
	
	@Override
	public VitalStatus bulkExport(VitalApp app, VitalSegment segment, OutputStream outputStream, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.bulkExport(organization, app, segment, outputStream, datasetURI);
	}

	@Override
	public VitalStatus bulkImport(VitalApp app, VitalSegment segment, InputStream inputStream)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return bulkImport(app, segment, inputStream, "");
	}
	
	@Override
	public VitalStatus bulkImport(VitalApp app, VitalSegment segment, InputStream inputStream, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.bulkImport(organization, app, segment, inputStream, datasetURI);
	}

	@Override
	public ResultList callFunction(VitalApp app, String arg0, Map<String, Object> arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		
		ResultList rl = queueCallFunctionInterceptor.callFunction(client, organization, app, arg0, arg1);
		if(rl != null) return rl;
		
		return client.callFunction(organization, app, arg0, arg1);
	}

	boolean closed = false;
	
	@Override
	public VitalStatus close() throws VitalServiceUnimplementedException,
			VitalServiceException {
		//already closed
		if(closed) return VitalStatus.withOKMessage("already closed");
		
		try {
			client.closeSession();
		} catch(Exception e) {
			log.error(e.getLocalizedMessage());
		}
		
		closed = true;
		
//		if(currentTransaction != null) {
//			try {
//				rollbackTransaction(currentTransaction);
//			} catch(Exception e) {}
//		}
		
		VitalServiceFactory.close(this);

		queueCallFunctionInterceptor.close();
		
		return VitalStatus.withOK();
		
	}

	@Override
	public VitalStatus commitTransaction(VitalTransaction arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.commitTransaction(arg0);
	}

	@Override
	public VitalTransaction createTransaction()
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.createTransaction();
	}

	@Override
	public VitalStatus delete(VitalApp app, URIProperty arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.delete(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalApp app, URIProperty arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.delete(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus delete(VitalApp app, List<URIProperty> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.delete(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus delete(VitalTransaction transaction,VitalApp app, List<URIProperty> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.delete(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteExpanded(VitalApp app, URIProperty arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpanded(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalApp app, URIProperty arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpanded(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteExpanded(VitalApp app, URIProperty arg0, VitalPathQuery arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpanded(NO_TRANSACTION, organization, app, arg0, arg1);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalApp app, URIProperty arg0, VitalPathQuery arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpanded(transaction, organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus deleteExpanded(VitalApp app, List<URIProperty> arg0,
			VitalPathQuery arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.deleteExpanded(NO_TRANSACTION, organization, app, arg0, arg1);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalApp app, List<URIProperty> arg0,
			VitalPathQuery arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.deleteExpanded(transaction, organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus deleteExpandedObject(VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpandedObject(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteExpandedObject(VitalTransaction transaction, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpandedObject(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteExpandedObjects(VitalApp app, List<GraphObject> arg0,
			VitalPathQuery arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.deleteExpandedObjects(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteExpandedObjects(VitalTransaction transaction, VitalApp app, List<GraphObject> arg0,
			VitalPathQuery arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.deleteExpandedObjects(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteFile(VitalApp app, URIProperty arg0, String arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteFile(organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus deleteObject(VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteObject(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteObject(VitalTransaction transaction, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteObject(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteObjects(VitalApp app, List<GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteObjects(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteObjects(VitalTransaction transaction, VitalApp app, List<GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteObjects(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus doOperations(VitalApp app, ServiceOperations arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.doOperations(organization, app, arg0);
	}

	@Override
	public VitalStatus downloadFile(VitalApp app, URIProperty arg0, String arg1,
			OutputStream arg2, boolean arg3)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.downloadFile(organization, app, arg0, arg1, arg2, arg3);
	}

	@Override
	public VitalStatus fileExists(VitalApp app, URIProperty arg0, String arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.fileExists(organization, app, arg0, arg1);
	}

	@Override
	public URIProperty generateURI(VitalApp app, Class<? extends GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.generateURI(organization, app, arg0);
	}

	// get
	
	// default cache=true, for consistency always include GraphContext
	@Override
	public ResultList get(VitalApp app, GraphContext graphContext, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.get(app, graphContext, uri, true);
	}
		
	@Override
	public ResultList get(VitalApp app, GraphContext graphContext, URIProperty uri, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.getImpl(app, graphContext, Arrays.asList(uri), cache, null);
	}
		
	@Override
	public ResultList get(VitalApp app, GraphContext graphContext, List<URIProperty> uris) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.get(app, graphContext, uris, true);
	}
		
	@Override
	public ResultList get(VitalApp app, GraphContext graphContext, List<URIProperty> uris, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.getImpl(app, graphContext, uris, cache, null);
	}
		
		
	// GraphContext is redundant here as this call only used for containers, but kept for consistency
		
	@Override
	public ResultList get(VitalApp app, GraphContext graphContext, URIProperty uri, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.getImpl(app, graphContext, Arrays.asList(uri), true, containers);
	}
		
	@Override
	public ResultList get(VitalApp app, GraphContext graphContext, List<URIProperty> uris, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.getImpl(app, graphContext, uris, true, containers);
	}
		
		
	protected ResultList getImpl(VitalApp app, GraphContext graphContext, List<URIProperty> uris, boolean cache, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException {

		if(graphContext == null) throw new NullPointerException("graph context cannot be null");

		if(uris == null || uris.size() < 1) throw new NullPointerException("uris list cannot be null or empty");

		List<GraphObject> gos = null;

		if(graphContext == GraphContext.Local) {

			gos = new ArrayList<GraphObject>();

			for(URIProperty uri : uris) {

				GraphObject g = VitalSigns.get().getFromCache(uri.get());

				if(g != null) {
					gos.add(g);
				}

			}


		} else if(graphContext == GraphContext.Container) {

			if(containers == null || containers.size() < 1) throw new NullPointerException("When graphContext == Container the containers list cannot be null or empty");

			gos = new ArrayList<GraphObject>();

			for(URIProperty uri : uris) {
				for(GraphObjectsIterable iterable : containers) {
					GraphObject g = iterable.get(uri.get());
					if(g != null) gos.add(g);
				}
			}
			
		} else if(graphContext == GraphContext.ServiceWide) {

			try {
				gos = client.getBatch(organization, app, uris);
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}

			if(cache) {
				addToCache(gos);
			}

		} else throw new RuntimeException("Unhandled graph context: " + graphContext);

		ResultList rl = new ResultList();
		VitalStatus status = VitalStatus.withOK();
		rl.setStatus(status);
		Set<String> found = new HashSet<String>();
		List<URIProperty> failedURIs = new ArrayList<URIProperty>();
		
		for(GraphObject g : gos) {
			rl.getResults().add(new ResultElement(g, 1D));
			found.add(g.getURI());
		}
		for(URIProperty u : uris) {
			if(found.contains(u.get())) {
				
			} else {
				failedURIs.add(u);
			}
		}
		status.setFailedURIs(failedURIs);
		status.setErrors(failedURIs.size());
		status.setSuccesses(rl.getResults().size());
		return rl;
		
	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.VITALPRIME;
	}

	@Override
	public ResultList getExpanded(VitalApp app, URIProperty arg0, boolean arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.getExpanded(organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList getExpanded(VitalApp app, URIProperty arg0, VitalPathQuery arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		ResultList rl = client.getExpanded(organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public VitalOrganization getOrganization() {
		return organization;
	}

	@Override
	public VitalSegment getSegment(VitalApp app, String arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.getSegment(organization, app, arg0);
	}

	@Override
	public List<VitalTransaction> getTransactions()
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.getTransactions();
	}

	@Override
	public ResultList insert(VitalApp app, VitalSegment arg0, GraphObject arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.insert(NO_TRANSACTION, organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList insert(VitalTransaction transaction, VitalApp app, VitalSegment arg0, GraphObject arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.insert(transaction, organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList insert(VitalApp app, VitalSegment arg0, List<GraphObject> arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.insert(NO_TRANSACTION, organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList insert(VitalTransaction transaction, VitalApp app, VitalSegment arg0, List<GraphObject> arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.insert(transaction, organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList listDatabaseConnections(VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listDatabaseConnections(organization, app);
	}

	@Override
	public ResultList listFiles(VitalApp app, String arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listFiles(organization, app, arg0);
	}

	@Override
	public List<VitalSegment> listSegments(VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listSegments(organization, app);
	}
	
	@Override
	public ResultList listSegmentsWithConfig(VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listSegmentsWithConfig(organization, app);
	}

	@Override
	public VitalStatus ping() throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.ping();
	}


	@Override
	public ResultList query(VitalApp app, VitalQuery arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.query(organization, app, arg0);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList queryContainers(VitalApp app, VitalQuery query,
			List<GraphObjectsIterable> containers)
			throws VitalServiceUnimplementedException, VitalServiceException {
		if(containers == null || containers.size() < 1) throw new VitalServiceException("Null or empty containers list");
		List<LuceneSegment> segments = new ArrayList<LuceneSegment>();
		for(GraphObjectsIterable gi : containers) {
			try {
				LuceneSegment ls = gi.getLuceneSegment();
				if(ls == null) throw new Exception("container is not queryable - does not provide index: " + gi.getClass().getCanonicalName());
				segments.add(ls);
			} catch(Exception e) {
				throw new VitalServiceException(e.getLocalizedMessage());
			}
		}
		return LuceneServiceQueriesImpl.handleQuery(organization, app, query, segments);
	}

	@Override
	public ResultList queryLocal(VitalApp app, VitalQuery arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return VitalSigns.get().query(arg0);
	}

	@Override
	public VitalStatus rollbackTransaction(VitalTransaction arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.rollbackTransaction(arg0);
	}

	@Override
	public ResultList save(VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(NO_TRANSACTION, organization, app, arg0);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(transaction, organization, app, arg0);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList save(VitalApp app, List<GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(NO_TRANSACTION, organization, app, arg0);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalApp app, List<GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(transaction, organization, app, arg0);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList save(VitalApp app, VitalSegment arg0, GraphObject arg1, boolean arg2)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(NO_TRANSACTION, organization, app, arg0, arg1, arg2);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalApp app, VitalSegment arg0, GraphObject arg1, boolean arg2)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(transaction, organization, app, arg0, arg1, arg2);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList save(VitalApp app, VitalSegment arg0, List<GraphObject> arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		ResultList rl = client.save(NO_TRANSACTION, organization, app, arg0, arg1, arg2);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalApp app, VitalSegment arg0, List<GraphObject> arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		ResultList rl = client.save(transaction, organization, app, arg0, arg1, arg2);
		addToCache(rl);
		return rl;
	}

	@Override
	public VitalStatus sendEvent(VitalApp app, VITAL_Event arg0, boolean arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.sendFlumeEvent(organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus sendEvents(VitalApp app, List<VITAL_Event> arg0, boolean arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.sendFlumeEvents(organization, app, arg0, arg1);
	}


	@Override
	public VitalStatus uploadFile(VitalApp app, URIProperty arg0, String arg1,
			InputStream arg2, boolean arg3)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.uploadFile(organization, app, arg0, arg1, arg2, arg3);
	}

	@Override
	public VitalStatus validate() throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.ping();
	}


	@Override
	public VitalStatus addApp(VitalApp arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.addApp(organization, arg0);
	}


	@Override
	public VitalStatus addDatabaseConnection(VitalApp arg0,
			DatabaseConnection arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.addDatabaseConnection(organization, arg0, arg1);
	}


	@Override
	public VitalSegment addSegment(VitalApp arg0, VitalSegment arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.addSegment(organization, arg0, arg1, null, arg2);
	}
	

	@Override
	public VitalSegment addSegment(VitalApp app, VitalSegment segment,
			VitalProvisioning provisioning, boolean create)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.addSegment(organization, app, segment, provisioning, create);
	}


	@Override
	public VitalApp getApp(String arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.getApp(organization, arg0);
	}


	@Override
	public List<VitalApp> listApps() throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.listApps(organization);
	}


	@Override
	public VitalStatus removeApp(VitalApp arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.removeApp(organization, arg0);
	}


	@Override
	public VitalStatus removeDatabaseConnection(VitalApp arg0, String arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.removeDatabaseConnection(organization, arg0, arg1);
	}


	@Override
	public VitalStatus removeSegment(VitalApp arg0, VitalSegment arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.removeSegment(organization, arg0, arg1, arg2);
	}


	public void setName(String serviceName) {
		this.name = serviceName;
	}
		
	
	public VitalStatus shutdown() {
		return client.shutdown();
	}
	
	public List<GraphObject> listDatascripts(String path, boolean getBodies)
			throws VitalServiceException {
		return client.listDatascripts(organization, null, path, getBodies);
	}
	
	
	public GraphObject addDatascript(String path, String body) throws VitalServiceException {
		return client.addDatascript(organization, null, path, body);
	}

	public VitalStatus removeDatascript(String path) throws VitalServiceException {
		return client.removeDatascript(organization, null, path);
	}


	@Override
	public VitalStatus addVitalServiceKey(VitalApp app, VitalServiceKey serviceKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.addVitalServiceKey(organization, app, serviceKey);
	}


	@Override
	public List<VitalServiceKey> listVitalServiceKeys(VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listVitalServiceKeys(organization, app);
	}


	@Override
	public VitalStatus removeVitalServiceKey(VitalApp app, VitalServiceKey serviceKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.removeVitalServiceKey(organization, app, serviceKey);
	}


	@Override
	public VitalServiceConfig getConfig() {
		return this.config;
	}


	@Override
	public void setConfig(VitalServiceConfig config) {
		this.config = (VitalServicePrimeConfig) config;
	}


	@Override
	public VitalAuthKey getAuthKey() {
		return serviceAdminKey;
	}


	@Override
	public void setAuthKey(VitalAuthKey authKey) {

		if(authKey == null) throw new NullPointerException("Auth key must not be empty");
		
		if(authKey instanceof VitalServiceAdminKey) {
			this.serviceAdminKey = (VitalServiceAdminKey) authKey;
		} else {
			throw new RuntimeException("VitalServiceAdmin may only accept auth key of type: " + VitalServiceAdminKey.class.getCanonicalName() );
		}
		
	}
	
	/*
	@Override
	public VitalStatus ping() throws VitalServiceException,
	VitalServiceUnimplementedException {
		return client.ping();
	}
	
	
	@Override
	public VitalStatus close() throws VitalServiceUnimplementedException,
			VitalServiceException {
		// TODO Auto-generated method stub
		return null;
	}
	*/
	
	/*

	@Override
	protected VitalStatus _close() throws VitalServiceException,
	VitalServiceUnimplementedException {
		
		try {
			client.closeSession();
		} catch(Exception e) {
			log.warn(e.getLocalizedMessage())
		}
		
		return VitalStatus.OK
	}

	
	@Override
	public URIProperty generateURI(Class<? extends GraphObject> clazz)
			throws VitalServiceUnimplementedException, VitalServiceException {
		if(uriGenerationStrategy == URIGenerationStrategy.remote) {
			return client.generateURI(organization, app, clazz)
		}
		return super.generateURI(clazz);
	}

	@Override
	protected ResultList _callFunction(
			String function, Map<String, Object> arguments)
	throws VitalServiceException, VitalServiceUnimplementedException {
		return client.callFunction(organization, app, function, arguments);
	}

	@Override
	protected GraphObject _getServiceWide(
			URIProperty uri) throws VitalServiceException,
	VitalServiceUnimplementedException {
		return client.get(organization, app, uri);
	}

	@Override
	protected List<GraphObject> _getServiceWideList(
			List<URIProperty> uris) throws VitalServiceException,
	VitalServiceUnimplementedException {
		return client.getBatch(organization, app, uris);
	}

	@Override
	protected GraphObject _save(
			VitalSegment targetSegment, GraphObject graphObject)
	throws VitalServiceException, VitalServiceUnimplementedException {
		return client.save(organization, app, targetSegment, graphObject, true);
//		throw new VitalServiceUnimplementedException("NO LONGER IN USE")
	}

	@Override
	protected ResultList _saveList(
			VitalSegment targetSegment, List<GraphObject> graphObjectsList)
	throws VitalServiceException, VitalServiceUnimplementedException {
		return client.save(organization, app, targetSegment, graphObjectsList, true);
//		throw new VitalServiceUnimplementedException("NO LONGER IN USE")
	}

	@Override
	protected VitalStatus _delete(URIProperty uri)
	throws VitalServiceException, VitalServiceUnimplementedException {
		return client.delete(organization, app, uri);
	}

	@Override
	protected VitalStatus _deleteList(
			List<URIProperty> uris) throws VitalServiceException,
	VitalServiceUnimplementedException {
		return client.delete(organization, app, uris);
	}

	@Override
	protected ResultList _selectQuery(
			VitalSelectQuery query) throws VitalServiceException,
	VitalServiceUnimplementedException {
		return client.query(organization, app, query)
	}

	@Override
	protected ResultList _graphQuery(
			VitalGraphQuery gq) {
		return client.query(organization, app, gq);
	}

	@Override
	public VitalStatus uploadFile(URIProperty uri, String fileName,
			InputStream inputStream, boolean overwrite)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return client.uploadFile(organization, app, uri, fileName, inputStream, overwrite)
	}

	@Override
	public VitalStatus downloadFile(URIProperty uri, String fileName,
			OutputStream outputStream, boolean closeOutputStream)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return client.downloadFile(organization, app, uri, fileName, outputStream, closeOutputStream)
	}

	@Override
	public VitalStatus fileExists(URIProperty uri, String fileName)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return client.fileExists(organization, app, uri, fileName)
	}

	@Override
	public VitalStatus deleteFile(URIProperty uri, String fileName)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return client.deleteFile(organization, app, uri, fileName)
	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.VITALPRIME;
	}

	@Override
	public VitalStatus deleteObjects(List<GraphObject> l)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return client.deleteObjects(organization, app, l);
	}
			
	//forward save/insert requests
	/*
	@Override
	public ResultList insert(VitalSegment targetSegment, GraphObject graphObject)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.insert(organization, app, targetSegment, graphObject)
	}

	@Override
	public ResultList insert(VitalSegment targetSegment,
			List<GraphObject> graphObjectsList)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.insert(organization, app, targetSegment, graphObjectsList);
	}

	@Override
	public ResultList save(VitalSegment targetSegment, GraphObject graphObject,
			boolean create) throws VitalServiceUnimplementedException,
			VitalServiceException {
		if(targetSegment == null) throw new NullPointerException("targetSegment must not be null")
		return client.save(organization, app, targetSegment, graphObject, create)
	}

	@Override
	public ResultList save(VitalSegment targetSegment,
			List<GraphObject> graphObjectsList, boolean create)
			throws VitalServiceUnimplementedException, VitalServiceException {
		if(targetSegment == null) throw new NullPointerException("targetSegment must not be null")
		return client.save(organization, app, targetSegment, graphObjectsList, create)
	}
			
	@Override
	public ResultList save(GraphObject graphObject)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.save(organization, app, null, graphObject, false)
	}

	@Override
	public ResultList save(List<GraphObject> graphObjectsList)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.save(organization, app, null, graphObjectsList, false)
	}
    */
			
	//forward save/insert requests

			
	/*
	@Override
	public VitalStatus commitTransaction(Transaction transaction) {
		VitalStatus status = client.commitTransaction(transaction)
		if(status.status != VitalStatus.Status.ok) throw new RuntimeException("Vital service error status: ${status}")
		return status;
	}
			
	@Override
	public Transaction createTransaction() {
		return client.createTransaction()
	}

	@Override
	public List<Transaction> getTransactions() {
		return client.getTransactions()
	}

	@Override
	public VitalStatus rollbackTransaction(Transaction transaction) {
		VitalStatus status = client.rollbackTransaction(transaction);
		if(status.status != VitalStatus.Status.ok) throw new RuntimeException("Vital service error status: ${status}")
		return status;	
	}

	@Override
	public VitalStatus setTransaction(Transaction transaction) {
		return VitalStatus.withError("NOT IMPLEMENTED, use createTransaction")
	}

	@Override
	public VitalStatus validate() throws VitalServiceUnimplementedException,
			VitalServiceException {
		return ping();
	}

	@Override
	protected VITAL_GraphContainerObject _getExistingObjects(List<String> uris) {
		return client.getExistingObjects(organization, app, uris);
	}

	@Override
	public VitalStatus _bulkExport(VitalSegment arg0, OutputStream arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.bulkExport(organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus _bulkImport(VitalSegment arg0, InputStream arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.bulkImport(organization, app, arg0, arg1);
	}

	@Override
	public ResultList listDatabaseConnections()
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listDatabaseConnections(organization, app);
	}

	@Override
	protected ResultList _externalQuery(VitalQuery query) {
		return client.query(organization, app, query)
	}
	*/

	
}
