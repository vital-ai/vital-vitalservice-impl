package ai.vital.prime.service.superadmin;

import static ai.vital.vitalservice.VitalServiceConstants.NO_TRANSACTION;

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
import ai.vital.superadmin.domain.VitalServiceSuperAdminKey;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.ServiceOperations;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.IVitalServiceConfigAware;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.superadmin.VitalServiceSuperAdmin;
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
import groovy.lang.Closure;

public class VitalServiceSuperAdminPrime implements VitalServiceSuperAdmin, IVitalServiceConfigAware {

	IVitalPrimeClient client;

	private String name;

	private VitalServiceSuperAdminKey serviceSuperAdminKey;

	private VitalServicePrimeConfig config;
	
	private final static Logger log = LoggerFactory.getLogger(VitalServiceSuperAdminPrime.class);
	
	private QueueCallFunctionInterceptor queueCallFunctionInterceptor = new QueueCallFunctionInterceptor();
	
	private VitalServiceSuperAdminPrime(IVitalPrimeClient client) {

		this.client = client;
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
			
	public static VitalServiceSuperAdminPrime create(IVitalPrimeClient client) {

		return new VitalServiceSuperAdminPrime(client);
	}

	
	@Override
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment,
			OutputStream outputStream) throws VitalServiceUnimplementedException, VitalServiceException {
		return bulkExport(organization, app, segment, outputStream, null);
	}


	@Override
	public VitalStatus bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.bulkExport(organization, app, segment, outputStream, datasetURI);
	}

	@Override
	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return bulkImport(organization, app, segment, inputStream, "");
	}
	
	@Override
	public VitalStatus bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.bulkImport(organization, app, segment, inputStream, datasetURI);
	}
	

	@Override
	public ResultList callFunction(VitalOrganization organization, VitalApp app, String arg0, Map<String, Object> arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		
		ResultList rl = queueCallFunctionInterceptor.callFunction(client, organization, app, arg0, arg1);
		if(rl != null) return rl;
		
		return client.callFunction(organization, app, arg0, arg1);
	}

	boolean closed = false;
	
	@Override
	public VitalStatus close() throws VitalServiceUnimplementedException,
			VitalServiceException {
		
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
		
//		VitalServiceFactory.close(this);
		
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
	public VitalStatus delete(VitalOrganization organization, VitalApp app, URIProperty arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.delete(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.delete(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus delete(VitalOrganization organization, VitalApp app, List<URIProperty> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.delete(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.delete(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, URIProperty arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpanded(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpanded(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, URIProperty arg0, VitalPathQuery arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpanded(NO_TRANSACTION, organization, app, arg0, arg1);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty arg0, VitalPathQuery arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpanded(transaction, organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, List<URIProperty> arg0,
			VitalPathQuery arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.deleteExpanded(NO_TRANSACTION, organization, app, arg0, arg1);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> arg0,
			VitalPathQuery arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.deleteExpanded(transaction, organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus deleteExpandedObject(VitalOrganization organization, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpandedObject(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteExpandedObject(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteExpandedObject(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteExpandedObjects(VitalOrganization organization, VitalApp app, List<GraphObject> arg0,
			VitalPathQuery arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.deleteExpandedObjects(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteExpandedObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> arg0,
			VitalPathQuery arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.deleteExpandedObjects(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteFile(VitalOrganization organization, VitalApp app, URIProperty arg0, String arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteFile(organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus deleteObject(VitalOrganization organization, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteObject(NO_TRANSACTION, organization, app, arg0);
	}

	@Override
	public VitalStatus deleteObject(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteObject(transaction, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteObjects(VitalOrganization organization, VitalApp app, List<GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteObjects(NO_TRANSACTION, organization, app, arg0);
	}
	
	@Override
	public VitalStatus deleteObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.deleteObjects(transaction, organization, app, arg0);
	}

	@Override
	public VitalStatus doOperations(VitalOrganization organization, VitalApp app, ServiceOperations arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.doOperations(organization, app, arg0);
	}

	@Override
	public VitalStatus downloadFile(VitalOrganization organization, VitalApp app, URIProperty arg0, String arg1,
			OutputStream arg2, boolean arg3)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.downloadFile(organization, app, arg0, arg1, arg2, arg3);
	}

	@Override
	public VitalStatus fileExists(VitalOrganization organization, VitalApp app, URIProperty arg0, String arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.fileExists(organization, app, arg0, arg1);
	}

	@Override
	public URIProperty generateURI(VitalOrganization organization, VitalApp app, Class<? extends GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.generateURI(organization, app, arg0);
	}

	// get
	
	// default cache=true, for consistency always include GraphContext
	@Override
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.get(organization, app, graphContext, uri, true);
	}
		
	@Override
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, URIProperty uri, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.getImpl(organization, app, graphContext, Arrays.asList(uri), cache, null);
	}
		
	@Override
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, List<URIProperty> uris) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.get(organization, app, graphContext, uris, true);
	}
		
	@Override
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, List<URIProperty> uris, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.getImpl(organization, app, graphContext, uris, cache, null);
	}
		
		
	// GraphContext is redundant here as this call only used for containers, but kept for consistency
		
	@Override
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, URIProperty uri, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.getImpl(organization, app, graphContext, Arrays.asList(uri), true, containers);
	}
		
	@Override
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, List<URIProperty> uris, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.getImpl(organization, app, graphContext, uris, true, containers);
	}
		
		
	protected ResultList getImpl(VitalOrganization organization, VitalApp app, GraphContext graphContext, List<URIProperty> uris, boolean cache, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException {

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
	public ResultList getExpanded(VitalOrganization organization, VitalApp app, URIProperty arg0, boolean arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.getExpanded(organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList getExpanded(VitalOrganization organization, VitalApp app, URIProperty arg0, VitalPathQuery arg1,
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
	public VitalSegment getSegment(VitalOrganization organization, VitalApp app, String arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.getSegment(organization, app, arg0);
	}

	@Override
	public List<VitalTransaction> getTransactions()
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.getTransactions();
	}

	@Override
	public ResultList insert(VitalOrganization organization, VitalApp app, VitalSegment arg0, GraphObject arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.insert(NO_TRANSACTION, organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment arg0, GraphObject arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.insert(transaction, organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList insert(VitalOrganization organization, VitalApp app, VitalSegment arg0, List<GraphObject> arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.insert(NO_TRANSACTION, organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment arg0, List<GraphObject> arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.insert(transaction, organization, app, arg0, arg1);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList listDatabaseConnections(VitalOrganization organization, VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listDatabaseConnections(organization, app);
	}

	@Override
	public ResultList listFiles(VitalOrganization organization, VitalApp app, String arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listFiles(organization, app, arg0);
	}

	@Override
	public List<VitalSegment> listSegments(VitalOrganization organization, VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listSegments(organization, app);
	}
	
	@Override
	public ResultList listSegmentsWithConfig(VitalOrganization organization,
			VitalApp app) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.listSegmentsWithConfig(organization, app);
	}


	@Override
	public VitalStatus ping() throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.ping();
	}


	@Override
	public ResultList query(VitalOrganization organization, VitalApp app, VitalQuery arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.query(organization, app, arg0);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList queryContainers(VitalOrganization organization, VitalApp app, VitalQuery query,
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
	public ResultList queryLocal(VitalOrganization organization, VitalApp app, VitalQuery arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return VitalSigns.get().query(arg0);
	}

	@Override
	public VitalStatus rollbackTransaction(VitalTransaction arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.rollbackTransaction(arg0);
	}

	@Override
	public ResultList save(VitalOrganization organization, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(NO_TRANSACTION, organization, app, arg0);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(transaction, organization, app, arg0);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList save(VitalOrganization organization, VitalApp app, List<GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(NO_TRANSACTION, organization, app, arg0);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(transaction, organization, app, arg0);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList save(VitalOrganization organization, VitalApp app, VitalSegment arg0, GraphObject arg1, boolean arg2)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(NO_TRANSACTION, organization, app, arg0, arg1, arg2);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment arg0, GraphObject arg1, boolean arg2)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl = client.save(transaction, organization, app, arg0, arg1, arg2);
		addToCache(rl);
		return rl;
	}

	@Override
	public ResultList save(VitalOrganization organization, VitalApp app, VitalSegment arg0, List<GraphObject> arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		ResultList rl = client.save(NO_TRANSACTION, organization, app, arg0, arg1, arg2);
		addToCache(rl);
		return rl;
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment arg0, List<GraphObject> arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		ResultList rl = client.save(transaction, organization, app, arg0, arg1, arg2);
		addToCache(rl);
		return rl;
	}

	@Override
	public VitalStatus sendEvent(VitalOrganization organization, VitalApp app, VITAL_Event arg0, boolean arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.sendFlumeEvent(organization, app, arg0, arg1);
	}

	@Override
	public VitalStatus sendEvents(VitalOrganization organization, VitalApp app, List<VITAL_Event> arg0, boolean arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.sendFlumeEvents(organization, app, arg0, arg1);
	}


	@Override
	public VitalStatus uploadFile(VitalOrganization organization, VitalApp app, URIProperty arg0, String arg1,
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
	public VitalStatus addApp(VitalOrganization organization, VitalApp arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.addApp(organization, arg0);
	}


	@Override
	public VitalStatus addDatabaseConnection(VitalOrganization organization, VitalApp arg0,
			DatabaseConnection arg1) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.addDatabaseConnection(organization, arg0, arg1);
	}


	@Override
	public VitalSegment addSegment(VitalOrganization organization, VitalApp arg0, VitalSegment arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.addSegment(organization, arg0, arg1, null, arg2);
	}

	@Override
	public VitalSegment addSegment(VitalOrganization organization,
			VitalApp app, VitalSegment config, VitalProvisioning provisioning,
			boolean createIfNotExists)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.addSegment(organization, app, config, provisioning, createIfNotExists);
	}


	@Override
	public VitalApp getApp(VitalOrganization organization, String arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.getApp(organization, arg0);
	}


	@Override
	public List<VitalApp> listApps(VitalOrganization organization) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.listApps(organization);
	}


	@Override
	public VitalStatus removeApp(VitalOrganization organization, VitalApp arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.removeApp(organization, arg0);
	}


	@Override
	public VitalStatus removeDatabaseConnection(VitalOrganization organization, VitalApp arg0, String arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.removeDatabaseConnection(organization, arg0, arg1);
	}


	@Override
	public VitalStatus removeSegment(VitalOrganization organization, VitalApp arg0, VitalSegment arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return client.removeSegment(organization, arg0, arg1, arg2);
	}


	public void setName(String serviceName) {
		this.name = serviceName;
	}


	@Override
	public VitalStatus addOrganization(VitalOrganization organization)
			throws VitalServiceUnimplementedException, VitalServiceException {
		client.addOrganization(organization);
		return VitalStatus.withOK();
	}


	@Override
	public VitalStatus removeOrganization(VitalOrganization organization)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.removeOrganization(organization);
	}


	@Override
	public List<VitalOrganization> listOrganizations()
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listOrganizations();
	}


	@Override
	public VitalOrganization getOrganization(String organizationID)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.getOrganization(organizationID);
	}


	@Override
	public VitalStatus addVitalServiceKey(VitalOrganization organization,
			VitalApp app, VitalServiceKey serviceKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.addVitalServiceKey(organization, app, serviceKey);
	}


	@Override
	public VitalStatus removeVitalServiceKey(VitalOrganization organization,
			VitalApp app, VitalServiceKey serviceKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.removeVitalServiceKey(organization, app, serviceKey);
	}


	@Override
	public List<VitalServiceKey> listVitalServiceKeys(
			VitalOrganization organization, VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listVitalServiceKeys(organization, app);
	}


	@Override
	public VitalStatus addVitalServiceAdminKey(VitalOrganization organization,
			VitalServiceAdminKey serviceAdminKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.addVitalServiceAdminKey(organization, null, serviceAdminKey);
	}


	@Override
	public VitalStatus removeVitalServiceAdminKey(
			VitalOrganization organization, VitalServiceAdminKey serviceAdminKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.removeVitalServiceAdminKey(organization, null, serviceAdminKey);
	}


	@Override
	public List<VitalServiceAdminKey> listVitalServiceAdminKeys(
			VitalOrganization organization)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return client.listVitalServiceAdminKeys(organization, null);
	}


	@Override
	public VitalServiceConfig getConfig() {
		return config;
	}


	@Override
	public void setConfig(VitalServiceConfig config) {
		this.config = (VitalServicePrimeConfig) config;
	}


	@Override
	public VitalAuthKey getAuthKey() {
		return this.serviceSuperAdminKey;
	}


	@Override
	public void setAuthKey(VitalAuthKey authKey) {

		if(authKey == null) throw new NullPointerException("Auth key must not be empty");
		
		if(authKey instanceof VitalServiceSuperAdminKey) {
			this.serviceSuperAdminKey = (VitalServiceSuperAdminKey) authKey;
		} else {
			throw new RuntimeException("VitalServiceSuperAdmin may only accept auth key of type: " + VitalServiceSuperAdminKey.class.getCanonicalName() );
		}
		
	}
		


	public List<GraphObject> listDatascripts(String path, boolean getBodies)
		throws VitalServiceException {
		return client.listDatascripts(null, null, path, getBodies);
	}

	public GraphObject addDatascript(String path, String body) throws VitalServiceException {
		return client.addDatascript(null, null, path, body);
	}

	public VitalStatus removeDatascript(String path) throws VitalServiceException {
		return client.removeDatascript(null, null, path);
	}
	
	
}
