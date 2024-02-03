package ai.vital.vitalservice.superadmin.impl;

import static ai.vital.vitalservice.VitalServiceConstants.NO_TRANSACTION;
import groovy.lang.Closure;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.lucene.model.LuceneSegment;
import ai.vital.service.lucene.impl.LuceneServiceQueriesImpl;
import ai.vital.vitalservice.DeleteOperation;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.InsertOperation;
import ai.vital.vitalservice.ServiceOperations;
import ai.vital.vitalservice.UpdateOperation;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.config.URIGenerationStrategy;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsImplementation;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.IVitalServiceConfigAware;
import ai.vital.vitalservice.impl.ServiceOperationsImplementation;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.impl.TransactionsImplementation;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalservice.impl.query.PathQueryHelperImpl;
import ai.vital.vitalservice.impl.query.PathQueryImplementation;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExternalSparqlQuery;
import ai.vital.vitalservice.query.VitalExternalSqlQuery;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalservice.superadmin.VitalServiceSuperAdmin;
import ai.vital.vitalservice.superadmin.factory.VitalServiceSuperAdminFactory;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.CompactStringSerializer;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.DatabaseConnection;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.RDFStatement;
import ai.vital.vitalsigns.model.VITAL_Event;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.ValidationStatus;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalAuthKey;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.superadmin.domain.VitalServiceSuperAdminKey;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.container.GraphObjectsIterable;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.uri.URIGenerator;



/**
 * Common vital service implementation class, encapsulates common functionality
 *
 */

public abstract class AbstractVitalServiceSuperAdminImplementation implements VitalServiceSuperAdmin, IVitalServiceConfigAware {

	private final static Logger log = LoggerFactory.getLogger(AbstractVitalServiceSuperAdminImplementation.class);
	

//	protected Transaction currentTransaction;
	
	protected TransactionsImplementation transactionsImplementation;
	
	protected URIGenerationStrategy uriGenerationStrategy = URIGenerationStrategy.local;
	
	protected DatabaseConnectionsImplementation dbConnectionsImpl = new DatabaseConnectionsImplementation();

	protected String name;
	
	boolean closed = false;
	
	protected SystemSegment systemSegment;
	
	protected VitalServiceConfig config;
	
	/**
	 * This flag controls whether global cache interaction should be enabled or not, true by default
	 */
	boolean useCache = true;

	private VitalServiceSuperAdminKey serviceSuperAdminKey;

	public AbstractVitalServiceSuperAdminImplementation(SystemSegment systemSegment) {
		this.systemSegment = systemSegment;
		this.transactionsImplementation = new TransactionsImplementation(new VitalServiceSuperAdminTransactionsExecutor(this));
	}

	protected void addToCache(ResultList rl) {
		if(useCache) {
			List<GraphObject> gs = new ArrayList<GraphObject>(rl.getResults().size());
			for(ResultElement re : rl.getResults()) {
				gs.add(re.getGraphObject());
			}

			VitalSigns.get().addToCache(gs);
		}

	}

	private void addToCache(List<GraphObject> gos) {

		if(useCache && gos != null && gos.size() > 0) {
			VitalSigns.get().addToCache(gos);
		}

	}
	

	// info about service connection
	
	//	public EndpointType getEndpointType() 
	
	
	// connection status
	@Override
	public VitalStatus validate() throws VitalServiceUnimplementedException, VitalServiceException {
		return ping();
	}
	
//	public VitalStatus ping()
	
	@Override
	public VitalStatus close() throws VitalServiceUnimplementedException, VitalServiceException {
		
		if(closed) return VitalStatus.withOKMessage("already closed");
		
		systemSegment.close();
		
		closed = true;
		
		VitalServiceSuperAdminFactory.close(this);
		
//		if(currentTransaction != null) {
//			try {
//				rollbackTransaction(currentTransaction);
//			} catch(Exception e) {}
//		}
		transactionsImplementation.rollbackAllTransactions();
		
		try {
			return _close();
		} catch (Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
	}

	protected abstract VitalStatus _close()  throws VitalServiceException,
	VitalServiceUnimplementedException;
	
	// service containers: segments, apps, organizations
	

	//implemented by subclasses
//	public VitalSegment addSegment(App app, VitalSegment config, boolean createIfNotExists) throws VitalServiceUnimplementedException, VitalServiceException
//	
//	public VitalStatus removeSegment(App app, VitalSegment segment, boolean deleteData) throws VitalServiceUnimplementedException, VitalServiceException
//
//	public List<App> listApps() throws VitalServiceUnimplementedException, VitalServiceException
//	
//	public VitalStatus addApp(App app) throws VitalServiceUnimplementedException, VitalServiceException
//	
//	public VitalStatus removeApp(App app) throws VitalServiceUnimplementedException, VitalServiceException
	
	
	// URIs
	@Override
	public URIProperty generateURI(VitalOrganization organization, VitalApp app, Class<? extends GraphObject> clazz) throws VitalServiceUnimplementedException, VitalServiceException {
		
		if(uriGenerationStrategy != URIGenerationStrategy.local) {
			log.warn("this endpoint implements only local uriGenerationStrategy, ignoring: " + uriGenerationStrategy);
		}
		
		return URIProperty.withString( URIGenerator.generateURI(app, clazz) );
	}
	
	
	
	// Transactions
	//transaction hooks
//	protected void _commitTransaction(Transaction transaction) throws Exception {
//		throw new RuntimeException("Endpoint: " + this.getEndpointType() + " does not support transactions");
//	}
//
//	protected void _createTransaction(Transaction transaction) throws Exception {
//		throw new RuntimeException("Endpoint: " + this.getEndpointType() + " does not support transactions");
//	}
//
//	protected void _rollbackTransaction(Transaction transaction) throws Exception {
//		throw new RuntimeException("Endpoint: " + this.getEndpointType() + " does not support transactions");
//	}
	
	
	protected void _commitTransaction(TransactionWrapper transaction) throws VitalServiceException {
		throw new RuntimeException("Endpoint: " + this.getEndpointType() + " does not support transactions");
	}
	
	protected void _transactionsCheck() throws VitalServiceUnimplementedException {
		throw new RuntimeException("Endpoint: " + this.getEndpointType() + " does not support transactions");
	}

	protected void _createTransaction(VitalTransaction transaction) throws VitalServiceException {
		throw new RuntimeException("Endpoint: " + this.getEndpointType() + " does not support transactions");
	}

	protected void _rollbackTransaction(TransactionWrapper transaction) throws VitalServiceException {
		throw new RuntimeException("Endpoint: " + this.getEndpointType() + " does not support transactions");
	}
	
	
	@Override
	public VitalTransaction createTransaction() throws VitalServiceUnimplementedException, VitalServiceException {

//		if(currentTransaction != null) throw new RuntimeException("An active transaction detected, commit or rollback it first.");
//
//		Transaction transaction = new Transaction();
//		transaction.setState(TransactionState.OPEN);
//
//		try {
//			_createTransaction(transaction);
//		} catch (Exception e) {
//			throw new VitalServiceException(e);
//		}
//
//		this.currentTransaction = transaction;
//
//		return transaction;

		return transactionsImplementation.createTransaction();
	}

	@Override
	public VitalStatus commitTransaction(VitalTransaction transaction) throws VitalServiceUnimplementedException, VitalServiceException {

//		if(currentTransaction == null) throw new RuntimeException("No active transaction");
//
//		if(!currentTransaction.getID().equals(transaction.getID())) 
//			throw new RuntimeException("Current transaction is different than provided: " + currentTransaction.getID() + 
//					" vs " + transaction.getID());
//
//		try {
//			
//			_commitTransaction(transaction);
//			transaction.setState(TransactionState.COMMITTED);
//			currentTransaction = null;
//			return VitalStatus.withOKMessage("Transaction committed");
//			
//		} catch(Exception e) {
//			return VitalStatus.withError(e.getLocalizedMessage());
//		}
		
		try {
			return transactionsImplementation.commitTransaction(transaction);
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
		

	}
	
	@Override
	public VitalStatus rollbackTransaction(VitalTransaction transaction) throws VitalServiceUnimplementedException, VitalServiceException {
		
//		if(currentTransaction == null) throw new RuntimeException("No active transaction");
//
//		if(!currentTransaction.getID().equals(transaction.getID())) 
//			throw new RuntimeException("Current transaction is different than provided: " + currentTransaction.getID() + " vs " + transaction.getID());
//
//		try {
//			_rollbackTransaction(transaction);
//			transaction.setState(TransactionState.ROLLED_BACK);
//			currentTransaction = null;
//			return VitalStatus.withOKMessage("Transaction rolled back");
//		} catch(Exception e) {
//			return VitalStatus.withError(e.getLocalizedMessage());
//		}
		
		
		try {
			return transactionsImplementation.rollbackTransaction(transaction);
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
		

	}
	

	@Override
	public List<VitalTransaction> getTransactions() throws VitalServiceUnimplementedException, VitalServiceException {
//		if(currentTransaction != null) return Arrays.asList(this.currentTransaction);
//		return Collections.emptyList();
		return transactionsImplementation.listTransactions();
	}
	
	
	
	// crud operations
	
	// get
		
	// default cache=true, for consistency always include GraphContext
	@Override
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.get(organization, app, graphContext, uri, true);
	}
		
	@Override
	public ResultList get(VitalOrganization organization, VitalApp app, GraphContext graphContext, URIProperty uri, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException {
		return getImpl(organization, app, graphContext, Arrays.asList(uri), cache, null);
		
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
				gos = _getServiceWideList(organization, app, uris);
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
	
	protected abstract List<GraphObject> _getServiceWideList(VitalOrganization organization, VitalApp app, List<URIProperty> uris) throws VitalServiceException, VitalServiceUnimplementedException;
	
	protected abstract GraphObject _getServiceWide(VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceException, VitalServiceUnimplementedException;
	
	
	
	// delete
	@Override
	public VitalStatus delete(VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException {
		return delete(NO_TRANSACTION, organization, app, uri);
	}
	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException {
		if(uri.get().startsWith( URIProperty.MATCH_ALL_PREFIX ) ) {
			
			if(transaction != null && transaction != NO_TRANSACTION) throw new VitalServiceException("clearing a segment is not a transactional operation");
			
			String segmentID = uri.get().substring(URIProperty.MATCH_ALL_PREFIX.length());
			if(segmentID.length() < 1) throw new VitalServiceException("No segment ID provided in special match-all case!");
			
			VitalSegment segment = systemSegment.getSegment(organization, app, segmentID);
			if(segment == null) throw new VitalServiceException("Segment not found: " + segmentID + ", cannot purge it");
			
			return _deleteAll(organization, app, segment);
			//purge cach
		}
		
		return this.delete(transaction, organization, app, Arrays.asList(uri));
	}
	
	protected abstract VitalStatus _deleteAll(VitalOrganization organization, VitalApp app, VitalSegment segment) throws VitalServiceException, VitalServiceUnimplementedException;
	
	@Override
	public VitalStatus delete(VitalOrganization organization, VitalApp app, List<URIProperty> uris) throws VitalServiceUnimplementedException, VitalServiceException {
		return delete(NO_TRANSACTION, organization, app, uris);
	}
	
	@Override
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris) throws VitalServiceUnimplementedException, VitalServiceException {
		
		if(uris == null || uris.size() < 1) throw new RuntimeException("Null or empty uris list");
		
		List<String> urisList = new ArrayList<String>();
		for(URIProperty u : uris) {
			urisList.add(u.get());
		}
		
		VITAL_GraphContainerObject existingObjects = _getExistingObjects(organization, app, urisList); 
//		
		List<URIProperty> tempList = new ArrayList<URIProperty>();
		
		List<URIProperty> failedURIs = new ArrayList<URIProperty>();
		
		for(String u : urisList) {
		
			if(existingObjects.getProperty(u) != null) {
				tempList.add(URIProperty.withString(u));
			} else {
				failedURIs.add(URIProperty.withString(u));
			}
			
		}
		
//		List<URIProperty> newList = new ArrayList<URIProperty>(uris);
		
		VitalStatus status = VitalStatus.withOK();
		
		if(tempList.size() > 0) {
			try {
				status = _deleteList(transaction, organization, app, tempList);
				if(status.getStatus() != VitalStatus.Status.ok) {
					return status;
				}
			} catch (Exception e) {
				return VitalStatus.withError(e.getLocalizedMessage());
			}
		}
		
		if(status.getStatus() == VitalStatus.Status.ok) {
			for(URIProperty uri : tempList) {
				VitalSigns.get().removeFromCache(uri.get());
				if(transaction != null && transaction != NO_TRANSACTION) {
					DeleteOperation dop = new DeleteOperation();
					dop.setOrganization(organization);
					dop.setGraphObjectURI(uri);
					dop.setApp(app);
					transactionsImplementation.addTransactionOperation(transaction, dop);
				}
//				if(this.currentTransaction != null) {
//					DeleteOperation dop = new DeleteOperation();
//					dop.setOrganization(organization);
//					dop.setGraphObjectURI(uri);
//					dop.setApp(app);
//					this.currentTransaction.getOperations().add(dop);
//				}
			}
		}
		
		status.setSuccesses(tempList.size());
		status.setErrors(failedURIs.size());
		status.setFailedURIs(failedURIs);
		
		status.setMessage("Deleted: " + tempList.size() + ", not found: " + failedURIs.size());
		
		return status;
		
	}
	
	protected abstract VitalStatus _deleteList(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris)  throws VitalServiceException,
	VitalServiceUnimplementedException;
	
	
	@Override
	public VitalStatus deleteObject(VitalOrganization organization, VitalApp app, GraphObject object) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.deleteObject(NO_TRANSACTION, organization, app, object);
	}
	
	@Override
	public VitalStatus deleteObject(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject object) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.delete(transaction, organization, app, URIProperty.withString(object.getURI()));
	}
	
	@Override
	public VitalStatus deleteObjects(VitalOrganization organization, VitalApp app, List<GraphObject> objects) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteObjects(NO_TRANSACTION, organization, app, objects);
	}
	@Override
	public VitalStatus deleteObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> objects) throws VitalServiceUnimplementedException, VitalServiceException {
		List<URIProperty> uris = new ArrayList<URIProperty>(objects.size());
		for(GraphObject g : objects) {
			uris.add(URIProperty.withString(g.getURI()));
		}
		return this.delete(transaction, organization, app, uris);
	}
	
	
	// insert, must be a new object
	@Override
	public ResultList insert(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject) throws VitalServiceUnimplementedException, VitalServiceException {
		return insert(NO_TRANSACTION, organization, app, targetSegment, graphObject);
	}
	
	@Override
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject) throws VitalServiceUnimplementedException, VitalServiceException {
		if(targetSegment == null) throw new NullPointerException("Null targetSegment");
		if(graphObject == null) throw new NullPointerException("Null graphObject");
		if(graphObject.getURI() == null) throw new NullPointerException("Null graphObject URI");
		return this.insert(transaction, organization, app, targetSegment, Arrays.asList(graphObject));
	}

	@Override
	public ResultList insert(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException {
		return insert(NO_TRANSACTION, organization, app, targetSegment, graphObjectsList);
	}
	
	@Override
	public ResultList insert(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException {
		if(targetSegment == null) throw new NullPointerException("Null targetSegment");
		if(graphObjectsList == null) throw new NullPointerException("Null graphObjectsList");
		if(graphObjectsList.size() < 1) throw new RuntimeException("Empty graphObjectsList");
		
		String suri = targetSegment.getURI();
		targetSegment = systemSegment.getSegmentByURI(organization, app, targetSegment);
		if(targetSegment == null) throw new RuntimeException("Segment with URI: " + suri + " not found");
				
		List<String> urisList = new ArrayList<String>();
		for(GraphObject graphObject : graphObjectsList) {
			if(graphObject instanceof RDFStatement) {
				throw new RuntimeException("This endpoint does not support inserting RDFStatements: " + getEndpointType());
			}
			if(graphObject == null) throw new NullPointerException("Null graphObject");
			if(graphObject.getURI() == null) throw new NullPointerException("Null graphObject URI");
			ValidationStatus vs = graphObject.validate();
			if(vs.getStatus() != ValidationStatus.Status.ok) {
				throw new RuntimeException("Graph object validation failed: " + vs.getErrors());
			}
			if(vs.getUriResponse()._transient) {
				throw new RuntimeException("Cannot persist object with transient URI: " + graphObject.getURI());
			}
			if(urisList.contains(graphObject.getURI())) {
				throw new RuntimeException("more than 1 graph object with same uri in input list: " + graphObject.getURI());
			}
			urisList.add(graphObject.getURI());
		}
		
		
		//low level code should return VITAL_GraphContainObject with URI->Segment(String) pairs
		VITAL_GraphContainerObject existingObjects = _getExistingObjects(organization, app, urisList); 
		
		
		
		List<URIProperty> failedURIs = new ArrayList<URIProperty>();
		
		int ok = 0;
		
		List<GraphObject> newList = new ArrayList<GraphObject>(graphObjectsList);
		for(Iterator<GraphObject> iter = newList.iterator(); iter.hasNext();) {
			GraphObject g = iter.next();
			if(existingObjects.getProperty(g.getURI()) != null) {
				iter.remove();
				failedURIs.add(URIProperty.withString(g.getURI()));
			} else {
			}
		}

		ResultList rl = new ResultList();
		
		if(newList.size() > 0) {
		
			try {
				rl = _saveList(transaction, organization, app, targetSegment, newList);
				ok = rl.getResults().size();
//				if(rl.getResults().size() != ok)
			} catch(Exception e) {
				rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
				return rl;
			}
			
		}
		
		VitalStatus status = VitalStatus.withOK();
		status.setFailedURIs(failedURIs);
		status.setErrors(failedURIs.size());
		status.setSuccesses(ok);
		status.setMessage("Inserted " + ok + " object(s), already existing object(s) (" + failedURIs.size() + "): " + failedURIs);
		rl.setStatus(status);
		
//		if(currentTransaction != null) {
//			for(GraphObject graphObject : rl) {
//				InsertOperation io = new InsertOperation();
//				io.setApp(app);
//				io.setGraphObject(graphObject);
//				io.setOrganization(organization);
//				io.setSegment(targetSegment);
//				this.currentTransaction.getOperations().add(io);
//			}
//		}
		if(transaction != null && transaction != NO_TRANSACTION) {
			for(GraphObject graphObject : rl) {
				InsertOperation io = new InsertOperation();
				io.setApp(app);
				io.setGraphObject(graphObject);
				io.setOrganization(organization);
				io.setSegment(targetSegment);
				transactionsImplementation.addTransactionOperation(NO_TRANSACTION, io);
			}
		}

		addToCache(rl);
			
		return rl;
		
	}
	
	protected abstract ResultList _saveList(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException;

	protected abstract VITAL_GraphContainerObject _getExistingObjects(VitalOrganization organization, VitalApp app, List<String> uris) throws VitalServiceException;
	
	
	// save, if create=true, create a new object if it doesn't exist
	@Override
	public ResultList save(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject, boolean create) throws VitalServiceUnimplementedException, VitalServiceException {
		return save(NO_TRANSACTION, organization, app, targetSegment, graphObject, create);
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject, boolean create) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.save(transaction, organization, app, targetSegment, Arrays.asList(graphObject), create);
	}

	@Override
	public ResultList save(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList, boolean create) throws VitalServiceUnimplementedException, VitalServiceException {
		return save(NO_TRANSACTION, organization, app, targetSegment, graphObjectsList, create);
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList, boolean create) throws VitalServiceUnimplementedException, VitalServiceException {
		
		//just repeat the old update/insert behaviour?
		if(targetSegment == null) throw new NullPointerException("Target segment cannot be null");
		if(graphObjectsList == null || graphObjectsList.size() < 1) throw new NullPointerException("GraphObjects list cannot be null or empty");

		String suri = targetSegment.getURI();
		targetSegment = systemSegment.getSegmentByURI(organization, app, targetSegment);
		if(targetSegment == null) throw new RuntimeException("Segment with URI: " + suri + " not found");
		
		List<String> urisList = new ArrayList<String>();

		for(GraphObject graphObject : graphObjectsList) {
			if(graphObject instanceof RDFStatement) {
				throw new RuntimeException("Cannot save/update RDFStatement, use insert method instead");
			}
			ValidationStatus vs = graphObject.validate();
			if(vs.getStatus() != ValidationStatus.Status.ok) {
				throw new RuntimeException("Graph object validation failed: " + vs.getErrors());
			}
			if(vs.getUriResponse()._transient) {
				throw new RuntimeException("Cannot persist object with transient URI: " + graphObject.getURI());
			}
			if(urisList.contains(graphObject.getURI())) {
				throw new RuntimeException("more than 1 graph object with same uri in input list: " + graphObject.getURI());
			}
			
			urisList.add(graphObject.getURI());
			
		}
		
		
		VITAL_GraphContainerObject existingObjects = _getExistingObjects(organization, app, urisList); 
		
		int mislocated = 0;
		
		int notExisting = 0;
		
		int toInsert = 0;
		
		int toUpdate = 0;
		
		List<URIProperty> failedURIs = new ArrayList<URIProperty>();
		
		List<GraphObject> newList = new ArrayList<GraphObject>(graphObjectsList);
		
		String sid = (String) targetSegment.getRaw(Property_hasSegmentID.class);
		
		for(Iterator<GraphObject> iter = newList.iterator(); iter.hasNext(); ) {
			
			GraphObject g = iter.next();
			
			StringProperty existingSegment = (StringProperty) existingObjects.getProperty(g.getURI());
			
			if(existingSegment == null) {
				
				if(!create) {
					notExisting++;
					iter.remove();
					failedURIs.add(URIProperty.withString(g.getURI()));
				} else {
					toInsert ++;
				}
				
			} else {
				
				if(existingSegment.toString().equals(sid)) {
					toUpdate++;
				} else {
					mislocated++;
					iter.remove();
					failedURIs.add(URIProperty.withString(g.getURI()));
					
				}
				
			}
			
		}
		
		
		ResultList rl = new ResultList();
		
		int ok = 0;
		
		if(newList.size() > 0 ) {
			try {
				rl = _saveList(transaction, organization, app, targetSegment, newList);
				ok = rl.getResults().size();
			} catch (Exception e) {
				rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
				return rl;
			}
		}
		
		
		VitalStatus status = VitalStatus.withOK();
		status.setFailedURIs(failedURIs);
		status.setErrors(failedURIs.size());
		status.setSuccesses(ok);
		
		String message = "";
		if(create) {
			message = "Inserted: " + toInsert + ", ";
		}
		
		message += ( "Updated: " + toUpdate + ", " );
		
		if(!create) {
			message += ( "Not found: " + notExisting + ", " );
		}
		
		message += ("Mislocated: " + mislocated);
		
		status.setMessage(message);
		rl.setStatus(status);
		
		addToCache(rl);
		

		if(transaction != null && transaction != NO_TRANSACTION) {
			for(GraphObject graphObject : graphObjectsList) {
				UpdateOperation uo = new UpdateOperation();
				uo.setApp(app);
				uo.setGraphObject(graphObject);
				uo.setOrganization(organization);
				uo.setSegment(targetSegment);
				transactionsImplementation.addTransactionOperation(transaction, uo);
			}
		}
//		if(currentTransaction != null) {
//			for(GraphObject graphObject : graphObjectsList) {
//				UpdateOperation uo = new UpdateOperation();
//				uo.setApp(app);
//				uo.setGraphObject(graphObject);
//				uo.setOrganization(organization);
//				uo.setSegment(targetSegment);
//				this.currentTransaction.getOperations().add(uo);
//			}
//		}

		return rl;
		
	}
	
	
	// object(s) must already exist, determine current segment & update
	@Override
	public ResultList save(VitalOrganization organization, VitalApp app, GraphObject graphObject) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.save(NO_TRANSACTION, organization, app, graphObject);
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject graphObject) throws VitalServiceUnimplementedException, VitalServiceException {
		return this.save(organization, app, Arrays.asList(graphObject));
	}
	
	@Override
	public ResultList save(VitalOrganization organization, VitalApp app, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException {
		return save(NO_TRANSACTION, organization, app, graphObjectsList);
	}
	
	@Override
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> graphObjectsList) throws VitalServiceUnimplementedException, VitalServiceException {
		
		if(graphObjectsList == null || graphObjectsList.size() < 1) throw new NullPointerException("GraphObjects list cannot be null or empty");

		List<String> urisList = new ArrayList<String>();

		for(GraphObject graphObject : graphObjectsList) {
			if(graphObject instanceof RDFStatement) {
				throw new RuntimeException("Cannot save/update RDFStatement, use insert method instead");
			}
			ValidationStatus vs = graphObject.validate();
			if(vs.getStatus() != ValidationStatus.Status.ok) {
				throw new RuntimeException("Graph object validation failed: " + vs.getErrors());
			}
			if(vs.getUriResponse()._transient) {
				throw new RuntimeException("Cannot persist object with transient URI: " + graphObject.getURI());
			}
			if(urisList.contains(graphObject.getURI())) {
				throw new RuntimeException("more than 1 graph object with same uri in input list: " + graphObject.getURI());
			}
			
			urisList.add(graphObject.getURI());
			
		}
		
		Map<String, VitalSegment> segmentsMap = systemSegment.listSegmentID2Map(organization, app);
		
		VITAL_GraphContainerObject existingObjects = _getExistingObjects(organization, app, urisList); 
		
		List<GraphObject> newList = new ArrayList<GraphObject>(graphObjectsList);
		
		List<URIProperty> failedURIs = new ArrayList<URIProperty>();
		
		int notFound = 0;
		
		Map<String, List<GraphObject>> segmentToList = new HashMap<String, List<GraphObject>>();
		
		for(Iterator<GraphObject> iter = newList.iterator(); iter.hasNext(); ) {
			
			GraphObject g = iter.next();
			
			StringProperty segment = (StringProperty) existingObjects.getProperty(g.getURI());
			
			if(segment != null) {
				
				List<GraphObject> l = segmentToList.get(segment.toString());
				if(l == null) {
					l = new ArrayList<GraphObject>();
					segmentToList.put(segment.toString(), l);
				}
				
				l.add(g);
				
			} else {
				iter.remove();
				notFound++;
				failedURIs.add(URIProperty.withString(g.getURI()));
			}
			
		}
		
		ResultList rl = new ResultList();
		
		int errors = 0;
		
		for(Entry<String, List<GraphObject>> entry : segmentToList.entrySet()) {
			
			VitalSegment segment = segmentsMap.get(entry.getKey());
			
			if(segment == null) throw new RuntimeException("Segment not found: " + entry.getKey());
			
			try {
				ResultList subRes = _saveList(transaction, organization, app, segment, entry.getValue());
				
				if(transaction != null && transaction != NO_TRANSACTION) {
					
					for(GraphObject g : subRes) {
						
						UpdateOperation uo = new UpdateOperation();
						uo.setApp(app);
						uo.setGraphObject(g);
						uo.setOrganization(organization);
						uo.setSegment(segment);
						transactionsImplementation.addTransactionOperation(transaction, uo);

					}
					
				}
				
				for(GraphObject s : subRes) {
					rl.getResults().add(new ResultElement(s, 1D));
				}
			} catch(Exception e) {
				for(GraphObject g : entry.getValue()) {
					failedURIs.add(URIProperty.withString(g.getURI()));
					errors++;
				}
			}
			
		}
		
		VitalStatus status = VitalStatus.withOK();
		status.setErrors(failedURIs.size());
		status.setSuccesses(rl.getResults().size());
		status.setMessage("Updated: " + status.getSuccesses() + ", not found: " + notFound + ", errors: " + errors);
		status.setFailedURIs(failedURIs);
		rl.setStatus(status);
		
		addToCache(rl);
		
		return rl;
		
	}

	

	@Override
	public VitalStatus doOperations(VitalOrganization organization, VitalApp app, ServiceOperations operations) throws VitalServiceUnimplementedException, VitalServiceException {
		//verify operations
		try {
			return new ServiceOperationsImplementation(new ServiceOperationExecutorSuperAdminImpl(this, organization, app), operations).execute();
		} catch (Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
		
	}
	
	

	@Override
	public ResultList callFunction(VitalOrganization organization, VitalApp app, String function,
			Map<String, Object> arguments) throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList rl;
		try {
			rl = _callFunction(organization, app, function, arguments);
		} catch (Exception e) {
			rl = new ResultList();
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
		}
		//do not put results into cache
		//addToCache(rl)
		return rl;
	}

	/**
	 * The real call function implementaion, it does not put objects n	
	 * @param function
	 * @param arguments
	 * @return
	 * @throws VitalServiceException
	 * @throws VitalServiceUnimplementedException
	 */
	protected abstract ResultList _callFunction(VitalOrganization organization, VitalApp app, String function,
	Map<String, Object> arguments) throws VitalServiceException,
	VitalServiceUnimplementedException;

	
	// ServiceWide
	@Override
	public ResultList query(VitalOrganization organization, VitalApp app, VitalQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
//		throw new VitalServiceUnimplementedException("NOT IMPLEMENTED");
		
		try {
			
			String txID = query.getBlockOnTransactionID();
			if(txID != null) {
				transactionsImplementation.blockOnTransactionID(txID);
			} 
			
			if( query instanceof VitalExternalSparqlQuery || query instanceof VitalExternalSqlQuery ) {
				
				if(dbConnectionsImpl != null) {
					return dbConnectionsImpl.query(organization, app, query);
				}
				
				return _externalQuery(organization, app,query);
				
			}
			
			
			query = (VitalQuery) query.clone();
			
		} catch(Exception e) {
			ResultList rl = new ResultList();
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
			return rl;
		}
		
		return new SuperAdminServiceQueryImplementation(systemSegment, query, this, organization, app).handleQuery();
			/*
			if( VitalSegment.isAllSegments(query.getSegments()) ) {
				query.setSegments(listSegments(organization, app));
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
	
//				String localSegment = LocalSegmentsQueryFilter.checkIfLocalQuery(query.getSegments());
				ResultList rl = null;
				//filter removed
//				if(localSegment != null) {
//					if(query.isReturnSparqlString()) {
//						throw new VitalServiceException("Local segment queries do not return sparl query strings");
//					}
//					rl = VitalSigns.get().doGraphQuery(localSegment, (VitalGraphQuery) query);
//				} else {
					rl = _graphQuery(organization, app, (VitalGraphQuery) query);
//				}
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
//				String localSegment = LocalSegmentsQueryFilter.checkIfLocalQuery(query.getSegments());
				ResultList rl = null;
				//filter removed
//				if(localSegment != null) {
//					if(query.isReturnSparqlString()) {
//						throw new VitalServiceException("Local segment queries do not return sparl query strings");
//					}
//					rl = VitalSigns.get().doSelectQuery(localSegment, (VitalSelectQuery) query);
//				} else {
					rl = _selectQuery(organization, app, (VitalSelectQuery) query);
//				}
				addToCache(rl);
				return rl;
			} else if(query instanceof VitalSparqlQuery) {
				
				return _sparqlQuery(organization, app, (VitalSparqlQuery)query);
				
			} else if(query instanceof VitalPathQuery) {
				
				return new PathQueryImplementation((VitalPathQuery) query, new VitalServiceSuperAdminPathQueryExecutor(this, organization, app)).execute();
				
			} else {
				throw new VitalServiceUnimplementedException("Unhandled query type: " + query.getClass().getCanonicalName());
			}
			} catch(Exception e) {
				ResultList rl = new ResultList();
				rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
				return rl;
			}
			 */
		
	}
	
	
	protected ResultList _externalQuery(VitalOrganization organization, VitalApp app, VitalQuery query) {
		throw new RuntimeException("This method should not be called by local endpoints!");
	}

	protected ResultList _sparqlQuery(VitalOrganization organization, VitalApp app, VitalSparqlQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		throw new VitalServiceUnimplementedException("Endpoint " + getEndpointType() + " does not implement sparql qeries");
	}
	
	// Local
	@Override
	public ResultList queryLocal(VitalOrganization organization, VitalApp app, VitalQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		return VitalSigns.get().query(query);
	}
	
	// Containers
	@Override
	public ResultList queryContainers(VitalOrganization organization, VitalApp app, VitalQuery query, List<GraphObjectsIterable> containers) throws VitalServiceUnimplementedException, VitalServiceException {
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
	
	protected abstract ResultList _selectQuery(VitalOrganization organization, VitalApp app, VitalSelectQuery query) throws VitalServiceException, VitalServiceUnimplementedException;

	/**
	 * inner graph query, does not check the segments 
	 * @param gq
	 * @return
	 */
	protected abstract ResultList _graphQuery(VitalOrganization organization, VitalApp app, VitalGraphQuery gq) throws VitalServiceUnimplementedException, VitalServiceException;

	

	
	// ServiceWide
	// ServiceWide
	@Override
	public ResultList getExpanded(VitalOrganization organization, VitalApp app, URIProperty uri, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException {

		ResultList rl = this.get(organization, app, GraphContext.ServiceWide, Arrays.asList(uri));
		GraphObject o = rl.first();
		if(o == null) {
			rl = new ResultList();
			rl.setStatus(VitalStatus.withError("GraphObject not found: " + uri.get()));
			return rl;
		}
		
		List<GraphObject> objects = new ArrayList<GraphObject>();
		objects.add(o);
		
		List<VitalSegment> segments = listSegments(organization, app);
		
		VitalPathQuery pq = null;
		try {
			pq = PathQueryHelperImpl.getDefaultExpandQuery(app, segments, objects);
		} catch (Exception e) {
			rl = new ResultList();
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
			return rl;
		}
		
		ResultList res = this.query(organization, app, pq);
		if(cache) {
			addToCache(res);
		}
		return res;
		
	}
	
	// path query determines segments list
	// there can be a prepared default path query helper that does the same as expand
	// i.e. VitalPathQuery.getDefaultExpandQuery(List<VitalSegment> segments, VitalSelectQuery rootselector) --> VitalPathQuery
	// for the case to query against containers or local, no segments specified:
	// VitalPathQuery.getDefaultExpandQuery(VitalSelectQuery rootselector) --> VitalPathQuery
	
	@Override
	public ResultList getExpanded(VitalOrganization organization, VitalApp app, URIProperty uri, VitalPathQuery pq, boolean cache) throws VitalServiceUnimplementedException, VitalServiceException {
		if(pq.getRootArc() != null) throw new RuntimeException("getExpanded does not accept a path query with root arc set");
		pq.setRootURIs(Arrays.asList(uri));
		if(pq.getArcs() == null || pq.getArcs().size() < 1) throw new RuntimeException("Null or empty arcs list in a path query");
		ResultList rl = this.query(organization, app, pq);
		if(cache) {
			addToCache(rl);
		}
		return rl;
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteExpanded(NO_TRANSACTION, organization, app, uri);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteResults(transaction, organization, app, getExpanded(organization, app, uri, false));
	}

	@Override
	public VitalStatus deleteExpandedObject(VitalOrganization organization, VitalApp app, GraphObject object) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteExpandedObject(NO_TRANSACTION, organization, app, object);
	}
	@Override
	public VitalStatus deleteExpandedObject(VitalTransaction transaction, VitalOrganization organization, VitalApp app, GraphObject object) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteResults(transaction, organization, app, getExpanded(organization, app, URIProperty.withString(object.getURI()), false));
		
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, URIProperty uri, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteExpanded(NO_TRANSACTION, organization, app, uri, query);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteResults(transaction, organization, app, getExpanded(organization, app, uri, query, false));
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalOrganization organization, VitalApp app, List<URIProperty> uris, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteExpanded(NO_TRANSACTION, organization, app, uris, query);
	}
	
	@Override
	public VitalStatus deleteExpanded(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		if(query.getRootArc() != null) throw new RuntimeException("Path query must not have root query set");
		if(query.getArcs() == null || query.getArcs().size() < 1) throw new RuntimeException("Path query must have at least 1 arc");
		query.setRootURIs(uris);
		return deleteResults(transaction, organization, app, this.query(organization, app, query));
		
	}
	
	@Override
	public VitalStatus deleteExpandedObjects(VitalOrganization organization, VitalApp app, List<GraphObject> objects, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		return deleteExpandedObjects(NO_TRANSACTION, organization, app, objects, query);
	}
	
	@Override
	public VitalStatus deleteExpandedObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> objects, VitalPathQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
		List<URIProperty> uris = new ArrayList<URIProperty>();
		for(GraphObject g : objects) {
			uris.add(URIProperty.withString(g.getURI()));
		}
		return this.deleteExpanded(transaction, organization, app, uris, query);
	}
	
	
	protected VitalStatus deleteResults(VitalTransaction transaction, VitalOrganization organization, VitalApp app, ResultList rl) throws VitalServiceUnimplementedException, VitalServiceException {

		List<URIProperty> uris = new ArrayList<URIProperty>();
		
		for(Iterator<GraphObject> iter = rl.iterator(); iter.hasNext(); ) {
			uris.add(URIProperty.withString(iter.next().getURI()));
		}
		
		if(uris.size() > 0) {
			return this.delete(transaction, organization, app, uris);
		}
		
		return VitalStatus.withOKMessage("No objects deleted");
		
	}
	
	// Import and Export
	
	@Override
	public final VitalStatus bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream) throws VitalServiceUnimplementedException, VitalServiceException {
		return bulkImport(organization, app, segment, inputStream, "");
	}
	
	@Override
	public final VitalStatus bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream, String datasetURI) throws VitalServiceUnimplementedException, VitalServiceException {
		if( getTransactions().size() > 0 ) throw new VitalServiceException("Bulk import cancelled, active transaction detected");
		return _bulkImport(organization, app, segment, inputStream, datasetURI);
	}
	
	@Override
	public final VitalStatus bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream) throws VitalServiceUnimplementedException, VitalServiceException {
		return bulkExport(organization, app, segment, outputStream, null);
	}
	
	@Override
	public final VitalStatus bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream, String datasetURI) throws VitalServiceUnimplementedException, VitalServiceException {
		if( getTransactions().size() > 0 ) throw new VitalServiceException("Bulk export cancelled, active transaction detected");
//		throw new VitalServiceUnimplementedException("NOT IMPLEMENTED");
		return _bulkExport(organization, app, segment, outputStream, datasetURI);
	}
	
	
	protected abstract VitalStatus _bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream, String datasetURI) throws VitalServiceUnimplementedException, VitalServiceException;
	protected abstract VitalStatus _bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream, String datasetURI) throws VitalServiceUnimplementedException, VitalServiceException;
	
	
	

	@Override
	public VitalStatus sendEvent(VitalOrganization organization, VitalApp app, final VITAL_Event event, boolean waitForDelivery) throws VitalServiceUnimplementedException, VitalServiceException {


		final String msg = CompactStringSerializer.toCompactString(event);



		return VitalStatus.OK;
	}

	
	@Override
	public VitalStatus sendEvents(VitalOrganization organization, VitalApp app, List<VITAL_Event> events, boolean waitForDelivery) throws VitalServiceUnimplementedException, VitalServiceException {


		final StringBuilder msg = new StringBuilder();

		for(VITAL_Event event : events) {
			CompactStringSerializer.toCompactString(event);
			if(msg.length() > 0) msg.append("\n");
			msg.append(CompactStringSerializer.toCompactString(event));
		}



		return VitalStatus.OK;
	}

	@Override
	public VitalStatus uploadFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName, InputStream inputStream,
			boolean overwrite) throws VitalServiceUnimplementedException, VitalServiceException {
		throw new VitalServiceUnimplementedException("Not supported.");
	}

	@Override
	public VitalStatus downloadFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName, OutputStream outputStream,
			boolean closeOutputStream) throws VitalServiceUnimplementedException, VitalServiceException {
		throw new VitalServiceUnimplementedException("Not supported.");
	}

	@Override
	public VitalStatus fileExists(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName) throws VitalServiceUnimplementedException, VitalServiceException {
		throw new VitalServiceUnimplementedException("Not supported.");
	}

	@Override
	public VitalStatus deleteFile(VitalOrganization organization, VitalApp app, URIProperty uri, String fileName) throws VitalServiceUnimplementedException, VitalServiceException {
		throw new VitalServiceUnimplementedException("Not supported.");
	}

	@Override
	public ResultList listFiles(VitalOrganization organization, VitalApp app, String filepath) throws VitalServiceUnimplementedException, VitalServiceException {
		throw new VitalServiceUnimplementedException("Not supported.");
	}
	

	@Override
	public VitalStatus addDatabaseConnection(VitalOrganization organization, VitalApp arg0, DatabaseConnection arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		if(getApp(organization, arg0) == null) {
			return VitalStatus.withError("App not found: " + arg0.getRaw(Property_hasAppID.class));
		}
		return dbConnectionsImpl.addDatabaseConnection(organization, arg0, arg1);
	}
	
	@Override
	public ResultList listDatabaseConnections(VitalOrganization organization, VitalApp arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
//		if(getApp(organization, arg0) == null) {
//			ResultList rl = new ResultList();
//			rl.setStatus(VitalStatus.withError("App not found: " + arg0.getRaw(Property_hasAppID.class)));
//			return rl;
//		}
		return dbConnectionsImpl.listDatabaseConnections(organization, arg0);
	}

	@Override
	public VitalStatus removeDatabaseConnection(VitalOrganization organization, VitalApp arg0, String arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		if(getApp(organization, arg0) == null) {
			return VitalStatus.withError("App not found: " + arg0.getRaw(Property_hasAppID.class));
		}
		return dbConnectionsImpl.removeDatabaseConnection(organization, arg0, arg1);
	}

	protected VitalApp getApp(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException {
		for(VitalApp a : listApps(organization)) {
			if(a.getRaw(Property_hasAppID.class).equals(app.getRaw(Property_hasAppID.class))) return a;
		}
		return null;
	}



	public URIGenerationStrategy getUriGenerationStrategy() {
		return uriGenerationStrategy;
	}

	public void setUriGenerationStrategy(URIGenerationStrategy uriGenerationStrategy) {
		this.uriGenerationStrategy = uriGenerationStrategy;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}	
	
	
	
	@Override
	public VitalSegment getSegment(VitalOrganization organization, VitalApp arg0, String arg1)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.getSegment(organization, arg0, arg1);
	}

	@Override
	public VitalApp getApp(VitalOrganization organization, String arg0)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.getApp(organization, arg0);
	}

	@Override
	public final List<VitalSegment> listSegments(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.listSegmentsOnly(organization, app);
	}
	
	@Override
	public ResultList listSegmentsWithConfig(VitalOrganization organization,
			VitalApp app) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return systemSegment.listSegments(organization, app);
	}

	@Override
	public final VitalSegment addSegment(VitalOrganization organization, VitalApp arg0, VitalSegment arg1,
			boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return systemSegment.addSegment(organization, arg0, arg1);
	}

	@Override
	public VitalSegment addSegment(VitalOrganization organization,
			VitalApp app, VitalSegment config, VitalProvisioning provisioning,
			boolean createIfNotExists)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.addSegment(organization, app, config, provisioning);
	}

	@Override
	public final VitalStatus addApp(VitalOrganization organization, VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		systemSegment.addApp(organization, app);
		return VitalStatus.withOK();
	}

	@Override
	public final List<VitalApp> listApps(VitalOrganization organization) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return systemSegment.listApps(organization);
	}

	@Override
	public final VitalStatus removeApp(VitalOrganization organization, VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		systemSegment.removeApp(organization, app);
		return VitalStatus.withOK();
	}

	@Override
	public final VitalStatus removeSegment(VitalOrganization organization, VitalApp app, VitalSegment segment,
			boolean deleteData) throws VitalServiceUnimplementedException,
			VitalServiceException {
		systemSegment.removeSegment(organization, app, segment);
		return VitalStatus.withOK();
	}

	@Override
	public final VitalStatus addOrganization(VitalOrganization organization)
			throws VitalServiceUnimplementedException, VitalServiceException {
		List<VitalServiceRootKey> rks = systemSegment.listVitalServiceRootKeys();
		if(rks.size() < 1) throw new RuntimeException("No root keys!");
		systemSegment.addOrganization(rks.get(0), organization);
		return VitalStatus.withOK();
	}

	@Override
	public final VitalStatus removeOrganization(VitalOrganization organization)
			throws VitalServiceUnimplementedException, VitalServiceException {
		systemSegment.removeOrganization(organization);
		return VitalStatus.withOK();
	}

	@Override
	public final List<VitalOrganization> listOrganizations()
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.listOrganizations();
	}

	@Override
	public final VitalOrganization getOrganization(String organizationID)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.getOrganization(organizationID);
	}

	@Override
	public VitalStatus addVitalServiceKey(VitalOrganization organization,
			VitalApp app, VitalServiceKey serviceKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.addVitalServiceKey(organization, app, serviceKey);
	}

	@Override
	public VitalStatus removeVitalServiceKey(VitalOrganization organization,
			VitalApp app, VitalServiceKey serviceKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.removeVitalServiceKey(organization, app, serviceKey);
	}

	@Override
	public List<VitalServiceKey> listVitalServiceKeys(
			VitalOrganization organization, VitalApp app)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.listVitalServiceKeys(organization, app);
	}

	@Override
	public VitalStatus addVitalServiceAdminKey(VitalOrganization organization,
			VitalServiceAdminKey serviceAdminKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.addVitalServiceAdminKey(organization, serviceAdminKey);
	}

	@Override
	public VitalStatus removeVitalServiceAdminKey(
			VitalOrganization organization, VitalServiceAdminKey serviceAdminKey)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.removeVitalServiceAdminKey(organization, serviceAdminKey);
	}

	@Override
	public List<VitalServiceAdminKey> listVitalServiceAdminKeys(
			VitalOrganization organization)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.listVitalServiceAdminKeys(organization);
	}

	public SystemSegment getSystemSegment() {
		return systemSegment;
	}

	@Override
	public VitalServiceConfig getConfig() {
		return config;
	}

	@Override
	public void setConfig(VitalServiceConfig config) {
		this.config = config;
	}

	@Override
	public VitalAuthKey getAuthKey() {
		return serviceSuperAdminKey;
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

	public TransactionsImplementation getTransactionsImplementation() {
		return transactionsImplementation;
	}
	
}
