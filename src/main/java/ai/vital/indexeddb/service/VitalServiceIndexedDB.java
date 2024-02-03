package ai.vital.indexeddb.service;

import static ai.vital.vitalservice.VitalServiceConstants.NO_TRANSACTION;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig;
import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig.QueryTarget;
import ai.vital.indexeddb.service.impl.AllegrographImpl;
import ai.vital.indexeddb.service.impl.DBInterface;
import ai.vital.indexeddb.service.impl.DBInterface.ScanListener;
import ai.vital.indexeddb.service.impl.IndexDBSystemSegmentExecutor;
import ai.vital.indexeddb.service.impl.IndexInterface;
import ai.vital.indexeddb.service.impl.LuceneDiskImpl;
import ai.vital.indexeddb.service.impl.SqlImpl;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.AbstractVitalServiceImplementation;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.AggregationResult;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.RDFStatement;
import ai.vital.vitalsigns.model.SparqlAskResponse;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionID;
import ai.vital.vitalsigns.model.property.URIProperty;

public class VitalServiceIndexedDB extends AbstractVitalServiceImplementation {

	private final static Logger log = LoggerFactory.getLogger(VitalServiceIndexedDB.class);
	
	IndexInterface index;
	
	DBInterface database;
	
	private VitalServiceIndexedDB(IndexInterface index, DBInterface database,
			VitalOrganization _organization, VitalApp _app) {
		super(new SystemSegment(new IndexDBSystemSegmentExecutor(index.getSystemSegmentExecutor(), database.getSystemSegmentExecutor())), _organization, _app);
		this.index = index;
		this.database = database;
		
		/*
		organization = database.getOrganization(_organization.ID)
		if(organization == null) throw new RuntimeException("Organization not found: ${_organization.ID}")
		app = database.getApp(_organization.ID, _app.ID)
		if(app == null) throw new RuntimeException("App not found: ${_app.ID}, organizationID: ${_organization.ID}")
		*/
		
	}
			
	public static VitalServiceIndexedDB create(VitalServiceIndexedDBConfig config, VitalOrganization organization, VitalApp app) throws IOException {
		

		
		IndexInterface indexInterface = null;
		DBInterface dbInterface = null;
		
		EndpointType indexType = config.getIndexConfig().getEndpointtype();
		
		if(EndpointType.LUCENEDISK == indexType) {
			indexInterface = new LuceneDiskImpl();
		} else {
			throw new RuntimeException("Endpoint type not supported as index: " + indexType);
		}
		
		EndpointType dbType = config.getDbConfig().getEndpointtype();
		
		if(EndpointType.ALLEGROGRAPH == dbType) {
			dbInterface = new AllegrographImpl();
		} else if(EndpointType.SQL == dbType) {
			dbInterface = new SqlImpl();
		} else {
			throw new RuntimeException("Endpoint type not supported as database: " + dbType);
		}
		
		indexInterface.initialize(config.getIndexConfig());
		dbInterface.initialize(config.getDbConfig());
		
		return new VitalServiceIndexedDB(indexInterface, dbInterface, organization, app);
		
	}
			
	@Override
	public VitalStatus ping() throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			index.ping();
			database.ping();
			return VitalStatus.withOK();
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}


	@Override
	protected VitalStatus _close() throws VitalServiceException,
	VitalServiceUnimplementedException {
		try { index.close(); } catch(Exception e){}
		try { database.close(); } catch(Exception e){}
		return VitalStatus.withOK();
	}

	@Override
	protected ResultList _callFunction(
			String function, Map<String, Object> arguments)
	throws VitalServiceException, VitalServiceUnimplementedException {
		throw new VitalServiceUnimplementedException("Functions calls not supporeted in indexed-db.");
	}

	@Override
	protected GraphObject _getServiceWide(
			URIProperty uri) throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			ResultList segmentsRS = systemSegment.listSegments(organization, app);
			return database.get(organization, app, uri, segmentsRS);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected List<GraphObject> _getServiceWideList(
			List<URIProperty> uris) throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			List<String> urisStrings = new ArrayList<String>();
			for(URIProperty uri : uris) {
				urisStrings.add(uri.get());
			}
			return database.getBatch(organization, app, urisStrings, systemSegment.listSegments(organization, app));
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _saveList(VitalTransaction transaction,
			VitalSegment targetSegment, List<GraphObject> graphObjectsList)
	throws VitalServiceException, VitalServiceUnimplementedException {
		ResultList listSegments = systemSegment.listSegments(organization, app);
		ResultList rs = null;
		try {
			rs = database.save(transaction, organization, app, targetSegment, graphObjectsList, listSegments);
		} catch(Exception e) {
			throw new VitalServiceException("Persisting into database failed: " + e.getLocalizedMessage(), e);
		}

		if(transaction == null || transaction == NO_TRANSACTION) {
			try {
				index.save(NO_TRANSACTION, organization, app, targetSegment, graphObjectsList, listSegments);
			} catch(Exception e) {
				throw new VitalServiceException("Saved in database but indexing failed: " + e.getLocalizedMessage(), e);
			}
		}

		return rs;
	}

	@Override
	protected VitalStatus _deleteList(VitalTransaction transaction,
			List<URIProperty> uris) throws VitalServiceException,
	VitalServiceUnimplementedException {
		//first delete from database

		ResultList listSegments = systemSegment.listSegments(organization, app);
		
		try {
			database.delete(transaction, organization, app, uris, listSegments);
		} catch(Exception e) {
			throw new VitalServiceException("Deleting from database failed: " + e.getLocalizedMessage(), e);
		}

		if(transaction == null || transaction == NO_TRANSACTION) {
			try {
				index.delete(NO_TRANSACTION, organization, app, uris, listSegments);
			} catch(Exception e) {
				log.error(e.getLocalizedMessage(), e);
//				throw new VitalServiceException("Deleted from database but deleting from index failed: ${e.localizedMessage}", e)
			}
		}

		return VitalStatus.withOK();
	}

	@Override
	protected ResultList _selectQuery(
			VitalSelectQuery query) throws VitalServiceException,
	VitalServiceUnimplementedException {

		ResultList listSegments = systemSegment.listSegments(organization, app);
		
		ResultList rs = null;
		VitalServiceIndexedDBConfig indexDBCfg = (VitalServiceIndexedDBConfig) this.config;
		
		try {
			if(indexDBCfg.getSelectQueries() == QueryTarget.index && index.supportsSelectQuery(organization, app, query)) {
				rs = index.selectQuery(organization, app, query, listSegments);
//				return resolveResultsList(organization, app, database, rs, query, listSegments);
				return rs;
			} else if(database.supportsSelectQuery(organization, app, query)) {
				rs = database.selectQuery(organization, app, query, listSegments);
				return rs;
//				return resolveResultsList(organization, app, database, rs, query, listSegments);
			} else {
				throw new Exception("Given select query not supported by the service, queryTarget: " + indexDBCfg.getSelectQueries());
			} 
		} catch(Exception e) {
			throw new VitalServiceException("Select query failed in index: " + e.getLocalizedMessage(), e);
		}

	}
	
	@Override
	protected ResultList _sparqlQuery(VitalSparqlQuery query)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList listSegments = systemSegment.listSegments(organization, app);
		try {
			return database.sparqlQuery(organization, app, query, listSegments);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _graphQuery(
			VitalGraphQuery gq) throws VitalServiceException {
		
		ResultList listSegments = systemSegment.listSegments(organization, app);
		
		ResultList rs = null;
		VitalServiceIndexedDBConfig indexDBCfg = (VitalServiceIndexedDBConfig) this.config;
		
		try {
			if(indexDBCfg.getGraphQueries() == QueryTarget.index /*index.supportsGraphQuery(organization, app, gq)*/) {
				rs = index.graphQuery(organization, app, gq, listSegments);
				return rs;
//				return resolveResultsList(organization, app, database, rs, gq, listSegments);
			} else if(database.supportsGraphQuery(organization, app, gq)) {
				rs = database.graphQuery(organization, app, gq, listSegments);
				return rs;
			} else {
				throw new Exception("Given graph query not supported by the service");
			}
		} catch(Exception e) {
			e.printStackTrace();
			throw new VitalServiceException("Graph query failed in index: " + e.getLocalizedMessage(), e);
		}
	}

	public static ResultList resolveResultsList(VitalOrganization organization, VitalApp app, DBInterface database, ResultList rs, VitalQuery query, ResultList segmentsRL) throws Exception {

		
		if(query instanceof VitalSelectAggregationQuery) {
			return rs;
		} else if(query instanceof VitalSelectQuery && ((VitalSelectQuery)query).getDistinct() ) {
			return rs;
		}
		
		if(rs.getResults().size() < 1) return rs;

		Set<String> urisToResolve = new HashSet<String>();
		
		Map<String, GraphObject> m = new HashMap<String, GraphObject>();

		for(GraphObject g : rs) {
		
			if(g instanceof AggregationResult || g instanceof RDFStatement || g instanceof SparqlAskResponse) {

				m.put(g.getURI(), g);
								
			} else {
				urisToResolve.add(g.getURI());
			}
				
		}

		
		if(urisToResolve.size() > 0) {
			
			List<GraphObject> gos = database.getBatch(organization, app, urisToResolve, segmentsRL);
			
					
			for(GraphObject g : gos) {
				m.put(g.getURI(), g);
			}
			
		}
		
		for(Iterator<ResultElement> iterator = rs.getResults().iterator(); iterator.hasNext(); ) {

			ResultElement el = iterator.next();

			GraphObject g = m.get(el.getGraphObject().getURI());

			if(g == null) {
				iterator.remove();
			} else {
				el.setGraphObject(g);
			}

		}

		return rs;

	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.INDEXDB;
	}

	
	@Override
	public VitalStatus deleteObject(GraphObject object)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.deleteObject(NO_TRANSACTION, object);
	}
	
	@Override
	public VitalStatus deleteObject(VitalTransaction transaction,
			GraphObject object) throws VitalServiceUnimplementedException,
			VitalServiceException {
		
		if(database instanceof AllegrographImpl) {
			
			if(object instanceof RDFStatement) {
				try {
					return ((AllegrographImpl)database).deleteRDFStatements(transaction, Arrays.asList(object));
				} catch (Exception e) {
					throw new VitalServiceException(e);
				}
			}
			
		}
		
		
		return super.deleteObject(transaction, object);
	}

	
	@Override
	public VitalStatus deleteObjects(List<GraphObject> objects)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.deleteObjects(NO_TRANSACTION, objects);
	}
	
	
	@Override
	public VitalStatus deleteObjects(VitalTransaction transaction, List<GraphObject> arg0)
			throws VitalServiceException, VitalServiceUnimplementedException {
		//check if it's special rdf case
		
		if(database instanceof AllegrographImpl) {
			
			List<RDFStatement> rdfStatements = new ArrayList<RDFStatement>();
			
			for(GraphObject g : arg0) {
				if(g instanceof RDFStatement) {
					rdfStatements.add((RDFStatement) g);
				} else {
					if(rdfStatements.size() > 0) throw new VitalServiceException("Cannot mix rdf statements with graph objects");
				}
			}
			
			if(rdfStatements.size() > 0) {
				try {
					return ((AllegrographImpl)database).deleteRDFStatements(transaction, arg0);
				} catch (Exception e) {
					throw new VitalServiceException(e);
				}
			}
		}
		
		
		//default
		return super.deleteObjects(transaction, arg0);
	}

			
	@Override
	public ResultList insert(VitalSegment targetSegment, GraphObject graphObject)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.insert(NO_TRANSACTION, targetSegment, graphObject);
	}
	
	@Override
	public ResultList insert(VitalTransaction tx, VitalSegment targetSegment, GraphObject graphObject)
			throws VitalServiceException, VitalServiceUnimplementedException {

		if(database instanceof AllegrographImpl) {
			
			if(graphObject instanceof RDFStatement) {
				try {
					return ((AllegrographImpl)database).insertRDFStatements(tx, Arrays.asList(graphObject));
				} catch (Exception e) {
					throw new VitalServiceException(e);
				}
			}
			
		}
		
				
		//default
		return super.insert(tx, targetSegment, graphObject);
	}

	@Override
	public ResultList insert(VitalSegment targetSegment,
			List<GraphObject> graphObjectsList)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.insert(NO_TRANSACTION, targetSegment, graphObjectsList);
	}
	
	@Override
	public ResultList insert(VitalTransaction tx, VitalSegment targetSegment,
			List<GraphObject> graphObjectsList) throws VitalServiceException,
			VitalServiceUnimplementedException {
		
		if(database instanceof AllegrographImpl) {
			
			//check if it's special rdf case
			List<GraphObject> rdfStatements = new ArrayList<GraphObject>();
			
			for(GraphObject g : graphObjectsList) {
				if(g instanceof RDFStatement) {
					rdfStatements.add(g);
				} else {
					if(rdfStatements.size() > 0) throw new VitalServiceException("Cannot mix rdf statements with other graph objects");
				}
			}
			
			if(rdfStatements.size() > 0) {
				try {
					return ((AllegrographImpl)database).insertRDFStatements(tx, rdfStatements);
				} catch (Exception e) {
					throw new VitalServiceException(e);
				}
			}
			
		}
		
			
		//default
		return super.insert(tx, targetSegment, graphObjectsList);
		
	}

	@Override
	protected void _commitTransaction(TransactionWrapper transaction) throws VitalServiceException {
		
		ResultList listSegments = systemSegment.listSegments(organization, app);

		try {
			database.commitTransaction(transaction.transaction, listSegments);
		} catch (Exception e1) {
		}

		try {
			index.synchronizeTransaction(transaction, listSegments);
		} catch(Exception e) {
			throw new RuntimeException("Transaction committed OK in database, index synchronization failed. You may need to rebuild the index, error: " + e.getLocalizedMessage());
		}

	}

	@Override
	protected void _createTransaction(VitalTransaction transaction) throws VitalServiceException {

		try {
			
			String transactionID = database.createTransaction();
			//copy the transaction details
			transaction.set(Property_hasTransactionID.class, transactionID);
			
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
		
		
	}

	@Override
	protected void _rollbackTransaction(TransactionWrapper transaction) throws VitalServiceException {

		try {
			database.rollbackTransaction(transaction.transaction);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
		
	}

	@Override
	protected VITAL_GraphContainerObject _getExistingObjects(List<String> uris) throws VitalServiceException {
		ResultList listSegments = systemSegment.listSegments(organization, app);
		try {
			return database.getExistingObjects(organization, app, uris, listSegments);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	public VitalStatus _bulkExport(VitalSegment arg0, OutputStream arg1, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList listSegments = systemSegment.listSegments(organization, app);
		try {
			return database.bulkExport(organization, app, arg0, arg1, listSegments, datasetURI);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	public VitalStatus _bulkImport(VitalSegment arg0, InputStream arg1, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList listSegments = systemSegment.listSegments(organization, app);
		try {
			return database.bulkImport(organization, app, arg0, arg1, listSegments, datasetURI);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}
	

		
	//own method to rebuild index
	public VitalStatus reindexSegment(final VitalApp app, final VitalSegment segment) throws Exception {
		
		final ResultList listSegments = systemSegment.listSegments(organization, app);
		
		//purge segment
		index.deleteAll(organization, app, segment, listSegments);
		
		ScanListener listenr = new ScanListener(){
					
			public void onBatchReady(List<GraphObject> arg0) {
				try {
					index.save(NO_TRANSACTION, organization, app, segment, arg0, listSegments);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			};
					
			public void onScanComplete() {};
					
		};
				
		database.scanSegment(organization, app, segment, 100, listenr, listSegments);
				
		return VitalStatus.withOK();
						
	}

	@Override
	protected VitalStatus _deleteAll(VitalSegment segment)
			throws VitalServiceException, VitalServiceUnimplementedException {

		ResultList listSegments = systemSegment.listSegments(organization, app);
		
		try {
			database.deleteAll(organization, app, segment, listSegments);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
		
		try {
			index.deleteAll(organization, app, segment, listSegments);
		} catch(Exception e) {
			throw new VitalServiceException("Data was removed from database but deleting from index failed, segment needs to be reindexed, reason: " + e.getLocalizedMessage());
		}
		
		return VitalStatus.withOK();
		
	}

	@Override
	protected void _transactionsCheck()
			throws VitalServiceUnimplementedException {
		database.transactionsCheck();
	}

	
	
	
}
