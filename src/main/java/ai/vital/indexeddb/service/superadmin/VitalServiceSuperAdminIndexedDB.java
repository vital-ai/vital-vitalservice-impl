package ai.vital.indexeddb.service.superadmin;

import static ai.vital.vitalservice.VitalServiceConstants.NO_TRANSACTION;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import ai.vital.indexeddb.service.VitalServiceIndexedDB;
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
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.superadmin.impl.AbstractVitalServiceSuperAdminImplementation;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.RDFStatement;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasOrganizationID;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionID;
import ai.vital.vitalsigns.model.property.URIProperty;

public class VitalServiceSuperAdminIndexedDB extends AbstractVitalServiceSuperAdminImplementation {

	IndexInterface index;
	
	private EndpointType indexType;
	
	private EndpointType databaseType;
	
	DBInterface database;
	
	private VitalServiceSuperAdminIndexedDB(IndexInterface index, DBInterface database) {
		super(new SystemSegment(new IndexDBSystemSegmentExecutor(index.getSystemSegmentExecutor(), database.getSystemSegmentExecutor())));
		this.index = index;
		this.database = database;
		
//		organization = database.getOrganization(_organization.ID)
//		if(organization == null) throw new RuntimeException("Organization not found: ${_organization.ID}")
	}
	
	public static VitalServiceSuperAdminIndexedDB create(VitalServiceIndexedDBConfig config) {
		

		IndexInterface indexInterface = null;
		DBInterface dbInterface = null;
		
		EndpointType itype = config.getIndexConfig().getEndpointtype();
		EndpointType dtype = config.getDbConfig().getEndpointtype();
		
		if(EndpointType.LUCENEDISK == itype) {
			indexInterface = new LuceneDiskImpl();
		} else {
			throw new RuntimeException("Endpoint type not supported as index: " + itype);
		}
		
		if(EndpointType.ALLEGROGRAPH == dtype) {
			dbInterface = new AllegrographImpl();
		} else if(EndpointType.SQL == dtype) {
			dbInterface = new SqlImpl();
		} else {
			throw new RuntimeException("Endpoint type not supported as database: " + dtype);
		}
		
		try {
			indexInterface.initialize(config.getIndexConfig());
			dbInterface.initialize(config.getDbConfig());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		VitalServiceSuperAdminIndexedDB instance = new VitalServiceSuperAdminIndexedDB(indexInterface, dbInterface);
		instance.indexType = itype;
		instance.databaseType = dtype;
		return instance;
				
	}


	@Override
	protected VitalStatus _close() throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			index.close();
		} catch (Exception e) {}
		try {
			database.close();
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
		return VitalStatus.withOK();
	}

	@Override
	protected ResultList _callFunction(VitalOrganization organization, VitalApp app,
			String function, Map<String, Object> arguments)
	throws VitalServiceException, VitalServiceUnimplementedException {
		throw new VitalServiceUnimplementedException("Functions calls not supporeted in indexed-db.");
	}

	@Override
	protected GraphObject _getServiceWide(VitalOrganization organization, VitalApp app,
			URIProperty uri) throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			return database.get(organization, app, uri, systemSegment.listSegments(organization, app));
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected List<GraphObject> _getServiceWideList(VitalOrganization organization, VitalApp app,
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
	protected ResultList _saveList(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
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
	protected VitalStatus _deleteList(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			List<URIProperty> uris) throws VitalServiceException,
	VitalServiceUnimplementedException {
		
		ResultList listSegments = systemSegment.listSegments(organization, app);
		
		//first delete from database
		try {
			database.delete(transaction, organization, app, uris, listSegments);
		} catch(Exception e) {
			throw new VitalServiceException("Deleting from database failed: " + e.getLocalizedMessage(), e);
		}

		if(transaction == null || transaction == NO_TRANSACTION) {
			try {
				index.delete(NO_TRANSACTION, organization, app, uris, listSegments);
			} catch(Exception e) {
				throw new VitalServiceException("Deleted from database but deleting from index failed: " + e.getLocalizedMessage(), e);
			}
		}

		return VitalStatus.withOK();
	}

	@Override
	protected ResultList _selectQuery(VitalOrganization organization, VitalApp app,
			VitalSelectQuery query) throws VitalServiceException,
	VitalServiceUnimplementedException {

		ResultList listSegments = systemSegment.listSegments(organization, app);
		
		ResultList rs = null;
		VitalServiceIndexedDBConfig indexDBCfg = (VitalServiceIndexedDBConfig) this.config;
		
		try {
			if(indexDBCfg.getSelectQueries() == QueryTarget.index && index.supportsSelectQuery(organization, app, query)) {
				rs = index.selectQuery(organization, app, query, listSegments);
//				return VitalServiceIndexedDB.resolveResultsList(organization, app, database, rs, query, listSegments);
				return rs;
			} else if(database.supportsSelectQuery(organization, app, query)) {
				rs = database.selectQuery(organization, app, query, listSegments);
//				return VitalServiceIndexedDB.resolveResultsList(organization, app, database, rs, query, listSegments);
				return rs;
			} else {
				throw new Exception("Given select query not supported by the service");
			} 
		} catch(Exception e) {
			throw new VitalServiceException("Select query failed in index: " + e.getLocalizedMessage(), e);
		}

	}

	@Override
	protected ResultList _graphQuery(VitalOrganization organization, VitalApp app,
			VitalGraphQuery gq) throws VitalServiceException {
		
		ResultList listSegments = systemSegment.listSegments(organization, app);
		
		ResultList rs = null;
		VitalServiceIndexedDBConfig indexDBCfg = (VitalServiceIndexedDBConfig) this.config;
		
		try {
			if(indexDBCfg.getGraphQueries() == QueryTarget.index /*index.supportsGraphQuery(organization, app, gq)*/) {
				rs = index.graphQuery(organization, app, gq, listSegments);
//				return VitalServiceIndexedDB.resolveResultsList(organization, app, database, rs, gq, listSegments);
				return rs;
			} else if(database.supportsGraphQuery(organization, app, gq)) {
				rs = database.graphQuery(organization, app, gq, listSegments);
				return rs;
			} else {
				throw new Exception("Given graph query not supported by the service");
			}
		} catch(Exception e) {
			throw new VitalServiceException("Graph query failed in index: " + e.getLocalizedMessage(), e);
		}

	}

	
	//own method to rebuild index
	public VitalStatus reindexSegment(final VitalOrganization organization, final VitalApp app, final VitalSegment segment) throws Exception {
		
		final ResultList listSegments = systemSegment.listSegments(organization, app);
		
		//purge segment
		index.deleteAll(organization, app, segment, listSegments);

		ScanListener listenr = new ScanListener(){
			
			public void onBatchReady(List<GraphObject> arg0) {
				try {
					index.insertNoCheck(organization, app, segment, arg0, listSegments);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			};
			
			public void onScanComplete() {};
			
		};
		
		database.scanSegment(organization, app, segment, 100, listenr, listSegments);
		
		return VitalStatus.withOK();
				
	}

	public EndpointType getIndexType() {
		return this.indexType;
	}
	
	public EndpointType getDatabaseType() {
		return this.databaseType;
	}

	public VitalStatus verifyIndices() throws Exception {
		
		List<String> errors = new ArrayList<String>();
		
		List<VitalOrganization> databaseOrganizations = systemSegment.listOrganizations();
		
		IndexDBSystemSegmentExecutor executor = (IndexDBSystemSegmentExecutor) systemSegment.getExecutor();
		
		SystemSegmentOperationsExecutor indexExecutor = executor.getIndexExecutor();

		int successes = 0;
		
		for(VitalOrganization c : databaseOrganizations) {
			
			List<VitalApp> databaseApps = systemSegment.listApps(c);
			
			for(VitalApp a : databaseApps) {
				
				ResultList segmentsRL = systemSegment.listSegments(c, a);
				
				List<VitalSegment> segs = new ArrayList<VitalSegment>();
				for(Iterator<VitalSegment> iterator = segmentsRL.iterator(VitalSegment.class, true); iterator.hasNext(); ) {
					VitalSegment s = iterator.next();
					segs.add(s);
				}
				
				
				for(VitalSegment s : segs) {
					
					String orgID = (String) c.getRaw(Property_hasOrganizationID.class);
					String appID = (String) a.getRaw(Property_hasAppID.class);
					String segmentID = (String) s.getRaw(Property_hasSegmentID.class);
					
					if( indexExecutor.getSegmentResource(s) == null ) {
						
						errors.add("Segment exists in database but not in index - organizationID: " + orgID + ", appID: " + appID + ", segmentID: " + segmentID + " segmentURI: " + s.getURI());
						
						continue;
						
					}
					
					
					int databaseSize = executor.getDbExecutor().getSegmentSize(s, segmentsRL);
					int indexSize = executor.getIndexExecutor().getSegmentSize(s, segmentsRL);
					
					if(indexSize != databaseSize) {
						
						errors.add("Different segment index size (" + indexSize + ") and database size (" + databaseSize + "): orgID: " + orgID + ", appID: " + appID + ", segmentID: " + segmentID + " segment URI: " + s.getURI());
						
						continue;
						
					}
					
					successes ++;
					
				}
			}
			
		}
		
		
		if(errors.size() < 1) {
			VitalStatus status = VitalStatus.withOK();
			status.setSuccesses(successes);
			status.setErrors(0);
			return status;
		}
		
		StringBuilder s = new StringBuilder();
		for(String e : errors) {
			if(s.length() > 0) s.append("\n");
			s.append(e);
		}
		
		VitalStatus status = VitalStatus.withError(s.toString());
		status.setSuccesses(successes);
		status.setErrors(errors.size());
		return status;
		
	}

	public VitalStatus verifyIndex(VitalOrganization c, VitalApp a, VitalSegment s) throws Exception {
		
		String orgID = (String) c.getRaw(Property_hasOrganizationID.class);
		String appID = (String) a.getRaw(Property_hasAppID.class);
		String segmentID = (String) s.getRaw(Property_hasSegmentID.class);
	
		IndexDBSystemSegmentExecutor executor = (IndexDBSystemSegmentExecutor) systemSegment.getExecutor();
		
		ResultList segmentsRL = systemSegment.listSegments(c, a);
		
		if( executor.getDbExecutor().getSegmentResource(s) == null ) {
			return VitalStatus.withError("Segment not found - org " + orgID + " app: " + appID + " segmentID: " + segmentID + " segmentURI: " + s.getURI());
		}
		
		SystemSegmentOperationsExecutor indexExecutor = executor.getIndexExecutor();
		
		if( indexExecutor.getSegmentResource(s) == null ) {
			
			return VitalStatus.withError("Segment exists in database but not in index - organizationID: " + orgID + ", appID: " + appID + ", segmentID: " + segmentID + " segmentURI: " + s.getURI());
			
		}
		
		
		int databaseSize = executor.getDbExecutor().getSegmentSize(s, segmentsRL);
		int indexSize = executor.getIndexExecutor().getSegmentSize(s, segmentsRL);
		
		if(indexSize != databaseSize) {
			
			return VitalStatus.withError("Different segment index size (" + indexSize + ") and database size (" + databaseSize + "): orgID: " + orgID + ", appID: " + appID + ", segmentID: " + segmentID + " segment URI: " + s.getURI());
			
		}
			
		return VitalStatus.withOKMessage("Segment OK");
		
	}
	
	public VitalStatus rebuildIndices() throws Exception {
		
		
		List<VitalOrganization> databaseOrganizations = systemSegment.listOrganizations();
		
		IndexDBSystemSegmentExecutor executor = (IndexDBSystemSegmentExecutor) systemSegment.getExecutor();
		
		SystemSegmentOperationsExecutor indexExecutor = executor.getIndexExecutor();
		
		StringBuilder m = new StringBuilder();
		
		for(final VitalOrganization c : databaseOrganizations) {
			
			List<VitalApp> databaseApps = systemSegment.listApps(c);
			
			for(final VitalApp a : databaseApps) {
				
				final ResultList segmentsRL = systemSegment.listSegments(c, a);
				
				List<VitalSegment> segs = new ArrayList<VitalSegment>();
				for(Iterator<VitalSegment> iterator = segmentsRL.iterator(VitalSegment.class, true); iterator.hasNext(); ) {
					VitalSegment s = iterator.next();
					segs.add(s);
				}
				
				
				for(final VitalSegment s : segs) {
					
					String orgID = (String) c.getRaw(Property_hasOrganizationID.class);
					String appID = (String) a.getRaw(Property_hasAppID.class);
					String segmentID = (String) s.getRaw(Property_hasSegmentID.class);
					
					if(indexExecutor.getSegmentResource(s) != null) {
						
						indexExecutor.deleteSegmentResource(s, segmentsRL);
						m.append("\nReadded index segment (org:" + orgID + " app:" + appID + " segment:" + segmentID + ") URI:" + s.getURI());
						
					} else {
						
						m.append("\nAdded new index segment (org:" + orgID + " app:" + appID + " segment:" + segmentID + ") URI:" + s.getURI());
						
					}
					
					indexExecutor.createSegmentResource(s, segmentsRL);
					
					ScanListener listenr = new ScanListener(){
						
						public void onBatchReady(List<GraphObject> arg0) {
							try {
								index.save(NO_TRANSACTION, c, a, s, arg0, segmentsRL);
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
						};
						
						public void onScanComplete() {};
						
					};
					
					database.scanSegment(c, a, s, 1000, listenr, segmentsRL);
					
				}
			}
			
		}
		
		return VitalStatus.withOKMessage(m.toString());
				
		
	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.INDEXDB;
	}

	@Override
	public VitalStatus deleteObject(VitalOrganization organization, VitalApp app, GraphObject object)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.deleteObject(NO_TRANSACTION, organization, app, object);
	}
	
	@Override
	public VitalStatus deleteObject(VitalTransaction transaction, VitalOrganization organization, VitalApp app, 
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
			
		
		return super.deleteObject(transaction, organization, app, object);
	}

	
	@Override
	public VitalStatus deleteObjects(VitalOrganization organization, VitalApp app, List<GraphObject> objects)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.deleteObjects(NO_TRANSACTION, organization, app, objects);
	}
	
	
	@Override
	public VitalStatus deleteObjects(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<GraphObject> arg0)
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
		return super.deleteObjects(transaction, organization, app, arg0);
	}

			
	@Override
	public ResultList insert(VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.insert(NO_TRANSACTION, organization, app, targetSegment, graphObject);
	}
	
	@Override
	public ResultList insert(VitalTransaction tx, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject)
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
		return super.insert(tx, organization, app, targetSegment, graphObject);
	}

	@Override
	public ResultList insert(VitalOrganization organization, VitalApp app, VitalSegment targetSegment,
			List<GraphObject> graphObjectsList)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.insert(NO_TRANSACTION, organization, app, targetSegment, graphObjectsList);
	}
	
	@Override
	public ResultList insert(VitalTransaction tx, VitalOrganization organization, VitalApp app, VitalSegment targetSegment,
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
		return super.insert(tx, organization, app, targetSegment, graphObjectsList);
		
	}
	
	@Override
	protected void _commitTransaction(TransactionWrapper transaction) throws VitalServiceException {
		
		ResultList listSegments = systemSegment.listAllSegments();
		
		try {
			database.commitTransaction(transaction.transaction, listSegments);
		} catch (Exception e1) {
			throw new VitalServiceException(e1);
		}
		
		try {
			index.synchronizeTransaction(transaction, listSegments);
		} catch(Exception e) {
			throw new RuntimeException("Transaction committed OK in database, index synchronization failed. You may need to rebuild the index, error: " + e.getLocalizedMessage(), e);
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
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
				
	}

	@Override
	protected VITAL_GraphContainerObject _getExistingObjects(VitalOrganization organization, VitalApp app,
			List<String> uris) throws VitalServiceException {
		ResultList listSegments = systemSegment.listSegments(organization, app);
		try {
			return database.getExistingObjects(organization, app , uris, listSegments);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	public VitalStatus _bulkExport(VitalOrganization organization, VitalApp app, VitalSegment segment, OutputStream outputStream, String datasetURI)
			throws VitalServiceException {
		ResultList listSegments = systemSegment.listSegments(organization, app);
		try {
			return database.bulkExport(organization, app, segment, outputStream, listSegments, datasetURI);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	public VitalStatus _bulkImport(VitalOrganization organization, VitalApp app, VitalSegment segment, InputStream inputStream, String datasetURI)
		throws VitalServiceUnimplementedException, VitalServiceException {
		ResultList listSegments = systemSegment.listSegments(organization, app);
		try {
			return database.bulkImport(organization, app, segment, inputStream, listSegments, datasetURI);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
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
	protected VitalStatus _deleteAll(VitalOrganization organization, VitalApp app, VitalSegment segment)
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
			throws VitalServiceUnimplementedException {}
			
}
