package ai.vital.vitalservice.impl;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.indexeddb.service.VitalServiceIndexedDB;
import ai.vital.indexeddb.service.admin.VitalServiceAdminIndexedDB;
import ai.vital.vitalservice.BaseDowngradeUpgradeOptions;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.ExportOptions;
import ai.vital.vitalservice.FileType;
import ai.vital.vitalservice.ImportOptions;
import ai.vital.vitalservice.ServiceDeleteOperation;
import ai.vital.vitalservice.ServiceInsertOperation;
import ai.vital.vitalservice.ServiceOperation;
import ai.vital.vitalservice.ServiceOperations;
import ai.vital.vitalservice.ServiceOperations.Type;
import ai.vital.vitalservice.ServiceUpdateOperation;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.VitalStatus.Status;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.URIProperty;

public class ServiceOperationsImplementation {
	
	private static final String COMMONS_SCRIPTS_DOWNGRADE_UPGRADE_DATASCRIPT = "commons/scripts/DowngradeUpgradeDatascript";

	private final static Logger log = LoggerFactory.getLogger(ServiceOperationsImplementation.class);
	
	/**
	 * All service calls are wrapper
	 *
	 */
	public static abstract class ServiceOperationsExecutor {
	
		public VitalOrganization organization;
		
		public VitalApp app;

		public ServiceOperationsExecutor(VitalOrganization organization, VitalApp app) {
			super();
			this.organization = organization;
			this.app = app;
		}

		public abstract List<VitalTransaction> getTransactions() throws VitalServiceUnimplementedException, VitalServiceException;

		public abstract VitalTransaction createTransaction() throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract VitalStatus delete(URIProperty graphObjectURI) throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract ResultList graphQuery(VitalGraphQuery graphQuery) throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract void commitTransaction(VitalTransaction transaction) throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract void save(VitalSegment segment, GraphObject graphObject) throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract GraphObject get(URIProperty uri) throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract void rollbackTransaction(VitalTransaction transaction) throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract List<VitalSegment> listSegments() throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract void addSegment(VitalSegment segment) throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract VitalStatus bulkImport(VitalSegment segment, InputStream inputStream, String datasetURI) throws VitalServiceException, VitalServiceUnimplementedException;

		public abstract boolean reindexSegment(VitalSegment segment) throws Exception;

		public abstract VitalStatus bulkExport(VitalSegment segment, OutputStream outputStream, String datasetURI) throws Exception;

		public abstract boolean isPrime();

		public abstract ResultList callFunction(String function, Map<String, Object> params) throws Exception;
		
		public abstract VitalService getService();
		
		public abstract VitalServiceAdmin getServiceAdmin();
		
		public abstract VitalApp getApp();
		
			
	}

	public static class ServiceOperationExecutorImpl extends ServiceOperationsExecutor {

		VitalService service;

		public ServiceOperationExecutorImpl(VitalService service) {
			super(service.getOrganization(), service.getApp());
			this.service = service;
		}
		
		@Override
		public List<VitalTransaction> getTransactions() throws VitalServiceUnimplementedException, VitalServiceException {
			return service.getTransactions();
		}

		@Override
		public VitalTransaction createTransaction() throws VitalServiceException, VitalServiceUnimplementedException {
			return service.createTransaction();
		}

		@Override
		public VitalStatus delete(URIProperty graphObjectURI) throws VitalServiceException, VitalServiceUnimplementedException {
			return service.delete(graphObjectURI);
		}

		@Override
		public ResultList graphQuery(VitalGraphQuery graphQuery) throws VitalServiceException, VitalServiceUnimplementedException {
			return service.query(graphQuery);
		}

		@Override
		public void commitTransaction(VitalTransaction transaction) throws VitalServiceException, VitalServiceUnimplementedException {
			service.commitTransaction(transaction);
		}

		@Override
		public void save(VitalSegment segment, GraphObject graphObject) throws VitalServiceException, VitalServiceUnimplementedException {
			service.save(segment, graphObject, true);
		}

		@Override
		public GraphObject get(URIProperty uri) throws VitalServiceException, VitalServiceUnimplementedException {
			return service.get(GraphContext.ServiceWide, uri).first();
		}

		@Override
		public void rollbackTransaction(VitalTransaction transaction) throws VitalServiceException, VitalServiceUnimplementedException {
			service.rollbackTransaction(transaction);
		}

		@Override
		public List<VitalSegment> listSegments() throws VitalServiceException,
				VitalServiceUnimplementedException {
			return service.listSegments();
		}

		@Override
		public void addSegment(VitalSegment segment)
				throws VitalServiceException,
				VitalServiceUnimplementedException {
			throw new VitalServiceException("App access service type does not allow creating a segment");
		}

		@Override
		public VitalStatus bulkImport(VitalSegment segment,
				InputStream inputStream, String datasetURI) throws VitalServiceException,
				VitalServiceUnimplementedException {
			return service.bulkImport(segment, inputStream, datasetURI);
		}

		@Override
		public boolean reindexSegment(VitalSegment segment) throws Exception {

			if(service instanceof VitalServiceIndexedDB) {
				((VitalServiceIndexedDB) service).reindexSegment(app, segment);
				return true;
			} else {
				return false;
			}
			
		}

		@Override
		public VitalStatus bulkExport(VitalSegment segment,
				OutputStream outputStream, String datasetURI) throws Exception {
			return service.bulkExport(segment, outputStream, datasetURI);
		}

		@Override
		public boolean isPrime() {
			return service.getEndpointType() == EndpointType.VITALPRIME;
		}

		@Override
		public ResultList callFunction(String function,
				Map<String, Object> params) throws Exception {
			return service.callFunction(function, params);
		}

		@Override
		public VitalService getService() {
			return service;
		}

		@Override
		public VitalServiceAdmin getServiceAdmin() {
			return null;
		}

		@Override
		public VitalApp getApp() {
			return service.getApp();
		}
		
	}
	
	public static class ServiceOperationExecutorAdminImpl extends ServiceOperationsExecutor {

		VitalServiceAdmin adminService;
		
		public ServiceOperationExecutorAdminImpl(VitalServiceAdmin adminService, VitalApp app) {
			super(adminService.getOrganization(), app);
			this.adminService = adminService;
		}

		@Override
		public List<VitalTransaction> getTransactions() throws VitalServiceUnimplementedException, VitalServiceException {
			return adminService.getTransactions();
		}

		@Override
		public VitalTransaction createTransaction() throws VitalServiceException,
				VitalServiceUnimplementedException {
			return adminService.createTransaction();
		}

		@Override
		public VitalStatus delete(URIProperty graphObjectURI)
				throws VitalServiceException,
				VitalServiceUnimplementedException {
			return adminService.delete(app, graphObjectURI);
		}

		@Override
		public ResultList graphQuery(VitalGraphQuery graphQuery)
				throws VitalServiceException,
				VitalServiceUnimplementedException {
			return adminService.query(app, graphQuery);
		}

		@Override
		public void commitTransaction(VitalTransaction transaction)
				throws VitalServiceException,
				VitalServiceUnimplementedException {
			adminService.commitTransaction(transaction);
		}

		@Override
		public void save(VitalSegment segment, GraphObject graphObject)
				throws VitalServiceException,
				VitalServiceUnimplementedException {
			adminService.save(app, segment, graphObject, true);
		}

		@Override
		public GraphObject get(URIProperty uri) throws VitalServiceException,
				VitalServiceUnimplementedException {
			return adminService.get(app, GraphContext.ServiceWide, uri).first();
		}

		@Override
		public void rollbackTransaction(VitalTransaction transaction)
				throws VitalServiceException,
				VitalServiceUnimplementedException {
			adminService.rollbackTransaction(transaction);
		}

		@Override
		public List<VitalSegment> listSegments() throws VitalServiceException,
				VitalServiceUnimplementedException {
			return adminService.listSegments(app);
		}

		@Override
		public void addSegment(VitalSegment segment)
				throws VitalServiceException,
				VitalServiceUnimplementedException {
			adminService.addSegment(app, segment, true);
		}

		@Override
		public VitalStatus bulkImport(VitalSegment segment,
				InputStream inputStream, String datasetURI) throws VitalServiceException,
				VitalServiceUnimplementedException {
			return adminService.bulkImport(app, segment, inputStream, datasetURI);
		}

		@Override
		public boolean reindexSegment(VitalSegment segment) throws Exception {
			if(adminService instanceof VitalServiceAdminIndexedDB) {
				((VitalServiceAdminIndexedDB)adminService).reindexSegment(app, segment);
				return true;
			}
			return false;
		}

		@Override
		public VitalStatus bulkExport(VitalSegment segment,
				OutputStream outputStream, String datasetURI) throws Exception {
			return adminService.bulkExport(app, segment, outputStream, datasetURI);
		}

		@Override
		public boolean isPrime() {
			return adminService.getEndpointType() == EndpointType.VITALPRIME;
		}

		@Override
		public ResultList callFunction(String function,
				Map<String, Object> params) throws Exception {
			return adminService.callFunction(app, function, params);
		}

		@Override
		public VitalService getService() {
			return null;
		}

		@Override
		public VitalServiceAdmin getServiceAdmin() {
			return adminService;
		}

		@Override
		public VitalApp getApp() {
			return app;
		}
		
	}
	
	private ServiceOperationsExecutor executor;
	
	private ServiceOperations serviceOps;
	
	public ServiceOperationsImplementation(ServiceOperationsExecutor executor, ServiceOperations operations) {
		this.executor = executor;
		this.serviceOps = operations;
	}
	
	public VitalStatus execute() throws VitalServiceException, VitalServiceUnimplementedException {
	
		validateRequest();
		
		return doExecute();
		
	}
	
	VitalStatus doExecute() throws VitalServiceException, VitalServiceUnimplementedException {

		VitalTransaction transaction = null;

		InputStream inputStream = null;
		
		OutputStream outputStream = null;
		
		if(serviceOps.isTransaction()) {
			if( executor.getTransactions().size() > 0 ) throw new VitalServiceException("Cannot perform operations, an active transaction detected.");
			transaction = executor.createTransaction();
		}


		VitalStatus status = VitalStatus.withOK();
		List<URIProperty> failedURIs = new ArrayList<URIProperty>();
		status.setFailedURIs(failedURIs);
		
		int ok = 0;
		
		try {

			if(serviceOps.getType() == Type.DELETE) {


				
				for(ServiceOperation _do : serviceOps.getOperations()) {

					ServiceDeleteOperation sdo = (ServiceDeleteOperation) _do;
					
					if(sdo.getGraphObjectURI() != null) {

						try {
							executor.delete(sdo.getGraphObjectURI());
							ok++;
						} catch(Exception e) {
							failedURIs.add(sdo.getGraphObjectURI());
							if(transaction != null) throw new VitalServiceException("Deleting an object " + sdo.getGraphObjectURI() + " failed, rolling back transaction.");
						}

					} else {
					
						//select objects
						ResultList rl = executor.graphQuery(sdo.getGraphQuery());
						
						for(GraphObject g : rl) {
							
							if(g instanceof GraphMatch) {
								for(Object obj : g.getPropertiesMap().values()) {

									IProperty p = (IProperty) obj;


									if(!(p instanceof URIProperty)) continue;
									URIProperty u = (URIProperty) p;
									try {
										executor.delete(URIProperty.withString(u.get()));
										ok++;
									} catch(Exception e) {
										failedURIs.add(URIProperty.withString(u.get()));
										if(transaction != null) throw new VitalServiceException("Deleting an object " + u.get() + " failed, rolling back transaction.");
									}
								}
							} else {
							
								try {
									executor.delete(URIProperty.withString(g.getURI()));
									ok++;
								} catch(Exception e) {
									failedURIs.add(sdo.getGraphObjectURI());
									if(transaction != null) throw new VitalServiceException("Deleting an object " + g.getURI() + " failed, rolling back transaction.");
								}
							
							}
							
						}
					
					}

				}
				
				if(transaction != null) {
					executor.commitTransaction(transaction);
					transaction = null;
				}
				
				status.setMessage("Deleted: " + ok + " failed: " + failedURIs.size());

			} else if(serviceOps.getType() == Type.INSERT) {
			
				VitalSegment segment = serviceOps.getSegment();
				
				for(ServiceOperation so : serviceOps.getOperations()) {
					
					ServiceInsertOperation sio = (ServiceInsertOperation) so;
					
					try {
						executor.save(segment, sio.getGraphObject());
						ok++;
					} catch(Exception e) {
						failedURIs.add(URIProperty.withString(sio.getGraphObject().getURI()));
						if(transaction != null) throw new VitalServiceException("Inserting an object " + sio.getGraphObject().getURI() + " failed, rolling back transaction.");
					}
					
				}
				
				if(transaction != null) {
					executor.commitTransaction(transaction);
					transaction = null;
				}
				
				status.setMessage("Inserted: " + ok + ", failed: " + failedURIs.size());
			
			} else if(serviceOps.getType() == Type.UPDATE) {
			
				VitalSegment segment = serviceOps.getSegment();
				
				for(ServiceOperation so : serviceOps.getOperations()) {
					
					ServiceUpdateOperation suo = (ServiceUpdateOperation) so;
					
					try {
						GraphObject current = executor.get(suo.getURI());
						if(current == null) throw new Exception("Object not found: " + suo.getURI());
						
						GraphObject newVersion = null;
						
						if(suo.getGraphObject() != null) {
							newVersion = suo.getGraphObject();
						} else {
							suo.getClosure().call(current);
							newVersion = current;
						}
						
						executor.save(segment, newVersion);
						ok++;
					} catch(Exception e) {
						failedURIs.add(URIProperty.withString(suo.getGraphObject().getURI()));
						if(transaction != null) throw new VitalServiceException("Updating an object " + suo.getGraphObject().getURI() + " failed, rolling back transaction.");
					}
					
				}
				
				if(transaction != null) {
					executor.commitTransaction(transaction);
					transaction = null;
				}
				
				status.setMessage("Updated: " + ok + ", failed: " + failedURIs.size());
			
			} else if(serviceOps.getType() == Type.IMPORT) {
				
				VitalSegment segment = serviceOps.getSegment();
				ImportOptions importOptions = serviceOps.getImportOptions();
				String path = importOptions.getPath();
				
				VitalStreamInfo streamInfo = getStreamInfo(path, importOptions.getCompressed(), importOptions.getFileType());

				boolean createSegment = importOptions.isCreateSegment();
				
				List<VitalSegment> segments = executor.listSegments();
				
				boolean segmentExists = false;
				for(VitalSegment seg : segments) {
					if(seg.getRaw(Property_hasSegmentID.class).equals(segment.getRaw(Property_hasSegmentID.class))) {
						segmentExists = true;
						break;
					}
				}
				
				if(!segmentExists) {
					
					if(! createSegment ) throw new Exception("Segment does not exist: " + segment.get(Property_hasSegmentID.class));
					
					executor.addSegment(segment);
					
				} else {
					
					if(importOptions.isRemoveData()) {
						
						executor.delete(URIProperty.getMatchAllURI(segment));
						
					}
					
				}
				
				
				if(streamInfo.ft == FileType.block) {
					
					inputStream = new FileInputStream(path);
					
//					executor.bulkImport(pa)
					
				} else if(streamInfo.ft == FileType.ntriples) {
					
					inputStream = NTriplesToVitalBlockPipe.ntriples2VitalBlockStream(new FileInputStream(path));
					
				} else {
					throw new RuntimeException("Unhandled vital file type: " + streamInfo.ft);
				}
				
				status = executor.bulkImport(segment, inputStream, importOptions.getDatasetURI());
				
				if(status.getStatus() != Status.ok) {
					return status;
				}
				
				if(importOptions.isReindexSegment()) {
					
					executor.reindexSegment(segment);
					
				}
				
				return status;
				
			} else if(serviceOps.getType() == Type.EXPORT) {
				
				VitalSegment segment = serviceOps.getSegment();
				
				ExportOptions exportOptions = serviceOps.getExportOptions();
				
				String path = exportOptions.getPath();
				
				VitalStreamInfo streamInfo = getStreamInfo(path, exportOptions.getCompressed(), exportOptions.getFileType());
				
				if(streamInfo.ft == FileType.block) {
					
					outputStream = new BufferedOutputStream( new FileOutputStream( path) );
					
//					executor.bulkImport(pa)
					
				} else if(streamInfo.ft == FileType.ntriples) {
					
					inputStream = NTriplesToVitalBlockPipe.ntriples2VitalBlockStream(new FileInputStream(path));
					
				} else {
					throw new RuntimeException("Unhandled vital file type: " + streamInfo.ft);
				}
				
				status = executor.bulkExport(segment, outputStream, exportOptions.getDatasetURI());
				return status;
				
			} else if(serviceOps.getType() == Type.DOWNGRADE || serviceOps.getType() == Type.UPGRADE) {
				
				BaseDowngradeUpgradeOptions opts = null;
				
				if( serviceOps.getType() == Type.DOWNGRADE ) {
					opts = serviceOps.getDowngradeOptions();
				} else {
					opts = serviceOps.getUpgradeOptions();
				}
				
				if( executor.isPrime() && opts.getSourceSegment() != null && opts.getDestinationSegment() != null) {
					
					if(serviceOps.getDowngradeUpgradeBuilderContents() == null || serviceOps.getDowngradeUpgradeBuilderContents().isEmpty()) {
						throw new RuntimeException("No builder source contents, cannot serialize and use it remotely in prime");
					}
					
					Map<String, Object> m = new HashMap<String, Object>();
					m.put("builderString", serviceOps.getDowngradeUpgradeBuilderContents());
					//overridden params
					m.put("sourceSegment", opts.getSourceSegment());
					m.put("destinationSegment", opts.getDestinationSegment());
					m.put("deleteSourceSegment", opts.getDeleteSourceSegment());
					m.put("oldOntologyFileName", opts.getOldOntologyFileName());
					m.put("oldOntologiesDirectory", opts.getOldOntologiesDirectory());
					ResultList rl = executor.callFunction(COMMONS_SCRIPTS_DOWNGRADE_UPGRADE_DATASCRIPT, m);
					return rl.getStatus();
					
				}
				
				UpgradeDowngradeProcedure procedure = executor.getService() != null ? new UpgradeDowngradeProcedure(executor.getService()) : new UpgradeDowngradeProcedure(executor.getServiceAdmin(), executor.getApp());
				
				return procedure.execute(serviceOps);
				
			} else {
				throw new RuntimeException("Unhandled service type: " + serviceOps.getType());
			}
		} catch(Exception e) {
			log.error(e.getLocalizedMessage(), e);
			status.setStatus(Status.error);
			status.setMessage(e.getLocalizedMessage());
			return status;
		} finally {
			if(transaction != null) {
				try {
					executor.rollbackTransaction(transaction);
				}catch(Exception e){}
			}
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
		
		status.setSuccesses(ok);
		status.setErrors(failedURIs.size());
		return status;
		
	}

	void validateRequest() throws VitalServiceException {
		
		Type type = serviceOps.getType();
		
		if(type == Type.DELETE) {
		
			if(serviceOps.getOperations().size() < 1) throw new VitalServiceException("Empty operations list");
			
			for(ServiceOperation so : serviceOps.getOperations()) {
				
				if(so instanceof ServiceDeleteOperation) {

					ServiceDeleteOperation sdo = (ServiceDeleteOperation) so;
					
					if( sdo.getGraphObjectURI() == null && sdo.getGraphQuery() == null) throw new VitalServiceException("${sdo.class.simpleName} must have either graphObjectURI or graphQuery");
					
					if( sdo.getGraphObjectURI() != null && sdo.getGraphQuery() != null) throw new VitalServiceException("${sdo.class.simpleName} must have either graphObjectURI of graphQuery, not both");
										
				} else {
					throw new VitalServiceException("Only " + ServiceDeleteOperation.class.getSimpleName() + " allowed with type: " + type.name());
				}
				
			}
			
		} else if(type == Type.INSERT) {
		
			if(serviceOps.getOperations().size() < 1) throw new VitalServiceException("Empty operations list");
			
			for(ServiceOperation so : serviceOps.getOperations()) {
				
				VitalSegment segment = serviceOps.getSegment();
				if( segment == null ) throw new VitalServiceException("ServiceOps with type INSERT must have segment set");
				
				if(so instanceof ServiceInsertOperation) {
					
					ServiceInsertOperation sio = (ServiceInsertOperation) so;
					
					if( sio.getGraphObject() == null ) throw new VitalServiceException(sio.getClass().getSimpleName() + " must have graphObject set");
					
				} else {
					throw new VitalServiceException("Only ${ServiceInsertOperation.class.simpleName} allowed with type: ${type.name()}");
				}
				
			}
		
		} else if(type == Type.UPDATE) {
		
			if(serviceOps.getOperations().size() < 1) throw new VitalServiceException("Empty operations list");
			
			VitalSegment segment = serviceOps.getSegment();
			if( segment == null ) throw new VitalServiceException("ServiceOps with type INSERT must have segment set");
			
			for(ServiceOperation so : serviceOps.getOperations()) {
				
				if(so instanceof ServiceUpdateOperation) {
					
					ServiceUpdateOperation suo = (ServiceUpdateOperation) so;
					
					if(suo.getURI() == null) throw new VitalServiceException(suo.getClass().getSimpleName() + " must have URI set");
					
					if(suo.getClosure() != null && suo.getGraphObject() != null) throw new VitalServiceException("Either closure or graph object allowed in " + suo.getClass().getSimpleName() + ", not both");
					
					if(suo.getClosure() == null && suo.getGraphObject() == null) throw new VitalServiceException("Either closure or graph object allowed in " + suo.getClass().getSimpleName());
					
				} else {
					throw new VitalServiceException("Only " + ServiceUpdateOperation.class.getSimpleName() + " allowed with type: " + type.name());
				}
				
			}
		
		} else if(type == Type.IMPORT) {
			
			VitalSegment segment = serviceOps.getSegment();
			if(segment == null) throw new VitalServiceException("No segment to import");
			ImportOptions importOptions = serviceOps.getImportOptions();
			if(importOptions == null) throw new VitalServiceException("No import options");
			String path = importOptions.getPath();
			if(path == null || path.isEmpty()) throw new VitalServiceException("No import path");
			
			try {
				VitalStreamInfo streamInfo = getStreamInfo(path, importOptions.getCompressed(), importOptions.getFileType());
				
				
				if(streamInfo.ft == FileType.block) {
					
				} else if(streamInfo.ft == FileType.ntriples) {
					
				} else {
					throw new RuntimeException("Unhandled import vital file type: " + streamInfo.ft);
				}
				
			} catch(Exception e) {
				throw new VitalServiceException("import path params error: " + e.getLocalizedMessage());
			}
			
		} else if(type == Type.EXPORT) {
			
			VitalSegment segment = serviceOps.getSegment();
			if(segment == null) throw new VitalServiceException("No segment to export");
			ExportOptions exportOptions = serviceOps.getExportOptions();
			if(exportOptions == null) throw new VitalServiceException("No export options");
			String path = exportOptions.getPath();
			if(path == null || path.isEmpty()) throw new VitalServiceException("No export path");
		
			try {
				VitalStreamInfo streamInfo = getStreamInfo(path, exportOptions.getCompressed(), exportOptions.getFileType());
				
				if(streamInfo.ft == FileType.block) {
					
				} else if(streamInfo.ft == FileType.ntriples) {
					
				} else {
					throw new RuntimeException("Unhandled export vital file type: " + streamInfo.ft);
				}
				
			} catch(Exception e) {
				throw new VitalServiceException("export path params error: " + e.getLocalizedMessage());
			}
			
		} else if(type == Type.DOWNGRADE || type == Type.UPGRADE) {
			
			BaseDowngradeUpgradeOptions opts = null;
			
			if(type == Type.DOWNGRADE) {
				opts = serviceOps.getDowngradeOptions();
				if(opts == null) throw new RuntimeException("No downgrade options");
			} else {
				opts = serviceOps.getUpgradeOptions();
				if(opts == null) throw new RuntimeException("No upgrade options");
			}
			
			if(opts.getDestinationPath() != null && opts.getDestinationSegment() != null) throw new RuntimeException("Cannot use both destination path and segment");
			if(opts.getSourcePath() != null && opts.getSourceSegment() != null) throw new RuntimeException("Cannot use both source path and segment");
			
			if(opts.getDestinationPath() == null && opts.getDestinationSegment() == null) throw new RuntimeException("No destination segment nor path");
			if(opts.getSourcePath() == null && opts.getSourceSegment() == null) throw new RuntimeException("No source segment nor path");
			
			if(opts.getDestinationPath() != null && opts.getSourcePath() != null) {
				
			} else if(opts.getDestinationSegment() != null && opts.getSourceSegment() != null) {
				
			} else if(opts.getDestinationSegment() != null){
				throw new RuntimeException("Cannot mix source path -> destination segment");
			} else if(opts.getSourceSegment() != null) {
				throw new RuntimeException("Cannot mix source segment -> destination path");
			}
			
		}
		
	}
	
	
	static class VitalStreamInfo {
		Boolean compressed = null;
		FileType ft = null;
	}
	
	static VitalStreamInfo getStreamInfo(String path, Boolean setCompression, FileType setFileType) {
		
		VitalStreamInfo i = new VitalStreamInfo();
		
		Boolean inferredCompressed = path.endsWith(".gz") ? true : null;
		FileType inferredFT = null;
		
		Boolean compressed = setCompression;
		FileType ft = setFileType;
		
		if(path.endsWith(".vital") || path.endsWith(".vital.gz")) {
			inferredFT = FileType.block;
			if(path.endsWith(".vital")) {
				inferredCompressed = false;
			}
		} else if(path.endsWith(".nt") || path.endsWith(".nt.gz")) {
			inferredFT = FileType.ntriples;
			if(path.endsWith(".nt")) {
				inferredCompressed = false;
			}
		}
		
		if(inferredCompressed != null && compressed != null) {
			
			if(inferredCompressed.booleanValue() != compressed.booleanValue()) {
				throw new RuntimeException("Inferred compression [" + inferredCompressed + "] does not match set value [" + compressed + "]");
			}
			
		} else if(compressed != null) {
			i.compressed  = compressed;
		} else if(inferredCompressed == null) {
			throw new RuntimeException("No inferred nor set compression flag");
		} else {
			i.compressed = inferredCompressed;
		}
		
		if(inferredFT != null && ft != null) {
			
			if(inferredFT != ft) {
				throw new RuntimeException("Inferred fileType [" + inferredFT + "] does not match set value [" + ft + "]");
			}
			
		} else if(ft != null) {
			i.ft = ft;
		} else if(inferredFT == null) {
			throw new RuntimeException("No inferred nor set file type");
		} else {
			i.ft = inferredFT;
		}
		
		return i;
		
	}
	

}
