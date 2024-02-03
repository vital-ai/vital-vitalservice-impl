package ai.vital.allegrograph.service.admin;

import static ai.vital.vitalservice.VitalServiceConstants.NO_TRANSACTION;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.allegrograph.service.AllegrographSystemSegmentExecutor;
import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig;
import ai.vital.triplestore.allegrograph.AllegrographWrapper;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.AbstractVitalServiceAdminImplementation;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.RDFStatement;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionID;
import ai.vital.vitalsigns.model.property.URIProperty;

public class VitalServiceAdminAllegrograph extends AbstractVitalServiceAdminImplementation {

	AllegrographWrapper wrapper;
	
	private VitalServiceAdminAllegrograph( VitalServiceAllegrographConfig cfg,
			VitalOrganization _organization) {
		super(new SystemSegment(new AllegrographSystemSegmentExecutor(initWrapper(cfg))), _organization);
		
		wrapper = ((AllegrographSystemSegmentExecutor)systemSegment.getExecutor()).getWrapper();
		
//		organization = wrapper.getOrganization(_organization.organizationID.toString())
//		if(organization == null) throw new RuntimeException("Organization not found: ${_organization.organizationID}")
		
	}

	private static AllegrographWrapper initWrapper(VitalServiceAllegrographConfig cfg) {
		
		AllegrographWrapper wrapper = AllegrographWrapper.create(cfg.getServerURL(), cfg.getUsername(), cfg.getPassword(), cfg.getCatalogName(), cfg.getRepositoryName(), cfg.getPoolMaxTotal());
		try {
			wrapper.open();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return wrapper;
	}
			
	public static VitalServiceAdminAllegrograph create(VitalServiceAllegrographConfig cfg, VitalOrganization organization) {
		

		return new VitalServiceAdminAllegrograph(cfg, organization);
	}
			
	@Override
	public VitalStatus ping() throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			return wrapper.ping();
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected VitalStatus _close() throws VitalServiceException,
	VitalServiceUnimplementedException {

		try {
			wrapper.close();
		} catch(Exception e) {
		}
		return VitalStatus.withOK();
	}

	@Override
	protected ResultList _callFunction(VitalApp app,
			String function, Map<String, Object> arguments)
	throws VitalServiceException, VitalServiceUnimplementedException {
		throw new VitalServiceException("functions calls not supported in alleograph service");
	}

	@Override
	protected GraphObject _getServiceWide(VitalApp app,
			URIProperty uri) throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			return wrapper.get(systemSegment.listSegmentsOnly(organization, app), uri);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected List<GraphObject> _getServiceWideList(VitalApp app,
			List<URIProperty> uris) throws VitalServiceException,
	VitalServiceUnimplementedException {

		List<String> urisStrings = new ArrayList<String>();
		for(URIProperty uri : uris) {
			urisStrings.add(uri.get());
		}
		try {
			return wrapper.getBatch(systemSegment.listSegmentsOnly(organization, app), urisStrings);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _saveList(VitalTransaction transaction, VitalApp app,
			VitalSegment targetSegment, List<GraphObject> graphObjectsList)
	throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return wrapper.save(transaction, targetSegment, graphObjectsList, systemSegment.listSegmentsOnly(organization, app));
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected VitalStatus _deleteList(VitalTransaction transaction, VitalApp app,
			List<URIProperty> uris) throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			return wrapper.delete(transaction, systemSegment.listSegmentsOnly(organization, app), uris);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _selectQuery(VitalApp app,
			VitalSelectQuery query) throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			return wrapper.selectQuery(query);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}
	
	@Override
	protected ResultList _sparqlQuery(VitalOrganization organization, VitalApp app, VitalSparqlQuery query)
			throws VitalServiceUnimplementedException, VitalServiceException {
		try {
			return wrapper.sparqlQuery(query);	
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _graphQuery(VitalApp app,
			VitalGraphQuery gq) throws VitalServiceException {
		try {
			return wrapper.graphQuery(gq);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}


	@Override
	public EndpointType getEndpointType() {
		return EndpointType.ALLEGROGRAPH;
	}

	
	@Override
	public VitalStatus deleteObject(VitalApp app, GraphObject object)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.deleteObject(NO_TRANSACTION, app, object);
	}
	
	@Override
	public VitalStatus deleteObject(VitalTransaction transaction, VitalApp app, 
			GraphObject object) throws VitalServiceUnimplementedException,
			VitalServiceException {
		if(object instanceof RDFStatement) {
			try {
				return wrapper.deleteRDFStatements(transaction, Arrays.asList(object));
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}
		}
		
		return super.deleteObject(transaction, app, object);
	}

	
	@Override
	public VitalStatus deleteObjects(VitalApp app, List<GraphObject> objects)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.deleteObjects(NO_TRANSACTION, app, objects);
	}
	
	
	@Override
	public VitalStatus deleteObjects(VitalTransaction transaction, VitalApp app, List<GraphObject> arg0)
			throws VitalServiceException, VitalServiceUnimplementedException {
		//check if it's special rdf case
		
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
				return wrapper.deleteRDFStatements(transaction, arg0);
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}
		}
		
		//default
		return super.deleteObjects(transaction, app, arg0);
	}

			
	@Override
	public ResultList insert(VitalApp app, VitalSegment targetSegment, GraphObject graphObject)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.insert(NO_TRANSACTION, app, targetSegment, graphObject);
	}
	
	@Override
	public ResultList insert(VitalTransaction tx, VitalApp app, VitalSegment targetSegment, GraphObject graphObject)
			throws VitalServiceException, VitalServiceUnimplementedException {

		if(graphObject instanceof RDFStatement) {
			try {
				return wrapper.insertRDFStatements(tx, Arrays.asList(graphObject));
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}
		}
				
		//default
		return super.insert(tx, app, targetSegment, graphObject);
	}

	@Override
	public ResultList insert(VitalApp app, VitalSegment targetSegment,
			List<GraphObject> graphObjectsList)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return this.insert(NO_TRANSACTION, app, targetSegment, graphObjectsList);
	}
	
	@Override
	public ResultList insert(VitalTransaction tx, VitalApp app, VitalSegment targetSegment,
			List<GraphObject> graphObjectsList) throws VitalServiceException,
			VitalServiceUnimplementedException {
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
				return wrapper.insertRDFStatements(tx, rdfStatements);
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}
		}
			
		//default
		return super.insert(tx, app, targetSegment, graphObjectsList);
		
	}

			
	@Override
	protected void _commitTransaction(TransactionWrapper transaction) throws VitalServiceException  {
		try {
			wrapper.commitTransation(transaction.getID());
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}
		
	@Override
	protected void _createTransaction(VitalTransaction transaction) throws VitalServiceException {
		try {
			String tID = wrapper.createTransaction();
			transaction.set(Property_hasTransactionID.class, tID);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}
		
	@Override
	protected void _rollbackTransaction(TransactionWrapper transaction) throws VitalServiceException {
		try {
			wrapper.rollbackTransaction(transaction.getID());
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}
	
	@Override
	protected VITAL_GraphContainerObject _getExistingObjects(VitalApp app, List<String> uris) throws VitalServiceException {
		try {
			return wrapper.getExistingObjects(systemSegment.listSegmentsOnly(organization, app), uris);
		} catch (VitalServiceException e) {
			throw e;
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	public VitalStatus _bulkExport(VitalApp arg0, VitalSegment arg1, OutputStream arg2, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		try {
			return wrapper.bulkExport(arg1, arg2, datasetURI);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	public VitalStatus _bulkImport(VitalApp arg0, VitalSegment arg1, InputStream arg2, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		try {
			return wrapper.bulkImportBlockCompact(arg1, arg2, datasetURI);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected VitalStatus _deleteAll(VitalApp app, VitalSegment segment)
			throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return wrapper.deleteAll(segment);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected void _transactionsCheck()
			throws VitalServiceUnimplementedException {}

	
}
