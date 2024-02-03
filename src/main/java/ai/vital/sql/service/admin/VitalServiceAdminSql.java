package ai.vital.sql.service.admin;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.sql.VitalSqlImplementation;
import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.service.VitalServiceSql;
import ai.vital.sql.service.VitalSqlSystemSegmentExecutor;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
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
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionID;
import ai.vital.vitalsigns.model.property.URIProperty;

public class VitalServiceAdminSql extends
		AbstractVitalServiceAdminImplementation {

	VitalSqlImplementation impl;

	public VitalServiceAdminSql(VitalServiceSqlConfig config,
			VitalOrganization organization) {
		super(new SystemSegment(new VitalSqlSystemSegmentExecutor(new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig(config))))), organization);
		
		impl = ((VitalSqlSystemSegmentExecutor)systemSegment.getExecutor()).getImpl();

	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.SQL;
	}

	@Override
	public VitalStatus ping() throws VitalServiceUnimplementedException,
			VitalServiceException {
		try {
			impl.ping();
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
		return VitalStatus.withOKMessage("Sql database functional");
	}

	@Override
	protected VitalStatus _close() throws VitalServiceException,
			VitalServiceUnimplementedException {
		try {
			impl.close();
			impl = null;
		} catch (Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
		return VitalStatus.withOKMessage("OK");
	}


	@Override
	protected List<GraphObject> _getServiceWideList(VitalApp app, List<URIProperty> uris)
			throws VitalServiceException, VitalServiceUnimplementedException {
		List<String> urisList = toStringList(uris);
		try {
			return impl.getBatch(systemSegment.listSegmentsOnly(organization, app), urisList);
		} catch (Exception e) {
			throw new VitalServiceException(e.getLocalizedMessage());
		}
	}

	private List<String> toStringList(List<URIProperty> uris) {
		List<String> s = new ArrayList<String>(uris.size());
		for(URIProperty up : uris) {
			s.add(up.get());
		}
		return s;
	}

	@Override
	protected GraphObject _getServiceWide(VitalApp app, URIProperty uri)
			throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return impl.get(systemSegment.listSegmentsOnly(organization, app), uri);
		} catch (Exception e) {
			throw new VitalServiceException(e.getLocalizedMessage());
		}
	}

	@Override
	protected VitalStatus _deleteList(VitalTransaction transaction, VitalApp app, List<URIProperty> uris)
			throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return impl.delete(transaction, systemSegment.listSegmentsOnly(organization, app), uris);
		} catch (Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
	}

	@Override
	protected VITAL_GraphContainerObject _getExistingObjects(VitalApp app, List<String> uris) {
		try {
			return impl.getExistingObjects(systemSegment.listSegmentsOnly(organization, app), uris);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected ResultList _saveList(VitalTransaction transaction, VitalApp app, VitalSegment targetSegment,
			List<GraphObject> graphObjectsList)
			throws VitalServiceUnimplementedException, VitalServiceException {
		try {
			return impl.save(transaction, targetSegment, graphObjectsList, systemSegment.listSegmentsOnly(organization, app));
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _callFunction(VitalApp app, String function,
			Map<String, Object> arguments) throws VitalServiceException,
			VitalServiceUnimplementedException {
		throw new VitalServiceUnimplementedException("Sql endpoint does not support functions calls");
	}

	@Override
	protected ResultList _selectQuery(VitalApp app, VitalSelectQuery query)
			throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return impl.selectQuery(query);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _graphQuery(VitalApp app, VitalGraphQuery query)
			throws VitalServiceUnimplementedException, VitalServiceException {
		try {
			return impl.graphQuery(query);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected VitalStatus _bulkImport(VitalApp app, VitalSegment segment,
			InputStream inputStream, String datasetURI) throws VitalServiceUnimplementedException,
			VitalServiceException {
		try {
			return impl.bulkImport(segment, inputStream, datasetURI);
		} catch (Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
	}

	@Override
	protected VitalStatus _bulkExport(VitalApp app, VitalSegment segment,
			OutputStream outputStream, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		try {
			return impl.bulkExport(segment, outputStream, datasetURI);
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
	}

	@Override
	protected void _commitTransaction(TransactionWrapper transactionWrapper) throws VitalServiceException {
		try {
			impl.commitTransaction(transactionWrapper.getID());
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected void _createTransaction(VitalTransaction transaction) throws VitalServiceException {
		try {
			String transactionID = impl.createTransaction();
			transaction.set(Property_hasTransactionID.class, transactionID);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected void _rollbackTransaction(TransactionWrapper transactionWrapper)
			throws VitalServiceException {
		try {
			impl.rollbackTransaction(transactionWrapper.getID());
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected VitalStatus _deleteAll(VitalApp app, VitalSegment segment)
			throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return impl.deleteAll(segment);
		} catch (Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
	}
	
	@Override
	protected void _transactionsCheck() throws VitalServiceUnimplementedException {}
	
	
	/**
	 * Returns the table name for segment, it must have a valid URI
	 * @param segment
	 * @return
	 */
	public String getSegmentTableName(VitalSegment segment) {
		return impl.getSegmentTable(segment).tableName;
	}

}