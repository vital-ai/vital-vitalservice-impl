package ai.vital.vitalservice.superadmin.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import ai.vital.indexeddb.service.superadmin.VitalServiceSuperAdminIndexedDB;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.ServiceOperationsImplementation.ServiceOperationsExecutor;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.superadmin.VitalServiceSuperAdmin;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;

public class ServiceOperationExecutorSuperAdminImpl extends ServiceOperationsExecutor {

	private VitalServiceSuperAdmin vitalServiceSuperAdmin;
	
	public ServiceOperationExecutorSuperAdminImpl(VitalServiceSuperAdmin vitalServiceSuperAdmin, VitalOrganization organization,
			VitalApp app) {
		super(organization, app);
		this.vitalServiceSuperAdmin = vitalServiceSuperAdmin;
	}

	@Override
	public List<VitalTransaction> getTransactions()
			throws VitalServiceUnimplementedException, VitalServiceException {
		return vitalServiceSuperAdmin.getTransactions();
	}

	@Override
	public VitalTransaction createTransaction() throws VitalServiceException,
			VitalServiceUnimplementedException {
		return vitalServiceSuperAdmin.createTransaction();
	}

	@Override
	public VitalStatus delete(URIProperty graphObjectURI)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return vitalServiceSuperAdmin.delete(organization, app, graphObjectURI);
	}

	@Override
	public ResultList graphQuery(VitalGraphQuery graphQuery)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return vitalServiceSuperAdmin.query(organization, app, graphQuery);
	}

	@Override
	public void commitTransaction(VitalTransaction transaction)
			throws VitalServiceException, VitalServiceUnimplementedException {
		vitalServiceSuperAdmin.commitTransaction(transaction);
	}

	@Override
	public void save(VitalSegment segment, GraphObject graphObject)
			throws VitalServiceException, VitalServiceUnimplementedException {
		vitalServiceSuperAdmin.save(organization, app, graphObject);
	}

	@Override
	public GraphObject get(URIProperty uri) throws VitalServiceException,
			VitalServiceUnimplementedException {
		return vitalServiceSuperAdmin.get(organization, app, GraphContext.ServiceWide, uri).first();
	}

	@Override
	public void rollbackTransaction(VitalTransaction transaction)
			throws VitalServiceException, VitalServiceUnimplementedException {
		vitalServiceSuperAdmin.rollbackTransaction(transaction);
	}

	@Override
	public List<VitalSegment> listSegments() throws VitalServiceException,
			VitalServiceUnimplementedException {
		return vitalServiceSuperAdmin.listSegments(organization, app);
	}

	@Override
	public void addSegment(VitalSegment segment) throws VitalServiceException,
			VitalServiceUnimplementedException {
		vitalServiceSuperAdmin.addSegment(organization, app, segment, true);
	}

	@Override
	public VitalStatus bulkImport(VitalSegment segment, InputStream inputStream, String datasetURI)
			throws VitalServiceException, VitalServiceUnimplementedException {
		return vitalServiceSuperAdmin.bulkImport(organization, app, segment, inputStream, datasetURI);
	}

	@Override
	public boolean reindexSegment(VitalSegment segment) throws Exception {
		if(vitalServiceSuperAdmin instanceof VitalServiceSuperAdminIndexedDB) {
			((VitalServiceSuperAdminIndexedDB)vitalServiceSuperAdmin).reindexSegment(organization, app, segment);
			return true;
		}
		return false;
	}

	@Override
	public VitalStatus bulkExport(VitalSegment segment,
			OutputStream outputStream, String datasetURI) throws Exception {
		return vitalServiceSuperAdmin.bulkExport(organization, app, segment, outputStream, datasetURI);
	}

	@Override
	public boolean isPrime() {
		return vitalServiceSuperAdmin.getEndpointType() == EndpointType.VITALPRIME;
	}

	@Override
	public ResultList callFunction(String function, Map<String, Object> params)
			throws Exception {
		return vitalServiceSuperAdmin.callFunction(organization, app, function, params);
	}

	@Override
	public VitalService getService() {
		return null;
	}

	@Override
	public VitalServiceAdmin getServiceAdmin() {
		return null;
	}

	@Override
	public VitalApp getApp() {
		return app;
	}

}
