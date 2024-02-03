package ai.vital.lucene.common.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.lucene.exception.LuceneException;
import ai.vital.service.lucene.impl.LuceneServiceImpl;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.AbstractVitalServiceImplementation;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.property.URIProperty;

public abstract class LuceneVitalServiceImplementation extends AbstractVitalServiceImplementation {

	protected LuceneServiceImpl serviceImpl = null;
	
	protected LuceneVitalServiceImplementation(
			VitalOrganization _organization, VitalApp _app, LuceneServiceImpl serviceImpl2) {
		super(new SystemSegment(new LuceneSystemSegmentExecutor(serviceImpl2)), _organization, _app);
		
		this.serviceImpl = serviceImpl2;
		
//		organization = serviceImpl.getOrganization(_organization.organizationID.toString())
//		if(organization == null) throw new RuntimeException("Organization not found: ${_organization.organizationID.toString()}")
//		app = serviceImpl.getApp(_organization.organizationID.toString(), _app.appID.toString())
//		if(app == null) throw new RuntimeException("App not found: ${_app.appID}, organizationID: ${_organization.organizationID}")
	}

	@Override
	public VitalStatus ping()  {
		return VitalStatus.OK;
	}


	@Override
	protected VitalStatus _close() throws VitalServiceException,
			VitalServiceUnimplementedException {
		try {
			serviceImpl.close();
		} catch(Exception e) {
		}
		return VitalStatus.OK;
	}

	@Override
	protected ResultList _callFunction(
			String function, Map<String, Object> arguments)
			throws VitalServiceException, VitalServiceUnimplementedException {
		throw new VitalServiceUnimplementedException("Lucene service does not support functions calls");
	}

	@Override
	protected GraphObject _getServiceWide(URIProperty uri)
			throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return serviceImpl.get(systemSegment.listSegmentsOnly(organization, app), uri);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected List<GraphObject> _getServiceWideList(List<URIProperty> uris)
			throws VitalServiceException, VitalServiceUnimplementedException {

		List<VitalSegment> segmentsPool = systemSegment.listSegmentsOnly(organization, app);
		
		List<GraphObject> gos = new ArrayList<GraphObject>();
		
		try {
			for(URIProperty uri : uris) {
				GraphObject g = serviceImpl.get(segmentsPool, uri);
				if(g != null) {
					gos.add(g);
				}
			}
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
				
		return gos;
	}

	@Override
	protected ResultList _saveList(VitalTransaction transaction, 
			VitalSegment targetSegment, List<GraphObject> graphObjectsList)
			throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return serviceImpl.save(targetSegment, graphObjectsList, systemSegment.listSegmentsOnly(organization, app));
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected VitalStatus _deleteList(VitalTransaction transaction, 
			List<URIProperty> uris) throws VitalServiceException,
			VitalServiceUnimplementedException {
		try {
			return serviceImpl.delete(systemSegment.listSegmentsOnly(organization, app), uris);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _selectQuery(
			VitalSelectQuery query) throws VitalServiceException,
			VitalServiceUnimplementedException {
		try {
			return serviceImpl.selectQuery(query);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _graphQuery(VitalGraphQuery gq) throws VitalServiceException {
		try {
			return serviceImpl.graphQuery(organization, app, gq);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}


	@Override
	protected VITAL_GraphContainerObject _getExistingObjects(
			List<String> uris) throws VitalServiceException {
		try {
			return serviceImpl.getExistingObjects(systemSegment.listSegmentsOnly(organization, app), uris);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
		
	}

	@Override
	public VitalStatus _bulkExport(VitalSegment arg0, OutputStream arg1, String arg2)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return serviceImpl.bulkExport(arg0, arg1, arg2);
	}

	@Override
	public VitalStatus _bulkImport(VitalSegment arg0, InputStream arg1, String arg2)
			throws VitalServiceUnimplementedException, VitalServiceException {
		return serviceImpl.bulkImport(arg0, arg1, arg2);
	}

	@Override
	protected VitalStatus _deleteAll(VitalSegment segment) throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return serviceImpl.deleteAll(segment);
		} catch (LuceneException e) {
			throw new VitalServiceException(e);
		}
	}

	
}
