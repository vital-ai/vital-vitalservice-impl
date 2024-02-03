package ai.vital.lucene.common.service.superadmin;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.lucene.common.service.LuceneSystemSegmentExecutor;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.service.lucene.impl.LuceneServiceImpl;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.superadmin.impl.AbstractVitalServiceSuperAdminImplementation;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.property.URIProperty;

public abstract class VitalServiceSuperAdminLuceneCommonImpl extends AbstractVitalServiceSuperAdminImplementation {

	LuceneServiceImpl serviceImpl;
	
	protected VitalServiceSuperAdminLuceneCommonImpl(
			LuceneServiceImpl serviceImpl) {
		super(new SystemSegment(new LuceneSystemSegmentExecutor(serviceImpl)));
		this.serviceImpl = serviceImpl;
		
	}

	@Override
	public VitalStatus ping() throws VitalServiceException,
			VitalServiceUnimplementedException {
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
	protected ResultList _callFunction(VitalOrganization organization, VitalApp app,
			String function, Map<String, Object> arguments)
	throws VitalServiceException, VitalServiceUnimplementedException {
		throw new VitalServiceUnimplementedException("Lucene service does not support functions calls");
	}

	@Override
	protected GraphObject _getServiceWide(VitalOrganization organization, VitalApp app, URIProperty uri)
	throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return serviceImpl.get(systemSegment.listSegmentsOnly(organization, app), uri);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected List<GraphObject> _getServiceWideList(VitalOrganization organization, VitalApp app, List<URIProperty> uris)
	throws VitalServiceException, VitalServiceUnimplementedException {

		List<GraphObject> gos = new ArrayList<GraphObject>();

		List<VitalSegment> segmentsPool = systemSegment.listSegmentsOnly(organization, app);
		
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
	protected ResultList _saveList(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			VitalSegment targetSegment, List<GraphObject> graphObjectsList)
	throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return serviceImpl.save(targetSegment, graphObjectsList, null);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected VitalStatus _deleteList(VitalTransaction transaction, VitalOrganization organization, VitalApp app,
			List<URIProperty> uris) throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			return serviceImpl.delete(systemSegment.listSegmentsOnly(organization, app), uris);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _selectQuery(VitalOrganization organization, VitalApp app,
			VitalSelectQuery query) throws VitalServiceException,
	VitalServiceUnimplementedException {
		try {
			return serviceImpl.selectQuery(query);
		} catch(Exception e) {
			throw new VitalServiceException(e);
		}
	}

	@Override
	protected ResultList _graphQuery(VitalOrganization organization, VitalApp app,
			VitalGraphQuery gq) {
		try {
			return serviceImpl.graphQuery(organization, app, gq);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected VITAL_GraphContainerObject _getExistingObjects(VitalOrganization organization, VitalApp app, List<String> uris) {
		try {
			return serviceImpl.getExistingObjects(listSegments(organization, app), uris);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public VitalStatus _bulkExport(VitalOrganization organization, VitalApp arg0, VitalSegment arg1, OutputStream arg2, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		VitalSegment segment = systemSegment.getExistingSegment(organization, arg0, arg1);
		return serviceImpl.bulkExport(segment, arg2, datasetURI);
	}

	@Override
	public VitalStatus _bulkImport(VitalOrganization organization, VitalApp arg0, VitalSegment arg1, InputStream arg2, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {
		VitalSegment segment = systemSegment.getExistingSegment(organization, arg0, arg1);
		return serviceImpl.bulkImport(segment, arg2, datasetURI);
	}

	@Override
	protected VitalStatus _deleteAll(VitalOrganization organization, VitalApp app, VitalSegment segment) throws VitalServiceException, VitalServiceUnimplementedException {
		try {
			return serviceImpl.deleteAll(segment);
		} catch (LuceneException e) {
			throw new VitalServiceException(e);
		}
	}
	
	

}
