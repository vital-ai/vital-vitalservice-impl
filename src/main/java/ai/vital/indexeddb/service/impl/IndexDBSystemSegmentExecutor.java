package ai.vital.indexeddb.service.impl;

import java.io.ByteArrayOutputStream;
import java.util.List;

import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.property.URIProperty;

public class IndexDBSystemSegmentExecutor implements SystemSegmentOperationsExecutor {

	SystemSegmentOperationsExecutor indexExecutor;
	
	SystemSegmentOperationsExecutor dbExecutor;

	public IndexDBSystemSegmentExecutor(
			SystemSegmentOperationsExecutor indexExecutor,
			SystemSegmentOperationsExecutor dbExecutor) {
		super();
		this.indexExecutor = indexExecutor;
		this.dbExecutor = dbExecutor;
	}

	@Override
	public VitalSegment getSegmentResource(VitalSegment segment) {
		return dbExecutor.getSegmentResource(segment);
	}

	@Override
	public ResultList query(VitalOrganization organization, VitalApp app,
			VitalQuery query, ResultList segmentsPoolRS) {
		return dbExecutor.query(organization, app, query, segmentsPoolRS);
	}

	@Override
	public GraphObject getGraphObject(VitalOrganization organization,
			VitalApp app, VitalSegment segment, String uri,
			ResultList segmentsPoolRS) {
		return dbExecutor.getGraphObject(organization, app, segment, uri, segmentsPoolRS);
	}

	@Override
	public VitalSegment createSegmentResource(VitalSegment segment,
			ResultList segmentsPoolRS) {
		indexExecutor.createSegmentResource(segment, segmentsPoolRS);
		return dbExecutor.createSegmentResource(segment, segmentsPoolRS);
	}

	@Override
	public void saveGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<GraphObject> asList,
			ResultList segmentsPoolRS) {
		dbExecutor.saveGraphObjects(org, app, systemSegment, asList, segmentsPoolRS);
	}

	@Override
	public void deleteGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<URIProperty> toDelete,
			ResultList segmentsPoolRS) {
		dbExecutor.deleteGraphObjects(org, app, systemSegment, toDelete, segmentsPoolRS);
	}

	@Override
	public void deleteSegmentResource(VitalSegment segment,
			ResultList segmentsPoolRS) {
		indexExecutor.deleteSegmentResource(segment, segmentsPoolRS);
		dbExecutor.deleteSegmentResource(segment, segmentsPoolRS);
	}

	@Override
	public void prepareSegmentResources(ResultList segmentRL,
			VitalSegment segment, VitalProvisioning provisioning) {
		dbExecutor.prepareSegmentResources(segmentRL, segment, provisioning);
	}

	public SystemSegmentOperationsExecutor getIndexExecutor() {
		return indexExecutor;
	}

	public SystemSegmentOperationsExecutor getDbExecutor() {
		return dbExecutor;
	}

	@Override
	public int getSegmentSize(VitalSegment s, ResultList segmentsRL) {
		return dbExecutor.getSegmentSize(s, segmentsRL);
	}

	@Override
	public void bulkExport(VitalSegment segment, ResultList segmentsRL,
			ByteArrayOutputStream bos) {
		dbExecutor.bulkExport(segment, segmentsRL, bos);
	}
	

}
