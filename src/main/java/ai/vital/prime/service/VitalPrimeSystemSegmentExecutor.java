package ai.vital.prime.service;

import java.io.ByteArrayOutputStream;
import java.util.List;

import ai.vital.prime.client.IVitalPrimeClient;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.property.URIProperty;

public class VitalPrimeSystemSegmentExecutor implements SystemSegmentOperationsExecutor {

	IVitalPrimeClient client;
	
	public VitalPrimeSystemSegmentExecutor(IVitalPrimeClient client) {
		super();
		this.client = client;
	}

	@Override
	public VitalSegment getSegmentResource(VitalSegment segment) {
		throw new RuntimeException("Vital prime client shouldn't check system segment");
	}

	@Override
	public ResultList query(VitalOrganization organization, VitalApp app,
			VitalQuery query, ResultList segmentsPoolRS) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GraphObject getGraphObject(VitalOrganization organization,
			VitalApp app, VitalSegment segment, String uri,
			ResultList segmentsPoolRS) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VitalSegment createSegmentResource(VitalSegment segment,
			ResultList segmentsPoolRS) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void saveGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<GraphObject> asList,
			ResultList segmentsPoolRS) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<URIProperty> toDelete,
			ResultList segmentsPoolRS) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteSegmentResource(VitalSegment segment,
			ResultList segmentsPoolRS) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepareSegmentResources(ResultList segmentRL,
			VitalSegment segment, VitalProvisioning provisioning) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getSegmentSize(VitalSegment s, ResultList segmentsRL) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void bulkExport(VitalSegment segment, ResultList segmentsRL,
			ByteArrayOutputStream bos) {
		// TODO Auto-generated method stub
		
	}

}
