package ai.vital.lucene.common.service;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.lucene.exception.LuceneException;
import ai.vital.service.lucene.impl.LuceneServiceImpl;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.property.URIProperty;

public class LuceneSystemSegmentExecutor implements
		SystemSegmentOperationsExecutor {

	private final static Logger log = LoggerFactory.getLogger(LuceneSystemSegmentExecutor.class);
	
	LuceneServiceImpl impl;
	
	public LuceneSystemSegmentExecutor(
			LuceneServiceImpl impl) {
		super();
		this.impl = impl;
	}

	@Override
	public VitalSegment getSegmentResource(VitalSegment segment) {

		if( impl.getAllSegments().contains(segment.getURI() ) ) {
			return segment;
		}
		
		return null;
	}

	@Override
	public ResultList query(VitalOrganization organization, VitalApp app, VitalQuery query, ResultList segmentsRL) {

		try {
			if(query instanceof VitalSelectQuery) {
				return impl.selectQuery((VitalSelectQuery) query);
			} else if(query instanceof VitalGraphQuery) {
				return impl.graphQuery(organization, app, (VitalGraphQuery) query);
			} else {
				throw new RuntimeException("Unexpected query: " + query);
			}
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public GraphObject getGraphObject(VitalOrganization organization,
			VitalApp app, VitalSegment segment, String uri, ResultList segmentsRL) {

		try {
			return impl.get(Arrays.asList(segment), URIProperty.withString(uri));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public VitalSegment createSegmentResource(VitalSegment segment, ResultList segmentsRL) {

		try {
			impl.addSegment(segment);
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		
		return segment;
	}

	@Override
	public void saveGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<GraphObject> asList, ResultList segmentsRL) {

		try {
			impl.save(systemSegment, asList, Arrays.asList(systemSegment));
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void deleteGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<URIProperty> toDelete, ResultList segmentsRL) {

		try {
			impl.delete(Arrays.asList(systemSegment), toDelete);
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void deleteSegmentResource(VitalSegment segment, ResultList segmentsRL) {

		try {
			impl.removeSegment(segment, true);
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void prepareSegmentResources(ResultList segmentRL,
			VitalSegment segment, VitalProvisioning provisioning) {
		if(provisioning != null) {
			log.warn("Lucene system segment ignores provisioning");
		}
		segmentRL.getResults().add(new ResultElement(segment, 1D));
	}

	@Override
	public int getSegmentSize(VitalSegment s, ResultList segmentsRL) {
		try {
			return impl.getSegmentSize(s);
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void bulkExport(VitalSegment segment, ResultList segmentsRL,
			ByteArrayOutputStream bos) {
		try {
			impl.bulkExport(segment, bos, null);
		} catch (VitalServiceException e) {
			throw new RuntimeException(e);
		}
	}
	
}
