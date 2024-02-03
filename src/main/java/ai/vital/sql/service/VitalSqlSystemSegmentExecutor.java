package ai.vital.sql.service;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.sql.VitalSqlImplementation;
import ai.vital.vitalservice.VitalServiceConstants;
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

public class VitalSqlSystemSegmentExecutor implements
		SystemSegmentOperationsExecutor {

	private final static Logger log = LoggerFactory.getLogger(VitalSqlSystemSegmentExecutor.class);
	
	VitalSqlImplementation impl;
	
	public VitalSqlSystemSegmentExecutor(VitalSqlImplementation impl) {
		super();
		this.impl = impl;
	}

	@Override
	public VitalSegment getSegmentResource(VitalSegment segment) {
		try {
			
			if( impl.segmentExists(segment) ) {
				return segment;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	@Override
	public ResultList query(VitalOrganization organization, VitalApp app,
			VitalQuery query, ResultList segmentsPoolRS) {

		try {
			if(query instanceof VitalSelectQuery) {
				return impl.selectQuery((VitalSelectQuery) query);
			} else if(query instanceof VitalGraphQuery) {
				return impl.graphQuery((VitalGraphQuery) query);
			} else throw new RuntimeException("Unhandled query type: " + query.getClass());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public GraphObject getGraphObject(VitalOrganization organization,
			VitalApp app, VitalSegment segment, String uri,
			ResultList segmentsPoolRS) {
		try {
			return impl.get(Arrays.asList(segment), URIProperty.withString(uri));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public VitalSegment createSegmentResource(VitalSegment segment,
			ResultList segmentsPoolRS) {
		try {
			return impl.addSegment(segment, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void saveGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<GraphObject> asList,
			ResultList segmentsPoolRS) {

		try {
			impl.save(VitalServiceConstants.NO_TRANSACTION, systemSegment, asList, Arrays.asList(systemSegment));
		} catch(Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void deleteGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<URIProperty> toDelete,
			ResultList segmentsPoolRS) {

		try {
			impl.delete(VitalServiceConstants.NO_TRANSACTION, Arrays.asList(systemSegment), toDelete);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void deleteSegmentResource(VitalSegment segment,
			ResultList segmentsPoolRS) {

		try {
			impl.deleteSegment(segment, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void prepareSegmentResources(ResultList segmentRL,
			VitalSegment segment, VitalProvisioning provisioning) {

		if(provisioning != null) {
			log.warn("Vitalsql ignores segment provisioning");
		}
		
		segmentRL.getResults().add(new ResultElement(segment, 1D));
		

	}

	public VitalSqlImplementation getImpl() {
		return impl;
	}

	@Override
	public int getSegmentSize(VitalSegment s, ResultList segmentsRL) {
		try {
			return impl.getSegmentSize(s);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void bulkExport(VitalSegment segment, ResultList segmentsRL,
			ByteArrayOutputStream bos) {
		try {
			impl.bulkExport(segment, bos, null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
