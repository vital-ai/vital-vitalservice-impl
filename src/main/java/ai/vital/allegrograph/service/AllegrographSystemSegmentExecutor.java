package ai.vital.allegrograph.service;

import static ai.vital.vitalservice.VitalServiceConstants.NO_TRANSACTION;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.triplestore.allegrograph.AllegrographWrapper;
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

public class AllegrographSystemSegmentExecutor implements SystemSegmentOperationsExecutor {

	private AllegrographWrapper wrapper;

	private final static Logger log = LoggerFactory.getLogger(AllegrographSystemSegmentExecutor.class);
	
	public AllegrographSystemSegmentExecutor(AllegrographWrapper wrapper) {
		this.wrapper = wrapper;
	}
	
	@Override
	public VitalSegment getSegmentResource(VitalSegment segment) {
		try {
			for(String u : wrapper.listAllSegments()) {
				if(segment.getURI().equals(u)) {
					return segment;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	@Override
	public ResultList query(VitalOrganization organization, VitalApp app,
			VitalQuery query, ResultList segmentsRL) {
		if(query instanceof VitalSelectQuery) {
			try {
				return wrapper.selectQuery((VitalSelectQuery) query);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else if(query instanceof VitalGraphQuery) {
			try {
				return wrapper.graphQuery((VitalGraphQuery) query);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new RuntimeException("unexpected query type: " + query.getClass().getCanonicalName());
		}
	}

	@Override
	public GraphObject getGraphObject(VitalOrganization organization,
			VitalApp app, VitalSegment segment, String uri, ResultList segmentsRL) {
		try {
			return wrapper.get(Arrays.asList(segment), URIProperty.withString(uri));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public VitalSegment createSegmentResource(VitalSegment segment, ResultList segmentsRL) {
		try {
			return wrapper.addSegment(segment);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void saveGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<GraphObject> asList, ResultList segmentsRL) {
		try {
			wrapper.save(NO_TRANSACTION, systemSegment, asList, Arrays.asList(systemSegment));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deleteGraphObjects(VitalOrganization org, VitalApp app,
			VitalSegment systemSegment, List<URIProperty> toDelete, ResultList segmentsRL) {
		try {
			wrapper.delete(NO_TRANSACTION, Arrays.asList(systemSegment), toDelete);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}

	@Override
	public void deleteSegmentResource(VitalSegment segment, ResultList segmentsRL) {
		try {
			wrapper.removeSegment(segment, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public AllegrographWrapper getWrapper() {
		return wrapper;
	}

	@Override
	public void prepareSegmentResources(ResultList segmentRL,
			VitalSegment segment, VitalProvisioning provisioning) {
		if(provisioning != null) {
			log.warn("Allegrograph ignores segment provisioning");
		}
		segmentRL.getResults().add(new ResultElement(segment, 1D));
	}

	@Override
	public int getSegmentSize(VitalSegment s, ResultList segmentsRL) {
		try {
			return wrapper.getSegmentSize(s);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void bulkExport(VitalSegment segment, ResultList segmentsRL,
			ByteArrayOutputStream bos) {
		try {
			wrapper.bulkExport(segment, bos, null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
