package ai.vital.service.lucene.impl;

import java.util.HashMap;
import java.util.Map;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.lucene.model.LuceneSegment;
import ai.vital.lucene.model.LuceneSegmentType;
import ai.vital.service.lucene.model.LuceneSegmentConfig;
import ai.vital.vitalsigns.model.VitalSegment;

public class LuceneServiceMemoryImpl extends LuceneServiceImpl {
	
	private LuceneServiceMemoryImpl() {
		super();
	}

	public static LuceneServiceMemoryImpl create() {
		

		return new LuceneServiceMemoryImpl();
	}
	

	@Override
	public void initializeRootIndices() throws LuceneException {

	}

	@Override
	protected LuceneSegmentConfig getConfig(VitalSegment segment) {

		return new LuceneSegmentConfig(LuceneSegmentType.memory, true, true, null);
		
	}

	@Override
	protected Map<String, LuceneSegment> openInitialSegments() {
		return new HashMap<String, LuceneSegment>();
	}

	
}
