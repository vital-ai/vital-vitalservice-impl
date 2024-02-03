package ai.vital.lucene.memory.service.superadmin;

import ai.vital.lucene.common.service.LuceneSystemSegmentExecutor;
import ai.vital.lucene.common.service.superadmin.VitalServiceSuperAdminLuceneCommonImpl;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.lucene.memory.service.VitalServiceLuceneMemory;
import ai.vital.lucene.memory.service.config.VitalServiceLuceneMemoryConfig;
import ai.vital.service.lucene.impl.LuceneServiceImpl;
import ai.vital.service.lucene.impl.LuceneServiceMemoryImpl;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.properties.Property_hasKey;

public class VitalServiceSuperAdminLuceneMemory extends VitalServiceSuperAdminLuceneCommonImpl {

	protected VitalServiceSuperAdminLuceneMemory() {
		super(initServiceImpl());
	}
	
	private static LuceneServiceImpl initServiceImpl() {
		LuceneServiceMemoryImpl impl = LuceneServiceMemoryImpl.create();
		try {
			impl.open();
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		
		
		LuceneSystemSegmentExecutor luceneSystemSegmentExecutor = new LuceneSystemSegmentExecutor(impl);
		SystemSegment ss = new SystemSegment(luceneSystemSegmentExecutor, false);
		try {
			ss.createSystemSegment();
			
			VitalServiceRootKey rootKey = (VitalServiceRootKey) new VitalServiceRootKey().generateURI((VitalApp)null);
			rootKey.set(Property_hasKey.class, VitalServiceLuceneMemory.ROOT_KEY);
			
			ss.addVitalServiceRootKey(rootKey);
			
		} catch (VitalServiceException e) {
			throw new RuntimeException(e);
		}
		
		return impl;
	}
	
	public static VitalServiceSuperAdminLuceneMemory create(VitalServiceLuceneMemoryConfig config) {
		

		return new VitalServiceSuperAdminLuceneMemory();
		
	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.LUCENEMEMORY;
	}
	
	
	
}