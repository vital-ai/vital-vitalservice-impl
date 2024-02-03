package ai.vital.lucene.memory.service;

import ai.vital.lucene.common.service.LuceneSystemSegmentExecutor;
import ai.vital.lucene.common.service.LuceneVitalServiceImplementation;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.lucene.memory.service.config.VitalServiceLuceneMemoryConfig;
import ai.vital.service.lucene.impl.LuceneServiceImpl;
import ai.vital.service.lucene.impl.LuceneServiceMemoryImpl;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.properties.Property_hasKey;

/**
 * since memory endpoint is volatile it should only be used in  
 *
 */
public class VitalServiceLuceneMemory extends LuceneVitalServiceImplementation {

	public final static String ROOT_KEY = "rrrr-rrrr-rrrr";
	
	public VitalServiceLuceneMemory(VitalOrganization organization, VitalApp app) {
		super(organization, app,  initServiceImpl(organization, app));
	}
	
	private static LuceneServiceImpl initServiceImpl(VitalOrganization organization, VitalApp app) {
		LuceneServiceImpl impl = LuceneServiceMemoryImpl.create();
		try {
			impl.open();
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		
//		impl.addOrganization(organization);
//		impl.addApp(organization, app);
//		impl.addSegment(organization, app, VitalSegment.withId('default'), new LuceneSegmentConfig(LuceneSegmentType.memory, true, true, null))
		
		LuceneSystemSegmentExecutor luceneSystemSegmentExecutor = new LuceneSystemSegmentExecutor(impl);
		SystemSegment ss = new SystemSegment(luceneSystemSegmentExecutor, false);
		try {
			ss.createSystemSegment();
			
			VitalServiceRootKey rootKey = (VitalServiceRootKey) new VitalServiceRootKey().generateURI((VitalApp)null);
			rootKey.set(Property_hasKey.class, ROOT_KEY);
			
			ss.addVitalServiceRootKey(rootKey);
			
			ss.addOrganization(rootKey, organization);
			
			ss.addApp(organization, app);
			
		} catch (VitalServiceException e) {
			throw new RuntimeException(e);
		}
		
		return impl;
	}
	
	public static VitalServiceLuceneMemory create(VitalServiceLuceneMemoryConfig cfg, VitalOrganization organization, VitalApp app) {


		VitalServiceLuceneMemory lm = new VitalServiceLuceneMemory(organization, app);
		return lm;
	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.LUCENEMEMORY;
	}


	public LuceneServiceImpl getServiceImpl_() {
		return this.serviceImpl;
	}		
	
	public VitalSegment addSegment(VitalSegment arg1, boolean arg2) throws VitalServiceUnimplementedException,
			VitalServiceException {
		return systemSegment.addSegment(organization, app, arg1);
	}
	
	public VitalSegment addSegment(VitalSegment config, VitalProvisioning provisioning, boolean createIfNotExists) throws VitalServiceUnimplementedException, VitalServiceException {
		return systemSegment.addSegment(organization, app, config, provisioning);
	}
	
}