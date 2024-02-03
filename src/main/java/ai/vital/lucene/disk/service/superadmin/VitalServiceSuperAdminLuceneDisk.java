package ai.vital.lucene.disk.service.superadmin;

import java.io.File;

import ai.vital.vitalservice.EndpointType;
import ai.vital.lucene.common.service.superadmin.VitalServiceSuperAdminLuceneCommonImpl;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.service.lucene.impl.LuceneServiceDiskImpl;
import ai.vital.service.lucene.impl.LuceneServiceImpl;

public class VitalServiceSuperAdminLuceneDisk extends VitalServiceSuperAdminLuceneCommonImpl {

	private VitalServiceSuperAdminLuceneDisk(File rootLocation, Boolean bufferWrites, int commitAfterNWrites, int commitAfterNSeconds) {
		super(initServiceImpl(rootLocation, bufferWrites, commitAfterNWrites, commitAfterNSeconds));
	}
			
	private static LuceneServiceImpl initServiceImpl(File rootLocation, Boolean bufferWrites, int commitAfterNWrites, int commitAfterNSeconds) {
		LuceneServiceDiskImpl impl = LuceneServiceDiskImpl.create(rootLocation, bufferWrites, commitAfterNWrites, commitAfterNSeconds);
		try {
			impl.open();
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		return impl;
	}
	
	public static VitalServiceSuperAdminLuceneDisk create(VitalServiceLuceneDiskConfig config) {
		

		
		return new VitalServiceSuperAdminLuceneDisk(new File(config.getRootPath()), config.getBufferWrites(), config.getCommitAfterNWrites(), config.getCommitAfterNSeconds());
		
	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.LUCENEDISK;
	}

		
	
} 
