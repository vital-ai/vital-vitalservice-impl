package ai.vital.lucene.disk.service;

import java.io.File;

import ai.vital.lucene.common.service.LuceneVitalServiceImplementation;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.service.lucene.impl.LuceneServiceDiskImpl;
import ai.vital.service.lucene.impl.LuceneServiceImpl;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;


public class VitalServiceLuceneDisk extends LuceneVitalServiceImplementation {

	private VitalServiceLuceneDisk(VitalOrganization organization, VitalApp app, File rootPath, Boolean bufferWrites, int commitAfterNWrites, int commitAfterNSeconds) {
		super(organization, app, initServiceImpl(rootPath, bufferWrites, commitAfterNWrites, commitAfterNSeconds));
	}

	private static LuceneServiceImpl initServiceImpl(File rootLocation, Boolean bufferWrites, int commitAfterNWrites, int commitAfterNSeconds) {
		LuceneServiceImpl impl = LuceneServiceDiskImpl.create(rootLocation, bufferWrites, commitAfterNWrites, commitAfterNSeconds);
		try {
			impl.open();
		} catch (LuceneException e) {
			throw new RuntimeException();
		}
		return impl;
	}
			
	public static VitalServiceLuceneDisk create(VitalServiceLuceneDiskConfig cfg, VitalOrganization organization, VitalApp app) {
		//security check

		return new VitalServiceLuceneDisk(organization, app, new File(cfg.getRootPath()), cfg.getBufferWrites(), cfg.getCommitAfterNWrites(), cfg.getCommitAfterNSeconds());
	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.LUCENEDISK;
	}
	
	

		
		
}