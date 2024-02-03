package ai.vital.lucene.disk.service.admin;

import java.io.File;

import ai.vital.lucene.common.service.admin.LuceneVitalServiceAdminImplementation2;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.service.lucene.impl.LuceneServiceDiskImpl;
import ai.vital.service.lucene.impl.LuceneServiceImpl;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalsigns.model.VitalOrganization;


public class VitalServiceAdminLuceneDisk extends LuceneVitalServiceAdminImplementation2 {

	private VitalServiceAdminLuceneDisk(VitalOrganization organization, File rootPath, Boolean bufferWrites, int commitAfterNWrites, int commitAfterNSeconds) {
		super(organization, initServiceImpl(rootPath, bufferWrites, commitAfterNWrites, commitAfterNSeconds));
	}
	
	private static LuceneServiceImpl initServiceImpl(File rootPath, Boolean bufferWrites, int commitAfterNWrites, int commitAfterNSeconds) {
		LuceneServiceImpl impl = LuceneServiceDiskImpl.create(rootPath, bufferWrites, commitAfterNWrites, commitAfterNSeconds);
		try {
			impl.open();
		} catch (LuceneException e) {
			throw new RuntimeException(e);
		}
		return impl;
	}
	
	public static VitalServiceAdminLuceneDisk create(VitalServiceLuceneDiskConfig config, VitalOrganization organization) {
		return new VitalServiceAdminLuceneDisk(organization, new File(config.getRootPath()), config.getBufferWrites(), config.getCommitAfterNWrites(), config.getCommitAfterNSeconds());
		
	}

	@Override
	public EndpointType getEndpointType() {
		return EndpointType.LUCENEDISK;
	}

}
