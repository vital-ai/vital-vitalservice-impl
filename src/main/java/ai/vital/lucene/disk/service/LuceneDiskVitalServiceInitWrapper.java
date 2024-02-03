package ai.vital.lucene.disk.service;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import ai.vital.lucene.common.service.LuceneSystemSegmentExecutor;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.service.lucene.impl.LuceneServiceDiskImpl;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.factory.VitalServiceInitWrapper;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;

public class LuceneDiskVitalServiceInitWrapper implements
		VitalServiceInitWrapper {

	private VitalServiceLuceneDiskConfig vsldc;
	private LuceneServiceDiskImpl lsdi;

	public LuceneDiskVitalServiceInitWrapper(VitalServiceLuceneDiskConfig vsldc) {
		this.vsldc = vsldc;
	}

	@Override
	public SystemSegmentOperationsExecutor createExecutor() {
		return new LuceneSystemSegmentExecutor(lsdi);
	}

	@Override
	public VitalStatus isInitialized() {
		
		File rootLocation = new File(vsldc.getRootPath());
		
		if(!rootLocation.exists()) {
			return VitalStatus.withError("Lucene index root location does not exist: " + rootLocation.getAbsolutePath());
		}
		
		lsdi = LuceneServiceDiskImpl.create(rootLocation, vsldc.getBufferWrites(), vsldc.getCommitAfterNWrites(), vsldc.getCommitAfterNSeconds());
		
		try {
			lsdi.open();
		} catch (LuceneException e) {
			return VitalStatus.withError("Couldn't initialize lucene impl.: " + e.getLocalizedMessage());
		}
		
		return VitalStatus.withOK();
	}

	@Override
	public void initialize() throws VitalServiceException {

		File rootLocation = new File(vsldc.getRootPath());
		
		LuceneServiceDiskImpl.init(rootLocation);
		
		lsdi = LuceneServiceDiskImpl.create(rootLocation, vsldc.getBufferWrites(), vsldc.getCommitAfterNWrites(), vsldc.getCommitAfterNSeconds());
		
		try {
			lsdi.open();
		} catch (LuceneException e) {
			throw new VitalServiceException(e);
		}

//		executor = new LuceneSystemSegmentExecutor(lsdi);
		
	}

	@Override
	public void close() {
		try {
			lsdi.close();
		} catch(Exception e) {}
	}

	@Override
	public void destroy() throws VitalServiceException {

		File rootLocation = new File(vsldc.getRootPath());

		try {
			FileUtils.deleteDirectory(rootLocation);
		} catch (IOException e) {
			throw new VitalServiceException(e);
		}
		
	}

}
