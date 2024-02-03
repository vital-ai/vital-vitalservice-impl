package ai.vital.allegrograph.service;

import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig;
import ai.vital.triplestore.allegrograph.AllegrographWrapper;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.factory.VitalServiceInitWrapper;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;

public class AllegrographVitalServiceInitWrapper implements VitalServiceInitWrapper {

	private VitalServiceAllegrographConfig vsac;
	
	private AllegrographWrapper agw;

	public AllegrographVitalServiceInitWrapper(
			VitalServiceAllegrographConfig vsac) {
		this.vsac = vsac;
	}

	@Override
	public VitalStatus isInitialized() {
		
		agw = AllegrographWrapper.create(vsac.getServerURL(), vsac.getUsername(), vsac.getPassword(), vsac.getCatalogName(), vsac.getRepositoryName(), vsac.getPoolMaxTotal());
		
		try {
			agw.open();
		} catch (Exception e) {
			return VitalStatus.withError("Allegrograph connection failed: " + e.getLocalizedMessage());
		}
		
		try {
			VitalStatus ping = agw.ping();
			if( ping.getStatus() != VitalStatus.Status.ok ) {
				return VitalStatus.withError("Allegrograph ping failed: " + ping.getMessage());
			}
		} catch (Exception e) {
			return VitalStatus.withError("Allegrograph ping failed: " + e.getLocalizedMessage());
		}
		
		return VitalStatus.withOK();

	}

	@Override
	public void initialize() throws VitalServiceException {

		agw = AllegrographWrapper.create(vsac.getServerURL(), vsac.getUsername(), vsac.getPassword(), vsac.getCatalogName(), vsac.getRepositoryName(), vsac.getPoolMaxTotal());
		
		try {
			agw.open();
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}
		
//		agw = AllegrographWrapper.create(vsac.getServerURL(), vsac.getUsername(), vsac.getPassword(), vsac.getCatalogName(), vsac.getRepositoryName());
//		
//		try {
//			agw.open();
//		} catch (Exception e) {
//			throw new VitalServiceException(e);
//		}
//		

	}

	@Override
	public void close() {

		try {
			agw.close();
		} catch(Exception e) {}

	}

	@Override
	public SystemSegmentOperationsExecutor createExecutor() {
		return new AllegrographSystemSegmentExecutor(agw);
	}

	@Override
	public void destroy() throws VitalServiceException {

		AllegrographWrapper agw = AllegrographWrapper.create(vsac.getServerURL(), vsac.getUsername(), vsac.getPassword(), vsac.getCatalogName(), vsac.getRepositoryName(), vsac.getPoolMaxTotal());
		
		try {
			agw.open();
			agw.purge();
		} catch (Exception e) {
			throw new VitalServiceException("Allegrograph connection failed: " + e.getLocalizedMessage());
		} finally {
			try {
				agw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		
	}

}
