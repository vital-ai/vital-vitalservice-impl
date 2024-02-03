package ai.vital.vitalservice.factory;

import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;

/**
 * common interface for service initialization
 *
 */
public interface VitalServiceInitWrapper {

	public SystemSegmentOperationsExecutor createExecutor();

	public VitalStatus isInitialized();
	
	public void initialize() throws VitalServiceException;
	
	public void close();
	
	public void destroy() throws VitalServiceException;
	
}
