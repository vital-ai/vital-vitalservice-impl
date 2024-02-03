package ai.vital.vitalservice.impl;

import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalsigns.model.VitalAuthKey;

/**
 * An interface that indicates the vitalservice implementation keeps the origin config and auth key
 *
 */
public interface IVitalServiceConfigAware {

	public VitalServiceConfig getConfig();

	public void setConfig(VitalServiceConfig config);
	
	public VitalAuthKey getAuthKey();
	
	public void setAuthKey(VitalAuthKey authKey);
	
	public void setName(String name);
	
	public String getName();
	
}
