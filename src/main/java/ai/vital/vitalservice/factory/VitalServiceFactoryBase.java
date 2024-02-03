package ai.vital.vitalservice.factory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.impl.IVitalServiceConfigAware;
import ai.vital.vitalsigns.meta.NamedServiceProvider;
import ai.vital.vitalsigns.utils.StringUtils;


public class VitalServiceFactoryBase {

	public final static String DEFAULT_PROFILE = "default";
	
	protected VitalServiceFactoryBase(){}
	
	protected static Map<EndpointType, EndpointConfigCreator<?>> endpointType2Creator = new HashMap<EndpointType, EndpointConfigCreator<?>>();
	
	public final static String VitalSystemSegment = "vital_system";
	
	protected static List<IVitalServiceConfigAware> allOpenServices = Collections.synchronizedList(new ArrayList<IVitalServiceConfigAware>());
	
	protected final static int maxActiveServices = 1000;
	
	protected static void checkServiceName(String serviceName) throws VitalServiceException {
		
		if(StringUtils.isEmpty(serviceName)) throw new VitalServiceException("Service name must not be null nor empty");
		
		synchronized(allOpenServices) {
			for(IVitalServiceConfigAware vs : allOpenServices) {
				if(serviceName.equals(vs.getName())) throw new VitalServiceException("Service with name: " + serviceName + " already open");
			}
		}
		
	}
	
	static {
		
		NamedServiceProvider.provider = new NamedServiceProvider() {
			
			@Override
			public Object getNamedService(String name) {

				synchronized (allOpenServices) {
					
					for(IVitalServiceConfigAware ca : allOpenServices ) {
						if(name.equals(ca.getName())) {
							return ca;
						}
					}
				}
				
				return null;
			}
		};
		
	}

}
