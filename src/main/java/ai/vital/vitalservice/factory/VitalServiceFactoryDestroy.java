package ai.vital.vitalservice.factory;

import ai.vital.allegrograph.service.AllegrographVitalServiceInitWrapper;
import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig;
import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig;
import ai.vital.lucene.disk.service.LuceneDiskVitalServiceInitWrapper;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.prime.client.IVitalPrimeClient;
import ai.vital.prime.client.VitalPrimeClientFactory;
import ai.vital.prime.service.config.VitalServicePrimeConfig;
import ai.vital.sql.service.SqlVitalServiceInitWrapper;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalsigns.model.VitalServiceRootKey;

class VitalServiceFactoryDestroy {

	static void destroy(VitalServiceConfig endpointConfig, VitalServiceRootKey rootKey) throws VitalServiceException {
		
		EndpointType type = endpointConfig.getEndpointtype();
		
		VitalServiceInitWrapper indexWrapper = null;
		
		VitalServiceInitWrapper wrapper = null;
		
		try {
		
			if(type == EndpointType.VITALPRIME) {
				
				IVitalPrimeClient client = VitalPrimeClientFactory.createClient(((VitalServicePrimeConfig)endpointConfig).getEndpointURL());
				
				VitalStatus status = client.destroy(rootKey);
				if(status.getStatus() != VitalStatus.Status.ok) {
					throw new VitalServiceException(status.toString());
				}
				
			} else if(type == EndpointType.ALLEGROGRAPH) {
			
				VitalServiceAllegrographConfig vsac = (VitalServiceAllegrographConfig) endpointConfig;
				wrapper = new AllegrographVitalServiceInitWrapper(vsac);
			
			}  else if(type == EndpointType.INDEXDB) {
				
				VitalServiceIndexedDBConfig idbConfig = (VitalServiceIndexedDBConfig) endpointConfig;
				
				VitalServiceConfig indexCfg = idbConfig.getIndexConfig();
				
				VitalServiceConfig dbConfig = idbConfig.getDbConfig();
				
				EndpointType itype = indexCfg.getEndpointtype();
				
				EndpointType dtype = dbConfig.getEndpointtype();
				
				if( itype == EndpointType.LUCENEDISK ) {
					
					VitalServiceLuceneDiskConfig vsldc = (VitalServiceLuceneDiskConfig)indexCfg;
					
					indexWrapper = new LuceneDiskVitalServiceInitWrapper(vsldc);
					
				} else {
					throw new VitalServiceException("Unhandled indexDB index type: " + itype);
				}
	
				
				if( dtype == EndpointType.ALLEGROGRAPH ) {
					
					VitalServiceAllegrographConfig vsac = (VitalServiceAllegrographConfig) dbConfig;
	
					wrapper = new AllegrographVitalServiceInitWrapper(vsac);
					
				} else if( dtype == EndpointType.SQL ) {
	
					wrapper = new SqlVitalServiceInitWrapper((VitalServiceSqlConfig) dbConfig);
					
				} else {
					throw new VitalServiceException("Unhandled indexDB database type: " + dtype);
				}
				
				
			} else if(type == EndpointType.MOCK) {
				
				throw new VitalServiceException("Mock service is an inmemory implementation and cannot be destroyed");
				
			} else if(type == EndpointType.LUCENEDISK) {
				
				VitalServiceLuceneDiskConfig vsldc = (VitalServiceLuceneDiskConfig)endpointConfig;
				
				wrapper = new LuceneDiskVitalServiceInitWrapper(vsldc);
				
			} else if(type == EndpointType.LUCENEMEMORY) {
				
				throw new VitalServiceException("Lucene memory cannot be destroyed");
				
			} else if(type == EndpointType.SQL) {
	
				wrapper = new SqlVitalServiceInitWrapper((VitalServiceSqlConfig) endpointConfig);
				
			} else {
				throw new RuntimeException("Unhandled init endpoint: " + type);
			}

			if(indexWrapper != null) {
				indexWrapper.destroy();
			}
			
			wrapper.destroy();
			
		} finally {
			
		}
	}
	
}
