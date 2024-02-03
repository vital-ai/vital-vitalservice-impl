package ai.vital.vitalservice.superadmin.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import ai.vital.allegrograph.service.AllegrographVitalServiceInitWrapper;
import ai.vital.allegrograph.service.VitalServiceAllegrographConfigCreator;
import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig;
import ai.vital.allegrograph.service.superadmin.VitalServiceSuperAdminAllegrograph;
import ai.vital.indexeddb.service.VitalServiceIndexedDBConfigCreator;
import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig;
import ai.vital.indexeddb.service.superadmin.VitalServiceSuperAdminIndexedDB;
import ai.vital.lucene.disk.service.LuceneDiskVitalServiceInitWrapper;
import ai.vital.lucene.disk.service.VitalServiceLuceneDiskConfigCreator;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.lucene.disk.service.superadmin.VitalServiceSuperAdminLuceneDisk;
import ai.vital.lucene.memory.service.VitalServiceLuceneMemoryConfigCreator;
import ai.vital.lucene.memory.service.config.VitalServiceLuceneMemoryConfig;
import ai.vital.lucene.memory.service.superadmin.VitalServiceSuperAdminLuceneMemory;
import ai.vital.mock.service.VitalServiceMockConfigCreator;
import ai.vital.mock.service.config.VitalServiceMockConfig;
import ai.vital.mock.service.superadmin.VitalServiceSuperAdminMock;
import ai.vital.prime.client.IVitalPrimeClient;
import ai.vital.prime.client.VitalPrimeClientFactory;
import ai.vital.prime.service.VitalServicePrimeConfigCreator;
import ai.vital.prime.service.config.VitalServicePrimeConfig;
import ai.vital.prime.service.superadmin.VitalServiceSuperAdminPrime;
import ai.vital.sql.service.SqlVitalServiceInitWrapper;
import ai.vital.sql.service.VitalServiceSqlConfigCreator;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
import ai.vital.sql.service.superadmin.VitalServiceSuperAdminSql;
import ai.vital.superadmin.domain.VitalServiceSuperAdminKey;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.auth.VitalAuthKeyValidation;
import ai.vital.vitalservice.auth.VitalAuthKeyValidation.VitalAuthKeyValidationException;
import ai.vital.vitalservice.config.URIGenerationStrategy;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsConfig;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsImplementation.DatabaseConnectionWrapped;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.factory.VitalServiceConfigValidator;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.factory.VitalServiceFactory.ServiceConfigWrapper;
import ai.vital.vitalservice.factory.VitalServiceFactoryBase;
import ai.vital.vitalservice.factory.VitalServiceInitWrapper;
import ai.vital.vitalservice.impl.IVitalServiceConfigAware;
import ai.vital.vitalservice.impl.ServiceWideEdgesResolver;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.impl.query.GraphObjectSaveAssigner;
import ai.vital.vitalservice.impl.query.PathQueryHelperAssigner;
import ai.vital.vitalservice.impl.query.ToSparqlAssigner;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.superadmin.VitalServiceSuperAdmin;
import ai.vital.vitalservice.superadmin.impl.AbstractVitalServiceSuperAdminImplementation;
import ai.vital.vitalservice.superadmin.impl.SuperAdminSystemSegment;
//import ai.vital.vitalservice.superadmin.impl.AbstractVitalServiceSuperAdminImplementation;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.conf.VitalSignsConfig;
import ai.vital.vitalsigns.conf.VitalSignsConfig.DomainsStrategy;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalAuthKey;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.VitalSession;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasKey;
import ai.vital.vitalsigns.model.properties.Property_hasName;
import ai.vital.vitalsigns.model.properties.Property_hasOrganizationID;


public class VitalServiceSuperAdminFactory extends VitalServiceFactoryBase {

	private VitalServiceSuperAdminFactory(){}
	
	private final static Logger log = LoggerFactory.getLogger(VitalServiceSuperAdminFactory.class); 
	
	
	public static VitalStatus isInitialized(String profileName) throws VitalServiceException {
		return isInitialized(VitalServiceFactory.getProfileConfig(profileName).serviceConfig);
	}
	
	private static interface InitializationHandler {
		
		VitalStatus onSystemSegmentReady(SuperAdminSystemSegment systemSegment, VitalServiceRootKey rootKey) throws VitalServiceException;
		
	}
	
	
	public static VitalStatus isInitialized(VitalServiceConfig endpointConfig) throws VitalServiceException {
		return _isInitialized(endpointConfig, null);
	}
	
	public static VitalStatus addVitalServiceSuperAdminKey(VitalServiceConfig endpointConfig, VitalServiceRootKey rootKey, final VitalServiceSuperAdminKey superAdminKey) throws VitalServiceException {
		
		if(endpointConfig.getEndpointtype() == EndpointType.VITALPRIME) {
			IVitalPrimeClient client = VitalPrimeClientFactory.createClient(((VitalServicePrimeConfig)endpointConfig).getEndpointURL());
			
			return client.addVitalServiceSuperAdminKey(rootKey, superAdminKey);
		}
		
		final String k = (String) rootKey.getRaw(Property_hasKey.class);
		
		return _isInitialized(endpointConfig, new InitializationHandler() {
			
			@Override
			public VitalStatus onSystemSegmentReady(SuperAdminSystemSegment systemSegment, VitalServiceRootKey rootKey)
					throws VitalServiceException {
				
				String key = (String) rootKey.getRaw(Property_hasKey.class);
				
				if(!key.equals(k)) throw new VitalServiceException("Invalid root key, cannot add vital service super admin key");
				
				systemSegment.addVitalServiceSuperAdminKey(rootKey, superAdminKey);
				return VitalStatus.withOK();
			}
		});
	}
	
	public static VitalStatus removeVitalServiceSuperAdminKey(VitalServiceConfig endpointConfig, VitalServiceRootKey rootKey, final VitalServiceSuperAdminKey superAdminKey) throws VitalServiceException {
		
		if(endpointConfig.getEndpointtype() == EndpointType.VITALPRIME) {
			IVitalPrimeClient client = VitalPrimeClientFactory.createClient(((VitalServicePrimeConfig)endpointConfig).getEndpointURL());
			
			return client.removeVitalServiceSuperAdminKey(rootKey, superAdminKey);
		}
		
		final String k = (String) rootKey.getRaw(Property_hasKey.class);
		
		return _isInitialized(endpointConfig, new InitializationHandler() {
			
			@Override
			public VitalStatus onSystemSegmentReady(SuperAdminSystemSegment systemSegment,
					VitalServiceRootKey rootKey) throws VitalServiceException {
				
				String key = (String) rootKey.getRaw(Property_hasKey.class);
				
				if(!key.equals(k)) throw new VitalServiceException("Invalid root key, cannot add vital service super admin key");
				
				return systemSegment.removeVitalServiceSuperAdminKey(rootKey, superAdminKey);
			}
		});
		
	}
	
	public static List<VitalServiceSuperAdminKey> listVitalServiceSuperAdminKeys(VitalServiceConfig endpointConfig, VitalServiceRootKey rootKey) throws VitalServiceException {
		
		if(endpointConfig.getEndpointtype() == EndpointType.VITALPRIME) {
			IVitalPrimeClient client = VitalPrimeClientFactory.createClient(((VitalServicePrimeConfig)endpointConfig).getEndpointURL());
			
			List<VitalServiceSuperAdminKey> keys = new ArrayList<VitalServiceSuperAdminKey>();
			List<VitalAuthKey> input = client.listVitalServiceSuperAdminKeys(rootKey);
			for(VitalAuthKey k : input) {
				keys.add((VitalServiceSuperAdminKey)k);
			}
			return keys;
		}
		
		final String k = (String) rootKey.getRaw(Property_hasKey.class);
		
		final List<VitalServiceSuperAdminKey> keys = new ArrayList<VitalServiceSuperAdminKey>();
		
		_isInitialized(endpointConfig, new InitializationHandler() {
			
			@Override
			public VitalStatus onSystemSegmentReady(SuperAdminSystemSegment systemSegment,
					VitalServiceRootKey rootKey) throws VitalServiceException {
	
				String key = (String) rootKey.getRaw(Property_hasKey.class);
				
				if(!key.equals(k)) throw new VitalServiceException("Invalid root key, cannot list service super admin keys");
				
				keys.addAll(systemSegment.listVitalServiceSuperAdminKeys(rootKey));
				
				return VitalStatus.withOK();
			}
		});
		
		return keys;
		
	}
	
	private static VitalStatus _isInitialized(VitalServiceConfig endpointConfig, InitializationHandler handler) throws VitalServiceException {
	
		EndpointType type = endpointConfig.getEndpointtype();
		
//		LuceneServiceDiskImpl lsdi = null;
//		
//		AllegrographWrapper agw = null;
//		
//
//		VitalSqlImplementation sqlImpl = null;
		
		SystemSegmentOperationsExecutor executor = null;
		
		VitalServiceInitWrapper indexWrapper = null;
		
		VitalServiceInitWrapper wrapper = null;
		
		try {
			
			if(type == EndpointType.VITALPRIME) {
				
				IVitalPrimeClient client = VitalPrimeClientFactory.createClient(((VitalServicePrimeConfig)endpointConfig).getEndpointURL());
				
				return client.isInitialized();
				
			} else if(type == EndpointType.ALLEGROGRAPH) {
				
				VitalServiceAllegrographConfig vsac = (VitalServiceAllegrographConfig) endpointConfig;
				
				wrapper = new AllegrographVitalServiceInitWrapper(vsac); 
				
				/*
				agw = AllegrographWrapper.create(vsac.getServerURL(), vsac.getUsername(), vsac.getPassword(), vsac.getCatalogName(), vsac.getRepositoryName());
				
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
				
				executor = new AllegrographSystemSegmentExecutor(agw);
				*/
				
			} else if(type == EndpointType.INDEXDB) {
				
				VitalServiceIndexedDBConfig idbConfig = (VitalServiceIndexedDBConfig) endpointConfig;
				
				VitalServiceConfig indexCfg = idbConfig.getIndexConfig();
				
				VitalServiceConfig dbConfig = idbConfig.getDbConfig();
				
				EndpointType itype = indexCfg.getEndpointtype();
				
				EndpointType dtype = dbConfig.getEndpointtype();
				
				if( itype == EndpointType.LUCENEDISK ) {
					
					VitalServiceLuceneDiskConfig vsldc = (VitalServiceLuceneDiskConfig)indexCfg;
					
					indexWrapper = new LuceneDiskVitalServiceInitWrapper(vsldc);
					
//					File rootLocation = new File(vsldc.getRootPath());
//					
//					if(!rootLocation.exists()) {
//						return VitalStatus.withError("Lucene index root location does not exist: " + rootLocation.getAbsolutePath());
//					}
//					
//					
//					lsdi = LuceneServiceDiskImpl.create(rootLocation);
//					
//					try {
//						lsdi.open();
//					} catch (LuceneException e) {
//						return VitalStatus.withError("Couldn't initialize lucene impl.: " + e.getLocalizedMessage());
//					}
					
				} else {
					throw new VitalServiceException("Unhandled indexDB index type: " + itype);
				}

				
				if( dtype == EndpointType.ALLEGROGRAPH ) {
					
					VitalServiceAllegrographConfig vsac = (VitalServiceAllegrographConfig) dbConfig;
					
					wrapper = new AllegrographVitalServiceInitWrapper(vsac);
					
//					agw = AllegrographWrapper.create(vsac.getServerURL(), vsac.getUsername(), vsac.getPassword(), vsac.getCatalogName(), vsac.getRepositoryName());
//					
//					try {
//						agw.open();
//					} catch (Exception e) {
//						return VitalStatus.withError("Allegrograph connection failed: " + e.getLocalizedMessage());
//					}
//					
//					try {
//						VitalStatus ping = agw.ping();
//						if( ping.getStatus() != VitalStatus.Status.ok ) {
//							return VitalStatus.withError("Allegrograph ping failed: " + ping.getMessage());
//						}
//					} catch (Exception e) {
//						return VitalStatus.withError("Allegrograph ping failed: " + e.getLocalizedMessage());
//					}
//					
//					executor = new AllegrographSystemSegmentExecutor(agw);
					
				} else if( dtype == EndpointType.SQL ) {

					wrapper = new SqlVitalServiceInitWrapper((VitalServiceSqlConfig) dbConfig);
					
//					sqlImpl = new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig((VitalServiceSqlConfig) dbConfig)));
//					
//					try {
//						sqlImpl.ping();
//					} catch (SQLException e) {
//						return VitalStatus.withError("SQL endpoint ping failed: " + e.getLocalizedMessage());
//					}
//					
//					executor = new VitalSqlSystemSegmentExecutor(sqlImpl);
					 
				} else {
					throw new VitalServiceException("Unhandled indexDB database type: " + dtype);
				}
				
				
			} else if(type == EndpointType.MOCK) {
				
				throw new VitalServiceException("Mock service is an inmemory implementation and cannot be initialized, use open(Admin)Service method instead");
				
			} else if(type == EndpointType.LUCENEDISK) {
				
				VitalServiceLuceneDiskConfig vsldc = (VitalServiceLuceneDiskConfig)endpointConfig;
				
				wrapper = new LuceneDiskVitalServiceInitWrapper(vsldc);
				
//				File rootLocation = new File(vsldc.getRootPath());
//				
//				if(!rootLocation.exists()) {
//					return VitalStatus.withError("Lucene index root location does not exist: " + rootLocation.getAbsolutePath());
//				}
//				
//				lsdi = LuceneServiceDiskImpl.create(rootLocation);
//				
//				try {
//					lsdi.open();
//				} catch (LuceneException e) {
//					return VitalStatus.withError("Couldn't initialize lucene impl.: " + e.getLocalizedMessage());
//				}
//				
//				executor = new LuceneSystemSegmentExecutor(lsdi);
				
			} else if(type == EndpointType.LUCENEMEMORY) {
				
//				throw new VitalServiceException("Lucene memory cannot be initialized, use open(Admin)Service instead");
				return VitalStatus.withOKMessage("LuceneMemory implementation is always initialized");
				
			} else if(type == EndpointType.SQL) {
				
				wrapper = new SqlVitalServiceInitWrapper((VitalServiceSqlConfig) endpointConfig);
				
//				sqlImpl = new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig((VitalServiceSqlConfig) endpointConfig)));
//				
//				try {
//					sqlImpl.ping();
//				} catch (SQLException e) {
//					return VitalStatus.withError("SQL endpoint ping failed: " + e.getLocalizedMessage());
//				}
//				
//				executor = new VitalSqlSystemSegmentExecutor(sqlImpl);
				
			} else {
				throw new RuntimeException("Unhandled init endpoint: " + type);
			}
			
			if(indexWrapper != null) {
				VitalStatus indexStatus = indexWrapper.isInitialized();
				if(indexStatus.getStatus() != VitalStatus.Status.ok) {
					return indexStatus;
				}
			}
			
			VitalStatus innerStatus = wrapper.isInitialized();
			if(innerStatus.getStatus() != VitalStatus.Status.ok) {
				return innerStatus;
			}
			
			executor = wrapper.createExecutor();
			
			
			SuperAdminSystemSegment systemSegment = new SuperAdminSystemSegment(executor);
			
			if(!systemSegment.systemSegmentExists()) {
				return VitalStatus.withError("System segment does not exist");
			}
			
			
			List<VitalServiceRootKey> rootKeys = systemSegment.listVitalServiceRootKeys();
			
			if(rootKeys.size() < 1) {
				return VitalStatus.withError("No root keys found");
			}
			
			
			if(handler != null) {
				return handler.onSystemSegmentReady(systemSegment, rootKeys.get(0));
			}
			
			return VitalStatus.withOKMessage("Service is initialized");
			
			
		} finally {
			
			if(indexWrapper != null) {
				indexWrapper.close();
			}
			
			if(wrapper != null) {
				wrapper.close();
			}
			
//			if(lsdi != null) {
//				lsdi.close();
//			}
//			if(agw != null) {
//				try {
//					agw.close();
//				} catch (Exception e) {}
//			}
//			
//			if(sqlImpl != null) {
//				try {
//					sqlImpl.close();
//				} catch (SQLException e) {}
//			}
			
		}
		
	}
	
	public static VitalServiceRootKey initSuperAdminService(VitalServiceConfig endpointConfig, VitalServiceRootKey optionalRootKey, VitalServiceSuperAdminKey optionalSuperAdminKey) throws VitalServiceException {
	
		if(optionalRootKey == null) {
			
			optionalRootKey = (VitalServiceRootKey) new VitalServiceRootKey().generateURI((VitalApp)null);
			String newKey = RandomStringUtils.randomAlphabetic(4) + "-" + RandomStringUtils.randomAlphabetic(4) + "-" + RandomStringUtils.randomAlphabetic(4);
			try {
				VitalAuthKeyValidation.validateKey(newKey);
			} catch (VitalAuthKeyValidationException e) {
				throw new VitalServiceException("Randomly generated super admin key is invalid: " + e.getLocalizedMessage());
			}
			optionalRootKey.set(Property_hasKey.class, newKey);
			
		} else {
			if(optionalRootKey.getURI() == null) optionalRootKey.generateURI((VitalApp)null);
			String key = (String) optionalRootKey.getRaw(Property_hasKey.class);
			if(key == null) throw new VitalServiceException("super admin key string not set");
			try {
				VitalAuthKeyValidation.validateKey(key);
			} catch (VitalAuthKeyValidationException e) {
				throw new VitalServiceException("super admin key is invalid: " + e.getLocalizedMessage());
			}
		
		}
		
		EndpointType type = endpointConfig.getEndpointtype();

		VitalServiceInitWrapper indexWrapper = null;
		VitalServiceInitWrapper wrapper = null;
		
//		LuceneServiceDiskImpl lsdi = null;
//		
//		AllegrographWrapper agw = null;
//		
//
//		VitalSqlImplementation sqlImpl = null;
		
		try {
			
			SystemSegmentOperationsExecutor executor = null;
			//now based on the type initialize it
			
			if(type == EndpointType.ALLEGROGRAPH) {
				
				VitalServiceAllegrographConfig vsac = (VitalServiceAllegrographConfig) endpointConfig;
				wrapper = new AllegrographVitalServiceInitWrapper(vsac);
				
//				agw = AllegrographWrapper.create(vsac.getServerURL(), vsac.getUsername(), vsac.getPassword(), vsac.getCatalogName(), vsac.getRepositoryName());
//				
//				try {
//					agw.open();
//				} catch (Exception e) {
//					throw new VitalServiceException(e);
//				}
//				
//				executor = new AllegrographSystemSegmentExecutor(agw);
				
			} else if(type == EndpointType.INDEXDB) {
				
				VitalServiceIndexedDBConfig idbConfig = (VitalServiceIndexedDBConfig) endpointConfig;
				
				VitalServiceConfig indexCfg = idbConfig.getIndexConfig();
				
				VitalServiceConfig dbConfig = idbConfig.getDbConfig();
				
				EndpointType itype = indexCfg.getEndpointtype();
				
				EndpointType dtype = dbConfig.getEndpointtype();
				
				if( itype == EndpointType.LUCENEDISK ) {
					
					VitalServiceLuceneDiskConfig vsldc = (VitalServiceLuceneDiskConfig)indexCfg;
					
					indexWrapper = new LuceneDiskVitalServiceInitWrapper(vsldc);
					
//					File rootLocation = new File(vsldc.getRootPath());
//					
//					LuceneServiceDiskImpl.init(rootLocation);
//					
//					lsdi = LuceneServiceDiskImpl.create(rootLocation);
//					
//					try {
//						lsdi.open();
//					} catch (LuceneException e) {
//						throw new VitalServiceException(e);
//					}
					
				} else {
					throw new VitalServiceException("Unhandled indexDB index type: " + itype);
				}

				
				if( dtype == EndpointType.ALLEGROGRAPH ) {
					
					VitalServiceAllegrographConfig vsac = (VitalServiceAllegrographConfig) dbConfig;

					wrapper = new AllegrographVitalServiceInitWrapper(vsac);
					
//					agw = AllegrographWrapper.create(vsac.getServerURL(), vsac.getUsername(), vsac.getPassword(), vsac.getCatalogName(), vsac.getRepositoryName());
//					
//					try {
//						agw.open();
//					} catch (Exception e) {
//						throw new VitalServiceException(e);
//					}
//					
//					executor = new AllegrographSystemSegmentExecutor(agw);
					
				} else if( dtype == EndpointType.SQL ) {

					wrapper = new SqlVitalServiceInitWrapper((VitalServiceSqlConfig) dbConfig);
//					sqlImpl = new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig((VitalServiceSqlConfig) dbConfig)));
//					executor = new VitalSqlSystemSegmentExecutor(sqlImpl);
					 
				} else {
					throw new VitalServiceException("Unhandled indexDB database type: " + dtype);
				}
				
				
			} else if(type == EndpointType.LUCENEDISK) {
				
				VitalServiceLuceneDiskConfig vsldc = (VitalServiceLuceneDiskConfig)endpointConfig;
				
				wrapper = new LuceneDiskVitalServiceInitWrapper(vsldc);
				
//				File rootLocation = new File(vsldc.getRootPath());
//				
//				LuceneServiceDiskImpl.init(rootLocation);
//				
//				lsdi = LuceneServiceDiskImpl.create(rootLocation);
//				
//				try {
//					lsdi.open();
//				} catch (LuceneException e) {
//					throw new VitalServiceException(e);
//				}
//				
//				executor = new LuceneSystemSegmentExecutor(lsdi);
				
			} else if(type == EndpointType.LUCENEMEMORY) {
				
				throw new VitalServiceException("Lucene memory cannot be initialized, use open(Admin)Service instead");
				
			} else if(type == EndpointType.MOCK) {
				
				throw new VitalServiceException("Mock service cannot be initialized, use open(Admin)Service instead");
				
			} else if(type == EndpointType.SQL) {
				
				wrapper = new SqlVitalServiceInitWrapper((VitalServiceSqlConfig) endpointConfig);
//				sqlImpl = new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig((VitalServiceSqlConfig) endpointConfig)));
//				executor = new VitalSqlSystemSegmentExecutor(sqlImpl);

			} else if(type == EndpointType.VITALPRIME) {
				
				VitalServicePrimeConfig primeCfg = (VitalServicePrimeConfig) endpointConfig;
				
				IVitalPrimeClient client = VitalPrimeClientFactory.createClient(primeCfg.getEndpointURL());
				
				VitalStatus status = client.initialize(optionalRootKey, optionalSuperAdminKey);
				
				if(status.getStatus() != VitalStatus.Status.ok) {
					
					throw new VitalServiceException("Prime initialization failed: " + status.getMessage());
					
				}
				
				return optionalRootKey;
				
			} else {
				throw new RuntimeException("Unhandled init endpoint: " + type);
			}
			
			
			if(indexWrapper != null) {
				indexWrapper.initialize();
			}
			
			wrapper.initialize();
			
			
			
			executor = wrapper.createExecutor();
			
			SuperAdminSystemSegment systemSegment = new SuperAdminSystemSegment(executor);
			
			if(systemSegment.systemSegmentExists()) {
				log.warn("System segment already exists");
			} else {
				log.info("Creating system segment");
				systemSegment.createSystemSegment();
			}
			
			List<VitalServiceRootKey> rootKeys = systemSegment.listVitalServiceRootKeys();
			
			if(rootKeys.size() > 0) {
				
				optionalRootKey = rootKeys.get(0); 
				
				log.warn("at least 1 root service key already exists (" + rootKeys.size() + "), skipping");
				
			} else {
				
				optionalRootKey = systemSegment.addVitalServiceRootKey(optionalRootKey);
				
			}
			
			if(optionalSuperAdminKey != null) {
				
				systemSegment.addVitalServiceSuperAdminKey(optionalRootKey, optionalSuperAdminKey);
				
			}
			
			return optionalRootKey;
			
		} finally {
			
			if(indexWrapper != null) {
				indexWrapper.close();
			}
			
			if(wrapper != null) {
				wrapper.close();
			}
			
//			if(lsdi != null) {
//				lsdi.close();
//			}
//			if(agw != null) {
//				try {
//					agw.close();
//				} catch (Exception e) {}
//			}
//			
//			if(sqlImpl != null) {
//				try {
//					sqlImpl.close();
//				} catch (SQLException e) {}
//			}
			
		}
	}
	
	/**
	 * initializes the service - system segment root auth key and organization node
	 * @param profileName
	 * @param optionalRootKey
	 * @return
	 */
	public static VitalServiceRootKey initSuperAdminService(String profileName, VitalServiceRootKey optionalRootKey, VitalServiceSuperAdminKey optionalSuperAdminKey) throws VitalServiceException {
		
		ServiceConfigWrapper profileConfig = VitalServiceFactory.getProfileConfig(profileName);
		
		return initSuperAdminService(profileConfig.serviceConfig, optionalRootKey, optionalSuperAdminKey);
		
	}
	
	/**
	 * Opens a new vitalservice superadmin instance for given key and "default" profile
	 * @param key
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceSuperAdmin openSuperAdminService(VitalServiceSuperAdminKey key) throws VitalServiceException {
		return openSuperAdminService(key, DEFAULT_PROFILE);
	}
	
	/**
	 * Opens a new vitalservice super admin instance for given key and config object
	 * @param key
	 * @param config
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceSuperAdmin openSuperAdminService(VitalServiceSuperAdminKey key, VitalServiceConfig config) throws VitalServiceException {
		return openSuperAdminService(key, config, null);
	}
	
	/**
	 * Opens a new vitalservice instance for given key, config object and name
	 * @param key
	 * @param endpointConfig
	 * @param serviceName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceSuperAdmin openSuperAdminService(VitalServiceSuperAdminKey key, VitalServiceConfig endpointConfig, String serviceName) throws VitalServiceException {

		validateKey(key);

		if(serviceName == null || serviceName.isEmpty()) {
			serviceName = "vital-service-superadmin-" + serviceCounter.incrementAndGet();
		} else {
			checkServiceName(serviceName);
		}
		
		VitalServiceConfigValidator.checkIfServiceCanBeOpened(serviceName, endpointConfig);
		
		if(allOpenServices.size() >= maxActiveServices) throw new VitalServiceException("Max open services count limit reached: " + maxActiveServices);
		
		
		EndpointType et = endpointConfig.getEndpointtype();
		
		
		VitalServiceSuperAdmin vitalServiceSuperAdmin = null;
		

		if(et == EndpointType.VITALPRIME) {
			
			if( endpointConfig.getApp() != null ) {
				log.warn("service config app param ignored");
			}
			
			VitalServicePrimeConfig primeCfg = (VitalServicePrimeConfig) endpointConfig;
			//accept it

			IVitalPrimeClient client = VitalPrimeClientFactory.createClient(primeCfg.getEndpointURL());
			
			ResultList rl = null;
			try {
				rl = client.authenticate(key);
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}
			
			if(rl.getStatus().getStatus() != VitalStatus.Status.ok) {
				throw new VitalServiceException("Authentication error: " + rl.getStatus().getMessage());
			}

			VitalSession _session = null;
			
			for(GraphObject g : rl) {
				
				if(g instanceof VitalSession) {
					_session = (VitalSession) g;
				}
				
			}
			
			if(_session == null) throw new VitalServiceException("No session returned by prime");
			
			vitalServiceSuperAdmin = VitalServiceSuperAdminPrime.create(client);
			
		} else {
			
			try {
				vitalServiceSuperAdmin = createSuperAdminServiceInstance(endpointConfig);
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}
			
		}
		
		//where to read the organization/app from 
//		VitalSigns.get().setCurrentApp(vitalService.getApp());
//		VitalSigns.get().setVitalService(vitalService);
//		VitalSigns.get().setVitalServiceAdmin(null);
		VitalSigns.get().getEdgesResolvers().put(GraphContext.ServiceWide, new ServiceWideEdgesResolver());

		

		if( vitalServiceSuperAdmin.getEndpointType() == EndpointType.VITALPRIME ) {
			
			VitalSignsConfig vscfg = VitalSigns.get().getConfig();

			if(vscfg.domainsStrategy == DomainsStrategy.dynamic && !vscfg.loadDeployedJars && vscfg.autoLoad) {
				
				//TODO restore sync
				throw new RuntimeException("sync feature disabled");
				
//				try {
//					VitalSigns.get().sync();
//				} catch (Exception e) {
//					throw new VitalServiceException(e);
//				}
				
			} else {
			
				log.info("Initial sync skipped, triggered only when domainsStrategy=dynamic, loadDeployedJars=false and autoLoad=true");
			
			}
								
			
		} 
		
		if(vitalServiceSuperAdmin instanceof IVitalServiceConfigAware) {
			IVitalServiceConfigAware impl = (IVitalServiceConfigAware) vitalServiceSuperAdmin;
			impl.setName(serviceName);
			//TODO make it serializable in order to clone it
//			impl.setConfig((VitalServiceConfig) VitalJavaSerializationUtils.clone(endpointConfig));
			impl.setConfig(endpointConfig);
			impl.setAuthKey(key);
			allOpenServices.add(impl);
 		} else {
 			throw new RuntimeException("All vitalservicesuperadmin implementations must also implement " + IVitalServiceConfigAware.class.getCanonicalName());
 		}
		return vitalServiceSuperAdmin;

	}
	
	/**
	 * Opens a new vitalservice instance for given key and profile
	 * Service name will be assigned automatically
	 * @param key
	 * @param profileName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceSuperAdmin openSuperAdminService(VitalServiceSuperAdminKey key, String profileName) throws VitalServiceException {
		return openSuperAdminService(key, profileName, null);
	}
	
	static AtomicInteger serviceCounter = new AtomicInteger();
	
	private static String validateKey(VitalAuthKey key) throws VitalServiceException {
		
		if(key == null) throw new NullPointerException("Null key object");
		

		Object keyVal = key.get(Property_hasKey.class);
		if(keyVal == null) throw new NullPointerException("Null key string property");
		String keyS = keyVal.toString();
		
		try {
			VitalAuthKeyValidation.validateKey(keyS);
		} catch(VitalAuthKeyValidationException ex) {
			throw new VitalServiceException("The input key string is invalid, reason: " + ex.getLocalizedMessage());
		}
		
		return keyS;
		
	}
		
	/**
	 * Opens a new vitalservice superadmin instance for given key, profile and label
	 * @param key
	 * @param profileName
	 * @param serviceName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceSuperAdmin openSuperAdminService(VitalServiceSuperAdminKey key, String profileName, String serviceName) throws VitalServiceException{

		if(profileName == null || profileName.isEmpty()) throw new NullPointerException("profileName must not be null nor empty");
		
		validateKey(key);
		
		checkServiceName(serviceName);
		
		ServiceConfigWrapper endpointConfigWrapped = VitalServiceFactory.getProfileConfig(profileName);
		
		VitalServiceConfig endpointConfig = endpointConfigWrapped.serviceConfig;
		
		Config cfg = endpointConfigWrapped.cfg; 
		
		VitalServiceSuperAdmin vitalServiceSuperAdmin = openSuperAdminService(key, endpointConfig, serviceName);
		
		
		List<DatabaseConnectionWrapped> wrappedConfig = DatabaseConnectionsConfig.initFromServiceConfig(cfg);

		if( vitalServiceSuperAdmin.getEndpointType() == EndpointType.VITALPRIME ) {
			
			if(wrappedConfig.size() > 0) log.warn("Database connections configuration skipped in service with prime endpoint");

		} else {
		
			for(DatabaseConnectionWrapped dcw : wrappedConfig) {
				
				if(dcw.organizationID == null) {
					log.error("organizationID in db connection: " + dcw.connection.get(Property_hasName.class).toString() + " not set. It is required in vitalservicesuperadmin instance");
				}
				if(dcw.appID == null) {
					log.error("appID in db connection: " + dcw.connection.get(Property_hasName.class).toString() + " not set. It is required in vitalservicesuperadmin instance");
					continue;
				}
				
				dcw.connection.set(Property_hasOrganizationID.class, dcw.organizationID);
				dcw.connection.set(Property_hasAppID.class, dcw.appID);
				
				VitalOrganization o = VitalOrganization.withId(dcw.organizationID);
				VitalApp a = VitalApp.withId(dcw.appID);
				VitalStatus addStatus = null;;
				try {
					addStatus = vitalServiceSuperAdmin.addDatabaseConnection(o, a, dcw.connection);
				} catch (VitalServiceUnimplementedException e) {
					addStatus = VitalStatus.withError(e.getLocalizedMessage());
				}
//				
				if(addStatus.getStatus() != VitalStatus.Status.ok) {
					log.error("Couldn't add initial db connection: " + dcw.connection.get(Property_hasName.class).toString() + " ignored, reason: " + addStatus.getMessage());
				}
				
			}
			
		} 
		
		return vitalServiceSuperAdmin;
	}
	
	
	
	static {
		
		endpointType2Creator.put(EndpointType.LUCENEDISK, new VitalServiceLuceneDiskConfigCreator());
		endpointType2Creator.put(EndpointType.LUCENEMEMORY, new VitalServiceLuceneMemoryConfigCreator());
		endpointType2Creator.put(EndpointType.MOCK, new VitalServiceMockConfigCreator());
		endpointType2Creator.put(EndpointType.INDEXDB, new VitalServiceIndexedDBConfigCreator());
		endpointType2Creator.put(EndpointType.ALLEGROGRAPH, new VitalServiceAllegrographConfigCreator());
		endpointType2Creator.put(EndpointType.VITALPRIME, new VitalServicePrimeConfigCreator());
		endpointType2Creator.put(EndpointType.SQL, new VitalServiceSqlConfigCreator());
		
		
		//groovy dynamic implementations
		PathQueryHelperAssigner.assingHelper();
		ToSparqlAssigner.assignToSparql();
		GraphObjectSaveAssigner.assingSave();
		
//		VitalSigns.get().edgesResolvers.put(GraphContext.ServiceWide)
		
	}
	

	/**
	 * Creates a new instance of vital service, only some service types may be created using this method.
	 * @return
	 * @throws VitalServiceException 
	 * @throws VitalServiceUnimplementedException 
	 */
//	public static VitalServiceAdmin createVitalServiceAdmin(EndpointConfig config) {
//		
//		EndpointConfigCreator creator = endpointType2Creator.get(config.getEndpointtype());
//
//		if(creator == null) throw new RuntimeException("No creator for endpoint type found: " + config.getEndpointtype());
//		
//
//		return createAdminServiceInstance(config, flumeService, new Lock());
//		
//	}
	
	public static boolean close(VitalServiceSuperAdmin service) throws VitalServiceUnimplementedException, VitalServiceException {
		
		boolean found = false;
		
		synchronized(allOpenServices) {
			
			for(IVitalServiceConfigAware vs : allOpenServices) {
				
				if(vs == service) {
					found = true;
				}
				
			}
			
			allOpenServices.remove(service);
			
		}
		
		
		if(!found) return false;
		
		//close it again
		service.close();
		
		return true;
		
	}
	
	public static List<VitalServiceSuperAdmin> listOpenSuperAdminServices(VitalServiceSuperAdminKey key) {
		
		if(key == null) return listOpenSuperAdminServices();
		List<VitalServiceSuperAdmin> filtered = new ArrayList<VitalServiceSuperAdmin>();
		synchronized(allOpenServices) {
			for(IVitalServiceConfigAware ca : allOpenServices) {
				if(!(ca instanceof VitalServiceSuperAdmin)) continue;
				VitalServiceSuperAdmin service = (VitalServiceSuperAdmin) ca;
				VitalServiceSuperAdminKey k = (VitalServiceSuperAdminKey) ca.getAuthKey();
				if(k != null && k.getRaw(Property_hasKey.class).equals(key.getRaw(Property_hasKey.class))) {
					filtered.add(service);
				}
			}
		}
		return filtered;
		
	}
	public static List<VitalServiceSuperAdmin> listOpenSuperAdminServices() {
		List<VitalServiceSuperAdmin> r = new ArrayList<VitalServiceSuperAdmin>();
		synchronized(allOpenServices) {
			for(IVitalServiceConfigAware ca : allOpenServices) {
				if(ca instanceof VitalServiceSuperAdmin) {
					r.add((VitalServiceSuperAdmin) ca);
				}
			}
		}
		return r;
	}
	
	private static VitalServiceSuperAdmin createSuperAdminServiceInstance(VitalServiceConfig endpointConfig) throws Exception {
		
		EndpointType type = endpointConfig.getEndpointtype(); 
		
		VitalServiceSuperAdmin vitalServiceSuperAdmin = null;
		
		//TODO add flume service!
		if(type == EndpointType.LUCENEDISK) {
			
			vitalServiceSuperAdmin = VitalServiceSuperAdminLuceneDisk.create((VitalServiceLuceneDiskConfig) endpointConfig);
			
		} else if(type == EndpointType.LUCENEMEMORY) {
		
			vitalServiceSuperAdmin = VitalServiceSuperAdminLuceneMemory.create((VitalServiceLuceneMemoryConfig) endpointConfig);
		
		} else if(type == EndpointType.MOCK) {
		
			vitalServiceSuperAdmin = VitalServiceSuperAdminMock.create((VitalServiceMockConfig) endpointConfig);
		
		} else if(type == EndpointType.INDEXDB) {
		
			vitalServiceSuperAdmin = VitalServiceSuperAdminIndexedDB.create((VitalServiceIndexedDBConfig) endpointConfig);
			
		} else if(type == EndpointType.VITALPRIME) {
					
			throw new RuntimeException("Prime service instance should be created with a different method");
//			vitalService = VitalServicePrime.create((VitalServicePrimeConfig) endpointConfig, flumeService, lock, organization, app);
			
		} else if(type == EndpointType.ALLEGROGRAPH) {
		
			vitalServiceSuperAdmin = VitalServiceSuperAdminAllegrograph.create((VitalServiceAllegrographConfig) endpointConfig);
		
		} else if(type == EndpointType.SQL) {
		
			vitalServiceSuperAdmin = new VitalServiceSuperAdminSql((VitalServiceSqlConfig) endpointConfig);
			
		} else throw new Exception("Unhandled endpoint type: " + type);
		
		URIGenerationStrategy uriGenerationStrategy = endpointConfig.getUriGenerationStrategy();
		if(uriGenerationStrategy == null) uriGenerationStrategy = URIGenerationStrategy.local;
		((AbstractVitalServiceSuperAdminImplementation)vitalServiceSuperAdmin).setUriGenerationStrategy(uriGenerationStrategy);
		
		return vitalServiceSuperAdmin;
		
	}
	
	
}
