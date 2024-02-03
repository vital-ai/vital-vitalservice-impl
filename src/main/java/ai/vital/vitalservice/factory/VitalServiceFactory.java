package ai.vital.vitalservice.factory;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.allegrograph.service.AllegrographVitalServiceInitWrapper;
import ai.vital.allegrograph.service.VitalServiceAllegrograph;
import ai.vital.allegrograph.service.VitalServiceAllegrographConfigCreator;
import ai.vital.allegrograph.service.admin.VitalServiceAdminAllegrograph;
import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig;
import ai.vital.indexeddb.service.VitalServiceIndexedDB;
import ai.vital.indexeddb.service.VitalServiceIndexedDBConfigCreator;
import ai.vital.indexeddb.service.admin.VitalServiceAdminIndexedDB;
import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig;
import ai.vital.lucene.disk.service.LuceneDiskVitalServiceInitWrapper;
import ai.vital.lucene.disk.service.VitalServiceLuceneDisk;
import ai.vital.lucene.disk.service.VitalServiceLuceneDiskConfigCreator;
import ai.vital.lucene.disk.service.admin.VitalServiceAdminLuceneDisk;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.lucene.memory.service.VitalServiceLuceneMemory;
import ai.vital.lucene.memory.service.VitalServiceLuceneMemoryConfigCreator;
import ai.vital.lucene.memory.service.admin.VitalServiceAdminLuceneMemory;
import ai.vital.lucene.memory.service.config.VitalServiceLuceneMemoryConfig;
import ai.vital.mock.service.VitalServiceMock;
import ai.vital.mock.service.VitalServiceMockConfigCreator;
import ai.vital.mock.service.admin.VitalServiceAdminMock;
import ai.vital.mock.service.config.VitalServiceMockConfig;
import ai.vital.prime.client.IVitalPrimeClient;
import ai.vital.prime.client.VitalPrimeClientFactory;
import ai.vital.prime.service.VitalServicePrime;
import ai.vital.prime.service.VitalServicePrimeConfigCreator;
import ai.vital.prime.service.admin.VitalServiceAdminPrime;
import ai.vital.prime.service.config.VitalServicePrimeConfig;
import ai.vital.sql.service.SqlVitalServiceInitWrapper;
import ai.vital.sql.service.VitalServiceSql;
import ai.vital.sql.service.VitalServiceSqlConfigCreator;
import ai.vital.sql.service.admin.VitalServiceAdminSql;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.auth.VitalAuthKeyValidation;
import ai.vital.vitalservice.auth.VitalAuthKeyValidation.VitalAuthKeyValidationException;
import ai.vital.vitalservice.config.URIGenerationStrategy;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsConfig;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsImplementation;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsImplementation.DatabaseConnectionWrapped;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.AbstractVitalServiceAdminImplementation;
import ai.vital.vitalservice.impl.AbstractVitalServiceImplementation;
import ai.vital.vitalservice.impl.IVitalServiceConfigAware;
import ai.vital.vitalservice.impl.ServiceWideEdgesResolver;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.impl.query.GraphObjectSaveAssigner;
import ai.vital.vitalservice.impl.query.PathQueryHelperAssigner;
import ai.vital.vitalservice.impl.query.ToSparqlAssigner;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.conf.VitalSignsConfig;
import ai.vital.vitalsigns.conf.VitalSignsConfig.DomainsStrategy;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalAuthKey;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.VitalSession;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasKey;
import ai.vital.vitalsigns.model.properties.Property_hasName;
import ai.vital.vitalsigns.model.properties.Property_hasOrganizationID;
import ai.vital.vitalsigns.utils.StringUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class VitalServiceFactory extends VitalServiceFactoryBase {

	private VitalServiceFactory(){}
	
	
	private final static Logger log = LoggerFactory.getLogger(VitalServiceFactory.class); 
	
	public static VitalStatus isInitialized(String profileName) throws VitalServiceException {
		return isInitialized(getProfileConfig(profileName).serviceConfig);
	}
	
	public static VitalStatus isInitialized(VitalServiceConfig endpointConfig) throws VitalServiceException {
		return _isInitialized(endpointConfig, null);
	}
	
	private static interface InitializationHandler {
	
		VitalStatus onSystemSegmentReady(VitalOrganization organization, SystemSegment systemSegment, VitalServiceRootKey rootKey) throws VitalServiceException;
		
	}
	
	
	public static VitalStatus destroy(VitalServiceConfig endpointConfig, VitalServiceRootKey inputRootKey) throws VitalServiceException {
		
		final String k = (String) inputRootKey.getRaw(Property_hasKey.class);
		try {
			VitalAuthKeyValidation.validateKey(k);
		} catch (VitalAuthKeyValidationException e) {
			throw new VitalServiceException(e.getLocalizedMessage());
		}
		
		
		VitalStatus initStatus = _isInitialized(endpointConfig, new InitializationHandler() {
			
			@Override
			public VitalStatus onSystemSegmentReady(VitalOrganization organization,
					SystemSegment systemSegment, VitalServiceRootKey rootKey) throws VitalServiceException {
				
				String key = (String) rootKey.getRaw(Property_hasKey.class);
				
				if(!key.equals(k)) throw new VitalServiceException("Invalid root key, cannot delete");
				
				return VitalStatus.withOK();
			}
			
		});
		
		if(initStatus.getStatus() != VitalStatus.Status.ok) return initStatus;
		
		VitalServiceFactoryDestroy.destroy(endpointConfig, inputRootKey);
		
		return VitalStatus.withOKMessage("Service destroyed");
		
	}
	
	
	public static VitalStatus addVitalServiceAdminKey(VitalServiceConfig endpointConfig, VitalServiceRootKey rootKey, final VitalServiceAdminKey adminKey) throws VitalServiceException {
		
		if(endpointConfig.getEndpointtype() == EndpointType.VITALPRIME) {
			
			IVitalPrimeClient client = VitalPrimeClientFactory.createClient(((VitalServicePrimeConfig)endpointConfig).getEndpointURL());
			
			return client.addVitalServiceAdminKey(VitalSigns.get().getOrganization(), rootKey, adminKey);
		}

		final String k = (String) rootKey.getRaw(Property_hasKey.class); 
		
		return _isInitialized(endpointConfig, new InitializationHandler() {
			
			@Override
			public VitalStatus onSystemSegmentReady(VitalOrganization organization, SystemSegment systemSegment, VitalServiceRootKey rootKey) throws VitalServiceException {
				
				String key = (String) rootKey.getRaw(Property_hasKey.class);
				
				if(!key.equals(k)) throw new VitalServiceException("Invalid root key, cannot add service admin key");
				
				return systemSegment.addVitalServiceAdminKey(organization, adminKey);
			}
		});
	}
	
	public static VitalStatus removeVitalServiceAdminKey(VitalServiceConfig endpointConfig, VitalServiceRootKey rootKey, final VitalServiceAdminKey adminKey) throws VitalServiceException {
		
		if(endpointConfig.getEndpointtype() == EndpointType.VITALPRIME) {
			
			IVitalPrimeClient client = VitalPrimeClientFactory.createClient(((VitalServicePrimeConfig)endpointConfig).getEndpointURL());
			
			return client.removeVitalServiceAdminKey(VitalSigns.get().getOrganization(), rootKey, adminKey);
		}
		
		final String k = (String) rootKey.getRaw(Property_hasKey.class);
		
		return _isInitialized(endpointConfig, new InitializationHandler() {
			
			@Override
			public VitalStatus onSystemSegmentReady(VitalOrganization organization,
					SystemSegment systemSegment, VitalServiceRootKey rootKey) throws VitalServiceException {
				
				String key = (String) rootKey.getRaw(Property_hasKey.class);
				
				if(!key.equals(k)) throw new VitalServiceException("Invalid root key, cannot remove service admin key");
				
				return systemSegment.removeVitalServiceAdminKey(organization, adminKey);
			}
		});
		
	}
	
	public static List<VitalServiceAdminKey> listVitalServiceAdminKeys(VitalServiceConfig endpointConfig, VitalServiceRootKey rootKey) throws VitalServiceException {
		
		if(endpointConfig.getEndpointtype() == EndpointType.VITALPRIME) {
			
			IVitalPrimeClient client = VitalPrimeClientFactory.createClient(((VitalServicePrimeConfig)endpointConfig).getEndpointURL());
			
			return client.listVitalServiceAdminKeys(VitalSigns.get().getOrganization(), rootKey);
		}
		
		final String k = (String) rootKey.getRaw(Property_hasKey.class);
		
		final List<VitalServiceAdminKey> keys = new ArrayList<VitalServiceAdminKey>();
		
		VitalStatus status = _isInitialized(endpointConfig, new InitializationHandler() {
			
			@Override
			public VitalStatus onSystemSegmentReady(VitalOrganization organization,
					SystemSegment systemSegment, VitalServiceRootKey rootKey) throws VitalServiceException {
				
				String key = (String) rootKey.getRaw(Property_hasKey.class);
				
				if(!key.equals(k)) throw new VitalServiceException("Invalid root key, cannot list admin keys");
				
				keys.addAll(systemSegment.listVitalServiceAdminKeys(organization));
				
				return VitalStatus.withOK();
			}
		});
		
		if(status.getStatus() != VitalStatus.Status.ok) throw new VitalServiceException(status.getMessage());
		
		return keys;
	}
	
	private static VitalStatus _isInitialized(VitalServiceConfig endpointConfig, InitializationHandler handler) throws VitalServiceException {
	
		EndpointType type = endpointConfig.getEndpointtype();
		
//		LuceneServiceDiskImpl lsdi = null;
//		
//		AllegrographWrapper agw = null;
//		
//		DynamoDBServiceImpl dyndbImpl = null;
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
			
			SystemSegment systemSegment = new SystemSegment(executor);
			
			if(!systemSegment.systemSegmentExists()) {
				return VitalStatus.withError("System segment does not exist");
			}
			
			List<VitalServiceRootKey> rootKeys = systemSegment.listVitalServiceRootKeys();
			
			if(rootKeys.size() < 1) {
				return VitalStatus.withError("No root keys found");
			}
			
			VitalOrganization organization = VitalSigns.get().getOrganization();
			
			String orgID = (String) organization.getRaw(Property_hasOrganizationID.class);
			
			VitalOrganization existingOrg = systemSegment.getOrganization(orgID);
			
			if(existingOrg == null) {
				return VitalStatus.withError("organization node does not exist: " + orgID);
			}
			
			organization = existingOrg;

			if(handler != null) {
				return handler.onSystemSegmentReady(organization, systemSegment, rootKeys.get(0));
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
	
	/**
	 * 
	 * @param endpointConfig
	 * @param rootKey
	 * @param optionalAdminKey
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceRootKey initService(VitalServiceConfig endpointConfig, VitalServiceRootKey rootKey, VitalServiceAdminKey optionalAdminKey) throws VitalServiceException {
	
		if(rootKey == null) {
			
			rootKey = (VitalServiceRootKey) new VitalServiceRootKey().generateURI((VitalApp)null);
			String newKey = RandomStringUtils.randomAlphabetic(4) + "-" + RandomStringUtils.randomAlphabetic(4) + "-" + RandomStringUtils.randomAlphabetic(4);
			try {
				VitalAuthKeyValidation.validateKey(newKey);
			} catch (VitalAuthKeyValidationException e) {
				throw new VitalServiceException("Randomly generated root key is invalid: " + e.getLocalizedMessage());
			}
			rootKey.set(Property_hasKey.class, newKey);
			
		} else {
			if(rootKey.getURI() == null) rootKey.generateURI((VitalApp)null);
			String key = (String) rootKey.getRaw(Property_hasKey.class);
			if(key == null) throw new VitalServiceException("root key string not set");
			try {
				VitalAuthKeyValidation.validateKey(key);
			} catch (VitalAuthKeyValidationException e) {
				throw new VitalServiceException("Root key is invalid: " + e.getLocalizedMessage());
			}
		
		}
		
		if(optionalAdminKey != null) {
			if(optionalAdminKey.getURI() == null) optionalAdminKey.generateURI((VitalApp)null);
			String k = (String) optionalAdminKey.getRaw(Property_hasKey.class);
			try {
				VitalAuthKeyValidation.validateKey(k);
			} catch (VitalAuthKeyValidationException e) {
				throw new VitalServiceException("Optional admin key is invalid: " + e.getLocalizedMessage());
			}
		}
	
		
		EndpointType type = endpointConfig.getEndpointtype();
		
		VitalServiceInitWrapper indexWrapper = null;
		VitalServiceInitWrapper wrapper = null;

//		LuceneServiceDiskImpl lsdi = null;
//		
//		AllegrographWrapper agw = null;
//		
//		DynamoDBServiceImpl dyndbImpl = null;
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
				
				VitalStatus status = client.initialize(rootKey, optionalAdminKey);
				
				if(status.getStatus() != VitalStatus.Status.ok) {
					
					throw new VitalServiceException("Prime initialization failed: " + status.getMessage());
					
				}
				
				return rootKey;
				
			} else {
				throw new RuntimeException("Unhandled init endpoint: " + type);
			}
			
			
			if(indexWrapper != null) {
				indexWrapper.initialize();
			}
			
			wrapper.initialize();
			
			
			
			executor = wrapper.createExecutor();
			
			SystemSegment systemSegment = new SystemSegment(executor);
			
			if(systemSegment.systemSegmentExists()) {
				log.warn("System segment already exists");
			} else {
				log.info("Creating system segment");
				systemSegment.createSystemSegment();
			}
			
			List<VitalServiceRootKey> rootKeys = systemSegment.listVitalServiceRootKeys();
			
			if(rootKeys.size() > 0) {
			
				log.warn("at least 1 root service key already exists (" + rootKeys.size() + "), skipping");
				
				rootKey = rootKeys.get(0);
				
				
			} else {
				
				rootKey = systemSegment.addVitalServiceRootKey(rootKey);
				
			}
			
			VitalOrganization organization = VitalSigns.get().getOrganization();
			
			String orgID = (String) organization.getRaw(Property_hasOrganizationID.class);
			
			VitalOrganization existingOrg = systemSegment.getOrganization(orgID);
			
			
			
			if(existingOrg != null) {
				
				log.warn("Organization node already exists, URI: {}, id: {}", existingOrg.getURI(), orgID);
				
				organization = existingOrg;
				
			} else {
				
				log.info("Adding organization node ...");
				if( organization.getURI() == null ) organization.generateURI();
				
				organization = systemSegment.addOrganization(rootKey, organization);
				
				log.info("Organization node added, URI: {}, id: {}", organization.getURI(), orgID);
				
			}
			
			if(optionalAdminKey != null) {
				
				systemSegment.addVitalServiceAdminKey(organization, optionalAdminKey);
				
			}
			
			
			return rootKey;
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
	 * @param optionalAdminKey
	 * @return
	 */
	public static VitalServiceRootKey initService(String profileName, VitalServiceRootKey optionalRootKey, VitalServiceAdminKey optionalAdminKey) throws VitalServiceException {
		
		ServiceConfigWrapper profileConfig = getProfileConfig(profileName);
		
		return initService(profileConfig.serviceConfig, optionalRootKey, optionalAdminKey);
		
	}
	
	/**
	 * Opens a new vitalservice instance for given key and "default" profile
	 * @param key
=	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalService openService(VitalServiceKey key) throws VitalServiceException {
		return openService(key, DEFAULT_PROFILE);
	}
	
	/**
	 * Opens a new vitalservice instance for given key and config object
	 * @param key
	 * @param config
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalService openService(VitalServiceKey key, VitalServiceConfig config) throws VitalServiceException {
		return openService(key, config, null);
	}
	
	/**
	 * Opens a new vitalservice instance for given key, config object and name
	 * @param key
	 * @param endpointConfig
	 * @param serviceName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalService openService(VitalServiceKey key, VitalServiceConfig endpointConfig, String serviceName) throws VitalServiceException {

		validateKey(key);

		if(serviceName == null || serviceName.isEmpty()) {
			serviceName = "vital-service-" + serviceCounter.incrementAndGet();
		} else {
			checkServiceName(serviceName);
		}
		
		
		VitalServiceConfigValidator.checkIfServiceCanBeOpened(serviceName, endpointConfig);
		
		if(allOpenServices.size() >= maxActiveServices) throw new VitalServiceException("Max open services count limit reached: " + maxActiveServices);
		
		
		EndpointType et = endpointConfig.getEndpointtype();
		
		
		VitalService vitalService = null;
		

		VitalOrganization organization = VitalSigns.get().getOrganization();
		
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
			VitalOrganization _organization = null;
			VitalApp _app = null;
			
			for(GraphObject g : rl) {
				
				if(g instanceof VitalSession) {
					_session = (VitalSession) g;
				} else if(g instanceof VitalOrganization) {
					_organization = (VitalOrganization)g;
				} else if(g instanceof VitalApp) {
					_app = (VitalApp) g;
				}
				
			}
			
			if(_session == null) throw new VitalServiceException("No session returned by prime");
			if(_organization == null) throw new VitalServiceException("No organization returned by prime");
			if(_app == null) throw new VitalServiceException("No app returned by prime");
			
			String oid = (String) _organization.getRaw(Property_hasOrganizationID.class);
			if(!organization.getRaw(Property_hasOrganizationID.class).equals( oid ) ) {
				log.warn("Organization ID does not match the value returned by prime: " + organization.getRaw(Property_hasOrganizationID.class) + ", remote: " + oid);
//				throw new VitalServiceException("Organization ID does not match the value returned by prime: " + organization.getRaw(Property_hasOrganizationID.class) + ", remote: " + oid);
			}
			
			vitalService = VitalServicePrime.create(client, _organization, _app);
			
		} else {
			
			VitalApp app = endpointConfig.getApp();
			
			if(app == null) throw new VitalServiceException("App must be provided for local endpoints");
			
			if(app.getRaw(Property_hasAppID.class) == null) throw new RuntimeException("Service config must have app ID");
			
			try {
				vitalService = createServiceInstance(endpointConfig, organization, app);
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}
			
		}
		
		//where to read the organization/app from 
//		VitalSigns.get().setCurrentApp(vitalService.getApp());
//		VitalSigns.get().setVitalService(vitalService);
//		VitalSigns.get().setVitalServiceAdmin(null);
		VitalSigns.get().getEdgesResolvers().put(GraphContext.ServiceWide, new ServiceWideEdgesResolver());

		
		VitalSigns vs = VitalSigns.get();
		
		if(vs.getVitalService() == null && vs.getVitalServiceAdmin() == null) {
			
			log.info("No active service/adminservice set in vitalsigns, setting it now");
			vs.setVitalService(vitalService);
			vs.setCurrentApp(vitalService.getApp());
			
		} else {
		
			log.warn("An active vitalservice or adminservice already set in vitalsigns");
			
		}
		

		if( vitalService.getEndpointType() == EndpointType.VITALPRIME ) {
			
			VitalSignsConfig vscfg = VitalSigns.get().getConfig();

			if(vscfg.domainsStrategy == DomainsStrategy.dynamic && !vscfg.loadDeployedJars && vscfg.autoLoad) {
				
				//TODO restore sync
//				throw new RuntimeException("sync feature disabled");
				
//				log.warn("VitalServiceFactory: sync feature disabled!");
				
				try {
					VitalSigns.get().sync();
				} catch (Exception e) {
					throw new VitalServiceException(e);
				}
				
			} else {
			
				log.info("Initial sync skipped, triggered only when domainsStrategy=dynamic, loadDeployedJars=false and autoLoad=true");
			
			}
								
			
		} 
		
		if(vitalService instanceof IVitalServiceConfigAware) {
			IVitalServiceConfigAware impl = (IVitalServiceConfigAware) vitalService;
			impl.setName(serviceName);
			//TODO make is serializable in order to clone
//			impl.setConfig((VitalServiceConfig) VitalJavaSerializationUtils.clone(endpointConfig));
			impl.setConfig(endpointConfig);
			impl.setAuthKey(key);
			allOpenServices.add(impl);
			if(VitalSigns.get().getVitalService() == null) {
				VitalSigns.get().setVitalService(vitalService);
			}
 		} else {
 			throw new RuntimeException("All service implementations must also implement: " + IVitalServiceConfigAware.class.getCanonicalName());
 		}
		
		
		return vitalService;

	}
	
	
	/**
	 * Opens a new vitalservice instance for given key and profile
	 * Service name will be assigned automatically
	 * @param key
	 * @param profileName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalService openService(VitalServiceKey key, String profileName) throws VitalServiceException {
		return openService(key, profileName, null);
	}
	
	static AtomicInteger adminServiceCounter = new AtomicInteger();
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
		
	public static class ServiceConfigWrapper {
	
		ServiceConfigWrapper(Config cfg, VitalServiceConfig serviceConfig, VitalOrganization organization, VitalApp app) {
			super();
			this.cfg = cfg;
			this.serviceConfig = serviceConfig;
			this.organization = organization;
			this.app = app;
		}
		
		public Config cfg;
		
		public VitalServiceConfig serviceConfig;
		
		public VitalOrganization organization;
		
		public VitalApp app;
		
	}
	
	public static VitalServiceConfig parseConfigString(String configString) {
		
		Config cfg = ConfigFactory.parseString(configString);
		
		String typeV = cfg.getString("type");

		EndpointType type = EndpointType.fromString(typeV);
		
		@SuppressWarnings("unchecked")
		EndpointConfigCreator<VitalServiceConfig> creator = (EndpointConfigCreator<VitalServiceConfig>) endpointType2Creator.get(type);

		if(creator == null) throw new RuntimeException("No creator for endpoint type found: " + type);
		
		VitalServiceConfig endpointConfig = creator.createConfig();
		
		endpointConfig.setEndpointtype(type);
		
		String appID = null;
		try {
			appID = cfg.getString("appID");
		} catch(Exception e) {}
		
		VitalApp app = VitalApp.withId(appID);
		
		endpointConfig.setApp(app);
		
		creator.setCommonProperties(endpointConfig, cfg);
		creator.setCustomConfigProperties(endpointConfig, cfg);
		
		return endpointConfig;
		
	}
	
	public static ServiceConfigWrapper getProfileConfig(String profileName) throws VitalServiceException {
		
		Config cfg = getConfig();

		Config profileCfg = null;
		try {
			profileCfg = cfg.getConfig("profile." + profileName);
		} catch(Exception e) {
			throw new VitalServiceException("Service profile not found: " + profileName);
		}
		
		log.debug("Creating vital service, profile: " + profileName);
		
		cfg = profileCfg;
		
		
		String typeV = cfg.getString("type");

		EndpointType type = EndpointType.fromString(typeV);
		
		@SuppressWarnings("unchecked")
		EndpointConfigCreator<VitalServiceConfig> creator = (EndpointConfigCreator<VitalServiceConfig>) endpointType2Creator.get(type);

		if(creator == null) throw new RuntimeException("No creator for endpoint type found: " + type);
		
		VitalServiceConfig endpointConfig = creator.createConfig();
		
		endpointConfig.setEndpointtype(type);
		
		String organizationID = null;
		try {
			organizationID = cfg.getString("organizationID");
		} catch(Exception e) {}
		
//		if(organizationID == null || organizationID.isEmpty()) throw new Exception("No organizationID property!")
		if(organizationID != null) {
			log.warn("organizationID param ignored - organization is read from license");
		}
		
		
		String appID = null;
		try {
			appID = cfg.getString("appID");
		} catch(Exception e) {}
//		if(appID == null || appID.isEmpty()) throw new Exception("No appID property!");
		
		if(type == EndpointType.VITALPRIME && appID != null) {
			log.warn("appID not longer configurable for prime - it is set by the authentication");
		}
		
		VitalApp app = VitalApp.withId(appID);
		
		endpointConfig.setApp(app);
		
		creator.setCommonProperties(endpointConfig, cfg);
		creator.setCustomConfigProperties(endpointConfig, cfg);
		
		return new ServiceConfigWrapper(cfg, endpointConfig, VitalSigns.get().getOrganization(), app);
		
	}
	
	/**
	 * Opens a new vitalservice instance for given key, profile and label
	 * @param key
	 * @param profileName
	 * @param serviceName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalService openService(VitalServiceKey key, String profileName, String serviceName) throws VitalServiceException{

		if(profileName == null || profileName.isEmpty()) throw new NullPointerException("profileName must not be null nor empty");
		
		validateKey(key);
		
		ServiceConfigWrapper endpointConfigWrapped = getProfileConfig(profileName);
		
		VitalServiceConfig endpointConfig = endpointConfigWrapped.serviceConfig;
		
		Config cfg = endpointConfigWrapped.cfg; 
		
		VitalService vitalService = openService(key, endpointConfig, serviceName);
		
		
		List<DatabaseConnectionWrapped> wrappedConfig = DatabaseConnectionsConfig.initFromServiceConfig(cfg);

		if( vitalService.getEndpointType() == EndpointType.VITALPRIME ) {
			
			if(wrappedConfig.size() > 0) log.warn("Database connections configuration skipped in service with prime endpoint");

		} else {
		
			log.info("Initial sync skipped, Not an endpoint with datascripts support");
			
			DatabaseConnectionsImplementation dbConnImpl = new DatabaseConnectionsImplementation();
			
			for(DatabaseConnectionWrapped dcw : wrappedConfig) {
				
				if(dcw.organizationID != null) log.warn("organizationID set in db connection: " + dcw.connection.get(Property_hasName.class).toString() + " ignored");
				if(dcw.appID != null) log.warn("appID in db connection: " + dcw.connection.get(Property_hasName.class).toString() + " ignored");
				
				dcw.connection.set(Property_hasOrganizationID.class, vitalService.getOrganization().getRaw(Property_hasOrganizationID.class));
				dcw.connection.set(Property_hasAppID.class, vitalService.getApp().getRaw(Property_hasAppID.class));
				
				VitalStatus addStatus = null;
				try {
					addStatus = dbConnImpl.addDatabaseConnection(endpointConfigWrapped.organization, endpointConfigWrapped.app, dcw.connection);
				} catch (VitalServiceUnimplementedException e) {
					addStatus = VitalStatus.withError(e.getLocalizedMessage());
				}
				
				if(addStatus.getStatus() != VitalStatus.Status.ok) {
					log.error("Couldn't add initial db connection: " + dcw.connection.get(Property_hasName.class).toString() + " ignored, reason: " + addStatus.getMessage());
				}
				
			}
			
			((AbstractVitalServiceImplementation)vitalService).setDbConnectionsImpl(dbConnImpl);
			
			
		} 
		
		return vitalService;
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
//		FlumeService flumeService = null;
//		
//		return createAdminServiceInstance(config, flumeService, new Lock());
//		
//	}
	
	public static boolean close(VitalService service) throws VitalServiceUnimplementedException, VitalServiceException {
		
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
		
		if(VitalSigns.get().getVitalService() == service) {
			log.info("VitalSigns service set to null");
			VitalSigns.get().setVitalService(null);
			VitalSigns.get().setCurrentApp(null);
		} else {
			log.warn("This service was not set as active in vitalsigns");
		}
		
		return true;
		
	}
	
	public static boolean close(VitalServiceAdmin serviceAdmin) throws VitalServiceUnimplementedException, VitalServiceException {
		
		boolean found = false;
		
		synchronized(allOpenServices) {
			
			for(IVitalServiceConfigAware vsa : allOpenServices) {
				
				if(vsa == serviceAdmin) {
					found = true;
				}
				
			}
			
			allOpenServices.remove(serviceAdmin);
			
		}
		
		
		if(!found) return false;
		
		//close it again, it should detect inner loops!
		serviceAdmin.close();
		
		if(VitalSigns.get().getVitalServiceAdmin() == serviceAdmin) {
			log.info("VitalSigns service admin set to null");
			VitalSigns.get().setVitalServiceAdmin(null);
			VitalSigns.get().setCurrentApp(null);
		} else {
			log.warn("This service admin was not set as active in vitalsigns");
		}
		
		return true;
		
	}
	
	public static List<VitalService> listOpenServices() {
		List<VitalService> r = new ArrayList<VitalService>();
		synchronized(allOpenServices) {
			for(IVitalServiceConfigAware v : allOpenServices) {
				if(v instanceof VitalService) {
					r.add((VitalService) v);
				}
			}
		}
		return r;
	}
	
	public static List<VitalService> listOpenServices(VitalServiceKey key) {
		if(key == null) return listOpenServices();
		List<VitalService> filtered = new ArrayList<VitalService>();
		synchronized(allOpenServices) {
			for(IVitalServiceConfigAware ca : allOpenServices) {
				if(!(ca instanceof VitalService)) continue;
				VitalService service = (VitalService) ca;
				VitalServiceKey k = (VitalServiceKey) ca.getAuthKey();
				if(k != null && k.getRaw(Property_hasKey.class).equals(key.getRaw(Property_hasKey.class))) {
					filtered.add(service);
				}
			}
		}
		return filtered;
	}
	
	public static List<VitalServiceAdmin> listOpenAdminServices(VitalServiceAdminKey key) {
		if(key == null) return listOpenAdminServices();
		List<VitalServiceAdmin> filtered = new ArrayList<VitalServiceAdmin>();
		synchronized(allOpenServices) {
			for(IVitalServiceConfigAware ca : allOpenServices) {
				if(!(ca instanceof VitalServiceAdmin)) continue;
				VitalServiceAdmin service = (VitalServiceAdmin) ca;
				VitalServiceAdminKey k = (VitalServiceAdminKey) ca.getAuthKey();
				if(k != null && k.getRaw(Property_hasKey.class).equals(key.getRaw(Property_hasKey.class))) {
					filtered.add(service);
				}
			}
		}
		return filtered;
		
	}
	
	public static List<VitalServiceAdmin> listOpenAdminServices() {
		List<VitalServiceAdmin> r = new ArrayList<VitalServiceAdmin>();
		synchronized(allOpenServices) {
			for(IVitalServiceConfigAware ca : allOpenServices) {
				if(ca instanceof VitalServiceAdmin) {
					r.add((VitalServiceAdmin) ca);
				}
			}
		}
		return r;
	}
	
	private static VitalService createServiceInstance(VitalServiceConfig endpointConfig, VitalOrganization organization, VitalApp app) throws Exception {
		
		EndpointType type = endpointConfig.getEndpointtype(); 
		
		VitalService vitalService = null;
		
		//TODO add flume service!
		if(type == EndpointType.LUCENEDISK) {
			
			vitalService = VitalServiceLuceneDisk.create((VitalServiceLuceneDiskConfig) endpointConfig, organization, app);
			
		} else if(type == EndpointType.LUCENEMEMORY) {
		
			vitalService = VitalServiceLuceneMemory.create((VitalServiceLuceneMemoryConfig) endpointConfig, organization, app);
		
		} else if(type == EndpointType.MOCK) {
		
			vitalService = VitalServiceMock.create((VitalServiceMockConfig) endpointConfig, organization, app);
		
		} else if(type == EndpointType.INDEXDB) {
		
			vitalService = VitalServiceIndexedDB.create((VitalServiceIndexedDBConfig) endpointConfig, organization, app);
			
		} else if(type == EndpointType.VITALPRIME) {
					
			throw new RuntimeException("Prime service instance should be created with a different method");
//			vitalService = VitalServicePrime.create((VitalServicePrimeConfig) endpointConfig, flumeService, lock, organization, app);
			
		} else if(type == EndpointType.ALLEGROGRAPH) {
		
			vitalService = VitalServiceAllegrograph.create((VitalServiceAllegrographConfig) endpointConfig, organization, app);
		
		}  else if(type == EndpointType.SQL) {
		
			vitalService = new VitalServiceSql((VitalServiceSqlConfig) endpointConfig, organization, app);
			
		} else throw new Exception("Unhandled endpoint type: " + type);
		
		vitalService.setDefaultSegmentName(endpointConfig.getDefaultSegmentName());
		
		URIGenerationStrategy uriGenerationStrategy = endpointConfig.getUriGenerationStrategy();
		if(uriGenerationStrategy == null) uriGenerationStrategy = URIGenerationStrategy.local;
		((AbstractVitalServiceImplementation)vitalService).setUriGenerationStrategy(uriGenerationStrategy);
		
		return vitalService;
		
	}
	
	private static VitalServiceAdmin createAdminServiceInstance(VitalServiceConfig endpointConfig, VitalOrganization organization) {
		
		EndpointType type = endpointConfig.getEndpointtype();

		VitalServiceAdmin vitalServiceAdmin = null;
		
		if(type == EndpointType.LUCENEDISK) {
			
			vitalServiceAdmin = VitalServiceAdminLuceneDisk.create((VitalServiceLuceneDiskConfig) endpointConfig, organization);
			
		} else if(type == EndpointType.LUCENEMEMORY) {
		
			vitalServiceAdmin = VitalServiceAdminLuceneMemory.create((VitalServiceLuceneMemoryConfig) endpointConfig, organization);
		
		}  else if(type == EndpointType.MOCK) {
		
			vitalServiceAdmin = VitalServiceAdminMock.create((VitalServiceMockConfig) endpointConfig, organization);
		
		} else if(type == EndpointType.INDEXDB) {
		
			vitalServiceAdmin = VitalServiceAdminIndexedDB.create((VitalServiceIndexedDBConfig) endpointConfig, organization);
			
		} else if(type == EndpointType.VITALPRIME) {
					
			throw new RuntimeException("Use different method to create an admin instance");
//			vitalServiceAdmin = VitalServiceAdminPrime.create((VitalServicePrimeConfig) endpointConfig, flumeService, lock, organization);
			
		} else if(type == EndpointType.ALLEGROGRAPH) {
		
			vitalServiceAdmin = VitalServiceAdminAllegrograph.create((VitalServiceAllegrographConfig) endpointConfig, organization);
		
		}  else if(type == EndpointType.SQL) {
		
			vitalServiceAdmin = new VitalServiceAdminSql((VitalServiceSqlConfig) endpointConfig, organization);
		
		} else throw new RuntimeException("Unhandled endpoint type: " + type);
		
		
		URIGenerationStrategy uriGenerationStrategy = endpointConfig.getUriGenerationStrategy();
		if(uriGenerationStrategy == null) uriGenerationStrategy = URIGenerationStrategy.local;
		((AbstractVitalServiceAdminImplementation)vitalServiceAdmin).setUriGenerationStrategy(uriGenerationStrategy);
		
		return vitalServiceAdmin;
		
	}
	
	
	/**
	 * Opens a new vitalservice admin instance for given key, profile and label
	 * @param key
	 * @param profileName
	 * @param serviceName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceAdmin openAdminService(VitalServiceAdminKey key, String profileName, String serviceName) throws VitalServiceException {
	
		validateKey(key);

		ServiceConfigWrapper profileConfig = getProfileConfig(profileName);
		
		Config cfg = profileConfig.cfg;

		VitalServiceConfig endpointConfig = profileConfig.serviceConfig;
		
		VitalServiceAdmin vitalServiceAdmin = openAdminService(key, endpointConfig, serviceName);
		
		List<DatabaseConnectionWrapped> wrappedConfig = DatabaseConnectionsConfig.initFromServiceConfig(cfg);
		
		if( vitalServiceAdmin.getEndpointType() == EndpointType.VITALPRIME ) {
			
			if(wrappedConfig.size() > 0) log.warn("Database connections configuration skipped in service with prime endpoint");
			
		} else {
		
			for(DatabaseConnectionWrapped dcw : wrappedConfig) {
				
				if(dcw.organizationID != null) {
					log.warn("organizationID set in db connection: " + dcw.connection.get(Property_hasName.class).toString() + " ignored");
				}
				if(dcw.appID == null) {
					log.error("appID in db connection: " + dcw.connection.get(Property_hasName.class).toString() + " not set. It is required in vitalserviceadmin instance");
					continue;
				}
				
				dcw.connection.set(Property_hasOrganizationID.class, vitalServiceAdmin.getOrganization().getRaw(Property_hasOrganizationID.class));
				dcw.connection.set(Property_hasAppID.class, dcw.appID);
				
				VitalApp a = VitalApp.withId(dcw.appID);
				VitalStatus addStatus = null;;
				try {
					addStatus = vitalServiceAdmin.addDatabaseConnection(a, dcw.connection);
				} catch (VitalServiceUnimplementedException e) {
					addStatus = VitalStatus.withError(e.getLocalizedMessage());
				}
//				
				if(addStatus.getStatus() != VitalStatus.Status.ok) {
					log.error("Couldn't add initial db connection: " + dcw.connection.get(Property_hasName.class).toString() + " ignored, reason: " + addStatus.getMessage());
				}
				
			}
			
			
		}
		
		return vitalServiceAdmin;
		
		
		
	}
	
	
	
	/**
	 * Opens a new vitalservice admin instance for given key and "default" profile
	 * @param key
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceAdmin openAdminService(VitalServiceAdminKey key) throws VitalServiceException {
		return openAdminService(key, DEFAULT_PROFILE);
	}
	
	
	/**
	 * Opens a new vitalservice admin instance for given key and profile
	 * Service name will be assigned automatically
	 * @param key
	 * @param profileName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceAdmin openAdminService(VitalServiceAdminKey key, String profileName) throws VitalServiceException {
		return openAdminService(key, profileName, null);
	}
	
	
	/**
	 * Opens a new vitalservice admin instance for given key and config object
	 * @param key
	 * @param config
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceAdmin openAdminService(VitalServiceAdminKey key, VitalServiceConfig config) throws VitalServiceException {
		return openAdminService(key, config, null);
	}
	
	/**
	 * Opens a new vitalservice admin instance for given key, config object and name
	 * @param key
	 * @param endpointConfig
	 * @param serviceName
	 * @return
	 * @throws VitalServiceException
	 */
	public static VitalServiceAdmin openAdminService(VitalServiceAdminKey key, VitalServiceConfig endpointConfig, String serviceName) throws VitalServiceException {

		validateKey(key);

		
		if(serviceName == null || serviceName.isEmpty()) {
			serviceName = "vital-service-admin-" + adminServiceCounter.incrementAndGet();
		} else {
			checkServiceName(serviceName);
		}
		
		
		VitalServiceConfigValidator.checkIfServiceCanBeOpened(serviceName, endpointConfig);
		
		if(allOpenServices.size() >= maxActiveServices) throw new VitalServiceException("Max open services count limit reached: " + maxActiveServices);
		
		
		EndpointType et = endpointConfig.getEndpointtype();
		
		VitalServiceAdmin vitalServiceAdmin = null;
		

		VitalOrganization organization = VitalSigns.get().getOrganization();
		
		if(et == EndpointType.VITALPRIME) {
			
			if( endpointConfig.getApp() != null ) {
				log.warn("service config app param ignored");
			}
			
			VitalServicePrimeConfig primeCfg = (VitalServicePrimeConfig) endpointConfig;
			//accept it

			if( endpointConfig.getApp() != null ) {
				log.warn("service config app param ignored");
			}
			
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
			VitalOrganization _organization = null;
//			VitalApp _app = null;
			
			for(GraphObject g : rl) {
				
				if(g instanceof VitalSession) {
					_session = (VitalSession) g;
				} else if(g instanceof VitalOrganization) {
					_organization = (VitalOrganization)g;
//				} else if(g instanceof VitalApp) {
//					_app = (VitalApp) g;
//				}
				}
			}
			
			if(_session == null) throw new VitalServiceException("No session returned by prime");
			if(_organization == null) throw new VitalServiceException("No organization returned by prime");
//			if(_app == null) throw new VitalServiceException("No app returned by prime");
			
			String oid = (String) _organization.getRaw(Property_hasOrganizationID.class);
			if(!organization.getRaw(Property_hasOrganizationID.class).equals( oid ) ) {
				log.warn("Organization ID does not match the value returned by prime: " + organization.getRaw(Property_hasOrganizationID.class) + ", remote: " + oid);
//				throw new VitalServiceException("Organization ID does not match the value returned by prime: " + organization.getRaw(Property_hasOrganizationID.class) + ", remote: " + oid);
			}
			
			vitalServiceAdmin = VitalServiceAdminPrime.create(client, _organization);
			
		} else {
			
			try {
				vitalServiceAdmin = createAdminServiceInstance(endpointConfig, organization);
			} catch (Exception e) {
				throw new VitalServiceException(e);
			}
			
		}

		
		//where to read the organization/app from 
//		VitalSigns.get().setCurrentApp(null);
//		VitalSigns.get().setVitalService(null);
//		VitalSigns.get().setVitalServiceAdmin(vitalServiceAdmin);
		
		VitalSigns.get().getEdgesResolvers().put(GraphContext.ServiceWide, new ServiceWideEdgesResolver());

		if(vitalServiceAdmin instanceof IVitalServiceConfigAware) {
			IVitalServiceConfigAware impl = (IVitalServiceConfigAware) vitalServiceAdmin;
			impl.setName(serviceName);
			//TODO make it serializable in order to clone it
//			impl.setConfig((VitalServiceConfig) VitalJavaSerializationUtils.clone(endpointConfig));
			impl.setConfig(endpointConfig);
			impl.setAuthKey(key);
			allOpenServices.add(impl);
			if( VitalSigns.get().getVitalServiceAdmin() == null ) {
				VitalSigns.get().setVitalServiceAdmin(vitalServiceAdmin);
				if(VitalSigns.get().getCurrentApp() == null) {
					VitalSigns.get().setCurrentApp(endpointConfig.getApp());
				}
			}
		} else if(vitalServiceAdmin instanceof VitalServiceAdminPrime) {
			throw new RuntimeException("All VitalServiceAdmin implementations must also implement " + IVitalServiceConfigAware.class.getCanonicalName());
		}
		
		VitalSigns vs = VitalSigns.get();
		
		if(vs.getVitalService() == null && vs.getVitalServiceAdmin() == null) {
			
			log.info("No active service/adminservice set in vitalsigns, setting adminservice now, no app");
			vs.setVitalServiceAdmin(vitalServiceAdmin);
			vs.setCurrentApp(null);
			
		} else {
		
			log.warn("An active vitalservice or adminservice already set in vitalsigns");
			
		}

		return vitalServiceAdmin;
		
	}
	
	public static File getConfigFile() {
		
		String vitalHome = System.getenv("VITAL_HOME");
		
		if(!StringUtils.isEmpty(vitalHome)) {

			File vitalHomeF = new File(vitalHome);
			
			if(!vitalHomeF.exists()) throw new RuntimeException("VITAL_HOME path does not exist: " + vitalHomeF.getAbsolutePath());
			
			if(!vitalHomeF.isDirectory()) throw new RuntimeException("VITAL_HOME path is not a directory: " + vitalHomeF.getAbsolutePath());
			
			return new File(vitalHomeF, "vital-config/vitalservice/vitalservice.config");
						
		} else {
		
			throw new RuntimeException("VITAL_HOME not set!");
		}
	}
	
	public static Config getConfig() {

		String vitalHome = System.getenv("VITAL_HOME");
		
		Config cfg = null;
		
		if(!StringUtils.isEmpty(vitalHome)) {
			
			File vitalHomeF = new File(vitalHome);
			
			if(!vitalHomeF.exists()) throw new RuntimeException("VITAL_HOME path does not exist: " + vitalHomeF.getAbsolutePath());
			
			if(!vitalHomeF.isDirectory()) throw new RuntimeException("VITAL_HOME path is not a directory: " + vitalHomeF.getAbsolutePath());
			
			File configFile = new File(vitalHomeF, "vital-config/vitalservice/vitalservice.config");
			
			if(!configFile.exists()) throw new RuntimeException("config file path does not exist: " + configFile.getAbsolutePath());
			
			if(!configFile.isFile()) throw new RuntimeException("config file path is not a file: " + configFile.getAbsolutePath());

			cfg = ConfigFactory.parseFile(configFile);
						
		} else {
		
			log.warn("VITAL_HOME env variable not set, looking for service config in the classpath: /resources/vital-config/vitalservice/vitalservice.config");
			
			InputStreamReader isr = null;
			
			try {
				InputStream ins = VitalServiceFactory.class.getResourceAsStream("/resources/vital-config/vitalservice/vitalservice.config");
				if(ins == null) throw new RuntimeException("Classpath resource not found: /resources/vital-config/vitalservice/vitalservice.config");
				isr = new InputStreamReader( ins, "UTF-8");
				
				cfg = ConfigFactory.parseReader(isr);
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				IOUtils.closeQuietly(isr);
			}
			
		}
		
		cfg.resolve();
		
		return cfg;
		
	}
	
	public static List<String> getAvailableProfiles() {
		
		Config cfg = getConfig();

		return new ArrayList<String>(cfg.getConfig("profile").root().unwrapped().keySet());
		

	}

	//returns all open services config objects (any service type) 
	public static List<VitalServiceConfig> listAllOpenServicesConfig() {
		
		List<VitalServiceConfig> cfgs = new ArrayList<VitalServiceConfig>();
		
		synchronized (allOpenServices) {
			for(IVitalServiceConfigAware vs : allOpenServices) {
				VitalServiceConfig cfg = vs.getConfig();
				if(cfg != null) cfgs.add(cfg);
			}
		}
		
		return cfgs;
		
	}
}
