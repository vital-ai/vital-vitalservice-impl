package ai.vital.indexeddb.service

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config
import com.typesafe.config.ConfigException;

import ai.vital.allegrograph.service.VitalServiceAllegrographConfigCreator
import ai.vital.sql.service.VitalServiceSqlConfigCreator
import ai.vital.vitalservice.EndpointType;
import ai.vital.indexeddb.service.admin.VitalServiceAdminIndexedDB
import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig
import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig.QueryTarget;
import ai.vital.indexeddb.service.impl.AllegrographImpl
import ai.vital.indexeddb.service.impl.DBInterface;
import ai.vital.indexeddb.service.impl.IndexInterface;
import ai.vital.indexeddb.service.impl.LuceneDiskImpl
import ai.vital.lucene.disk.service.VitalServiceLuceneDiskConfigCreator;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.factory.EndpointConfigCreator

class VitalServiceIndexedDBConfigCreator extends EndpointConfigCreator<VitalServiceIndexedDBConfig> {

	private final static Logger log = LoggerFactory.getLogger(VitalServiceIndexedDBConfigCreator.class)
	
	@Override
	public VitalServiceIndexedDBConfig createConfig() {
		return new VitalServiceIndexedDBConfig();
	}

	@Override
	public void setCustomConfigProperties(VitalServiceIndexedDBConfig config,
			Config cfgObject) {

		Config cfg = cfgObject.getConfig("IndexDB")
	
		String index = cfg.getString("index_impl")
		
		String database = cfg.getString("database_impl")
		
		//index by default
		QueryTarget selectQueries = QueryTarget.index
		try {
			String selectQueriesParam = cfg.getString('selectQueries')
			selectQueries = QueryTarget.valueOf(selectQueriesParam)	
		} catch(ConfigException.Missing ex) {
			log.warn("No selectQueries param, default: " + selectQueries.name())
		}
		
		QueryTarget graphQueries = QueryTarget.database
		try {
			String graphQueriesParam = cfg.getString('graphQueries')
			graphQueries = QueryTarget.valueOf(graphQueriesParam)
		} catch(ConfigException.Missing ex) {
			log.warn("No graphQueries param, default: " + graphQueries.name())
		}
		
		EndpointConfigCreator indexCreator = null
		
		EndpointConfigCreator databaseCreator = null
		
		if(index == EndpointType.LUCENEDISK.name) {
			
			indexCreator = new VitalServiceLuceneDiskConfigCreator()
			
			
		} else {
			throw new RuntimeException("Unknown index implementation: ${index}")
		}
		
		if(database == EndpointType.ALLEGROGRAPH.name) {
		
			databaseCreator = new VitalServiceAllegrographConfigCreator()	
		
		} else if(database == EndpointType.SQL.name) {
		
			databaseCreator = new VitalServiceSqlConfigCreator()
			
		} else {
			throw new RuntimeException("Unknown database implementation: ${database}");
		}

		
		VitalServiceConfig indexConfig = indexCreator.createConfig()
		indexCreator.setCustomConfigProperties(indexConfig, cfg)

		indexConfig.app = config.app
		indexConfig.endpointtype = EndpointType.fromString(index)
				
		VitalServiceConfig databaseConfig = databaseCreator.createConfig()
		databaseCreator.setCustomConfigProperties(databaseConfig, cfg)
		
		databaseConfig.app = config.app
		databaseConfig.endpointtype = EndpointType.fromString(database)
		
		config.selectQueries = selectQueries
		config.graphQueries = graphQueries
		config.indexConfig = indexConfig
		config.dbConfig = databaseConfig
		
	}

	@Override
	public boolean allowMultipleInstances() {
		return false;
	}

}
