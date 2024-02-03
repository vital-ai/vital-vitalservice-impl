package ai.vital.sql.service;

import com.typesafe.config.Config;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
import ai.vital.sql.service.config.VitalServiceSqlConfig.DBType;
import ai.vital.vitalservice.factory.EndpointConfigCreator;

public class VitalServiceSqlConfigCreator extends EndpointConfigCreator<VitalServiceSqlConfig> {

	@Override
	public VitalServiceSqlConfig createConfig() {
		return new VitalServiceSqlConfig();
	}

	@Override
	public void setCustomConfigProperties(VitalServiceSqlConfig config,
			Config cfgObject) {

		Config cfg = cfgObject.getConfig("SQL");
		cfg.resolve();
		
		String dbType = cfg.getString("dbType");
		
		config.setDbType(DBType.valueOf(dbType));
		config.setEndpointURL(cfg.getString("endpointURL"));
		config.setPassword(cfg.getString("password"));
		config.setPoolInitialSize(cfg.getInt("poolInitialSize"));
		config.setPoolMaxTotal(cfg.getInt("poolMaxTotal"));
		config.setTablesPrefix(cfg.getString("tablesPrefix"));
		config.setUsername(cfg.getString("username"));
		
	}

	@Override
	public boolean allowMultipleInstances() {
		return true;
	}

}
