package ai.vital.sql.service.config;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.config.VitalServiceConfig;

public class VitalServiceSqlConfig extends VitalServiceConfig {

	private static final long serialVersionUID = 1L;
	
	public VitalServiceSqlConfig() {
		setEndpointtype(EndpointType.SQL);
	}
	
	public static enum DBType {
		MySQL,
		MySQLMemSQL,
		MySQLAurora,
		AmazonRedshift,
		PostgreSQL,
		ApacheSparkSQL,
		EMRSparkSQL,
		HiveSQL
	}
	
	private String tablesPrefix;
	
	private String endpointURL;
	
	private String username;
	
	private String password;
	
	private DBType dbType;

	private Integer poolInitialSize = 1;
	
	private Integer poolMaxTotal = 5;

	
	public String getTablesPrefix() {
		return tablesPrefix;
	}

	public void setTablesPrefix(String tablesPrefix) {
		this.tablesPrefix = tablesPrefix;
	}

	public String getEndpointURL() {
		return endpointURL;
	}

	public void setEndpointURL(String endpointURL) {
		this.endpointURL = endpointURL;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public DBType getDbType() {
		return dbType;
	}

	public void setDbType(DBType dbType) {
		this.dbType = dbType;
	}

	public Integer getPoolInitialSize() {
		return poolInitialSize;
	}

	public void setPoolInitialSize(Integer poolInitialSize) {
		this.poolInitialSize = poolInitialSize;
	}

	public Integer getPoolMaxTotal() {
		return poolMaxTotal;
	}

	public void setPoolMaxTotal(Integer poolMaxTotal) {
		this.poolMaxTotal = poolMaxTotal;
	}


}
