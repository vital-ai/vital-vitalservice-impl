package ai.vital.vitalservice.dbconnection.impl;


public class MySQLDatabase extends JDBCSQLDatabase {
	@Override
	protected String getDriverClassName() {
		return "com.mysql.jdbc.Driver";
	}

}
