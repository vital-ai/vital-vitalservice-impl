package ai.vital.vitalservice.dbconnection.impl;

public class PostgresqlSQLDatabase extends JDBCSQLDatabase {

	@Override
	protected String getDriverClassName() {
		return "org.postgresql.Driver";
	}

}
