package ai.vital.vitalservice.dbconnection.impl;

public class AmazonRedshiftSQLDatabase extends JDBCSQLDatabase {

	@Override
	protected String getDriverClassName() {
		return "com.amazon.redshift.jdbc41.Driver";
	}

}
