package ai.vital.vitalservice.dbconnection;

import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExternalSqlQuery;
import ai.vital.vitalsigns.model.SqlDatabaseConnection;

public abstract class ExternalSqlDatabase {

	public abstract void validateConfig(SqlDatabaseConnection connection);

	public abstract ResultList query(SqlDatabaseConnection connection, VitalExternalSqlQuery sq);
	
}

