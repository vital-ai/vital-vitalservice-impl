package ai.vital.vitalservice.dbconnection;

import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExternalSparqlQuery;
import ai.vital.vitalsigns.model.SparqlDatabaseConnection;

public abstract class ExternalSparqlDatabase {

	public abstract void validateConfig(SparqlDatabaseConnection connection);

	public abstract ResultList query(SparqlDatabaseConnection connection, VitalExternalSparqlQuery sq);

}
