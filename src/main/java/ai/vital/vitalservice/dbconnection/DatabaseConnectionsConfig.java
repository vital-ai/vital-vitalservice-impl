package ai.vital.vitalservice.dbconnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsImplementation.DatabaseConnectionWrapped;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsImplementation.SparqlEndpointType;
import ai.vital.vitalservice.dbconnection.DatabaseConnectionsImplementation.SqlEndpointType;
import ai.vital.vitalsigns.model.DatabaseConnection;
import ai.vital.vitalsigns.model.SparqlDatabaseConnection;
import ai.vital.vitalsigns.model.SqlDatabaseConnection;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasCatalogName;
import ai.vital.vitalsigns.model.properties.Property_hasDatabase;
import ai.vital.vitalsigns.model.properties.Property_hasEndpointType;
import ai.vital.vitalsigns.model.properties.Property_hasEndpointURL;
import ai.vital.vitalsigns.model.properties.Property_hasName;
import ai.vital.vitalsigns.model.properties.Property_hasOrganizationID;
import ai.vital.vitalsigns.model.properties.Property_hasPassword;
import ai.vital.vitalsigns.model.properties.Property_hasRepositoryName;
import ai.vital.vitalsigns.model.properties.Property_hasUsername;
import ai.vital.vitalsigns.model.properties.Property_isReadOnly;
import ai.vital.vitalsigns.utils.StringUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class DatabaseConnectionsConfig {

	private final static Logger log = LoggerFactory.getLogger(DatabaseConnectionsConfig.class);
	
	public static List<DatabaseConnectionWrapped> initFromServiceConfig(
			Config cfg) {

		List<DatabaseConnectionWrapped> r = new ArrayList<DatabaseConnectionWrapped>();

		Config nd = null;

		try {
			nd = cfg.getConfig("NamedDatabase");
		} catch (ConfigException.Missing e) {
			log.warn("No NamedDatabase.<name> sections in service profile");
			return r;
		}

		Set<String> keySet = nd.root().keySet();

		try {

			for (String key : keySet) {

				Config c = nd.getConfig(key);

				DatabaseConnection dbc = fromConfigObject(key, c);
				
				String organizationID = (String) dbc.getRaw(Property_hasOrganizationID.class);
				String appID = (String) dbc.getRaw(Property_hasAppID.class);
				
				r.add(new DatabaseConnectionWrapped(organizationID, appID, dbc));

			}

		} catch (Exception e) {
			log.error("Error when processing NamedDatabases section: "
					+ e.getLocalizedMessage());
		}

		return r;

	}

	public static DatabaseConnection fromConfigObject(String name, Config c) throws Exception {


		String type = c.getString("endpointType");

		// get type
		SparqlEndpointType spt = SparqlEndpointType.fromString(type);

		SqlEndpointType sqt = SqlEndpointType.fromString(type);
		
		DatabaseConnection dbc = null;

		String appID = null;
		try {
			appID = c.getString("appID");
		} catch (ConfigException.Missing e) {
			// throw new
			// Exception("No appID in database connection config");
		}

		String endpointURL = null;
		try {
			endpointURL = c.getString("endpointURL");
		} catch (ConfigException.Missing e) {
			throw new Exception(
					"No endpointURL in database connection config");
		}

		String username = null;
		try {
			username = c.getString("username");
		} catch (ConfigException.Missing e) {
			// throw new
			// Exception("No username in database connection config");
		}

		String password = null;
		try {
			password = c.getString("password");
		} catch (ConfigException.Missing e) {
			// throw new Exception("No password")
		}

		String organizationID = null;
		try {
			organizationID = c.getString("organizationID");
		} catch (ConfigException.Missing e) {
		}

		ExternalSparqlDatabase sdbImpl = null;
		
		ExternalSqlDatabase sqlDbImpl = null; 
		
		if (spt != null) {

			sdbImpl = DatabaseConnectionsImplementation.externalSparqlDatabasesImplementations.get(spt);
			
			if (spt == SparqlEndpointType.Allegrograph) {

				
				dbc = new SparqlDatabaseConnection();

				if (StringUtils.isEmpty(username))
					throw new Exception(spt.name()
							+ " database requires username");
				if (StringUtils.isEmpty(password))
					throw new Exception(spt.name()
							+ " database requires password");

				String catalogName = null;
				try {
					catalogName = c.getString("catalogName");
				} catch (ConfigException.Missing e) {

				}

				dbc.set(Property_hasCatalogName.class, catalogName);

				String repositoryName = null;
				try {
					repositoryName = c.getString("repositoryName");
				} catch (ConfigException.Missing e) {
					throw new Exception(spt.name()
							+ " database requires repositoryName");
				}

				dbc.set(Property_hasRepositoryName.class, repositoryName);

			} else {
				throw new Exception("Unhandled sparql endpoint type: "
						+ spt);
			}

		} else if(sqt != null) {
			
			sqlDbImpl = DatabaseConnectionsImplementation.externalSqlDatabasesImplementations.get(spt);
			
			if(sqt == SqlEndpointType.MySQL || 
					sqt == SqlEndpointType.AmazonRedshift || 
					sqt == SqlEndpointType.PostgreSQL ) {
			
				dbc = new SqlDatabaseConnection();
				
				if (StringUtils.isEmpty(username))
					throw new Exception(sqt.name()
							+ " database requires username");
				if (StringUtils.isEmpty(password))
					throw new Exception(sqt.name()
							+ " database requires password");
				
				String database= null;
				try {
					database = c.getString("database");
				} catch (ConfigException.Missing e) {
					throw new Exception(sqt.name()
							+ " requires database (name)");
				}
				
				dbc.set(Property_hasDatabase.class, database);
				
			} else {
				throw new Exception("Unhandled sql endpoint type: " + sqt);
			}
			
		} else {
			throw new Exception(
					"Unknown database connection endpoint type: "
							+ type);
		}

		dbc.set(Property_hasName.class, name);
		dbc.generateURI((VitalApp) null);
		dbc.set(Property_hasEndpointType.class, type);
		dbc.set(Property_hasEndpointURL.class, endpointURL);
		dbc.set(Property_hasUsername.class, username);
		dbc.set(Property_hasPassword.class, password);
		dbc.set(Property_hasOrganizationID.class, organizationID);
		dbc.set(Property_hasAppID.class, appID);
		//all databases from config file are read only
		dbc.set(Property_isReadOnly.class, true);
		if(sdbImpl != null) {
			sdbImpl.validateConfig((SparqlDatabaseConnection) dbc);
		}
		
		if(sqlDbImpl != null) {
			sqlDbImpl.validateConfig((SqlDatabaseConnection) dbc);
		}
		
		return dbc;
		
	}
	
	
}
