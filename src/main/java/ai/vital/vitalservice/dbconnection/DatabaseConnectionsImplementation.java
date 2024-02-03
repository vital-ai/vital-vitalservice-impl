package ai.vital.vitalservice.dbconnection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.dbconnection.impl.AllegrographSparqlDatabase;
import ai.vital.vitalservice.dbconnection.impl.AmazonRedshiftSQLDatabase;
import ai.vital.vitalservice.dbconnection.impl.MySQLDatabase;
import ai.vital.vitalservice.dbconnection.impl.PostgresqlSQLDatabase;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExternalSparqlQuery;
import ai.vital.vitalservice.query.VitalExternalSqlQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalsigns.model.DatabaseConnection;
import ai.vital.vitalsigns.model.SparqlDatabaseConnection;
import ai.vital.vitalsigns.model.SqlDatabaseConnection;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasEndpointType;
import ai.vital.vitalsigns.model.properties.Property_hasName;
import ai.vital.vitalsigns.model.properties.Property_hasOrganizationID;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.utils.StringUtils;

public class DatabaseConnectionsImplementation {

//	private final static Logger log = LoggerFactory.getLogger(DatabaseConnectionsImplemenetation.class);
	
	static Map<SparqlEndpointType, ExternalSparqlDatabase> externalSparqlDatabasesImplementations = new HashMap<SparqlEndpointType, ExternalSparqlDatabase>();
	
	static Map<SqlEndpointType, ExternalSqlDatabase> externalSqlDatabasesImplementations = new HashMap<SqlEndpointType, ExternalSqlDatabase>(); 
	
	public final static Logger log = LoggerFactory.getLogger(DatabaseConnectionsImplementation.class);
	
	public static class DatabaseConnectionWrapped {
		
		public String organizationID;
		public String appID;
		public DatabaseConnection connection;
		
		public DatabaseConnectionWrapped(String organizationID, String appID,
				DatabaseConnection connection) {
			super();
			this.organizationID = organizationID;
			this.appID = appID;
			this.connection = connection;
		}
		
	}
	
	static Set<SqlEndpointType> implementedSqlEndpoints = Collections.unmodifiableSet(new HashSet<SqlEndpointType>(Arrays.asList(
			SqlEndpointType.AmazonRedshift, 
			SqlEndpointType.MySQL, 
			SqlEndpointType.PostgreSQL)));
	
	static {
		
		externalSparqlDatabasesImplementations.put(SparqlEndpointType.Allegrograph, new AllegrographSparqlDatabase());
		
		
		Class<?> dbcp2Class = null;
		
		try {
			dbcp2Class = Class.forName("org.apache.commons.dbcp2.BasicDataSource");
		} catch (ClassNotFoundException e) {
			log.warn("dbcp2 disabled, sql external databases support disabled");
		}
		
		if(dbcp2Class != null) {
			externalSqlDatabasesImplementations.put(SqlEndpointType.MySQL, new MySQLDatabase());
			externalSqlDatabasesImplementations.put(SqlEndpointType.AmazonRedshift, new AmazonRedshiftSQLDatabase());
			externalSqlDatabasesImplementations.put(SqlEndpointType.PostgreSQL, new PostgresqlSQLDatabase());
		}
		
	}
	
	public static enum SparqlEndpointType {
		
		Allegrograph;
		
		public static SparqlEndpointType fromString(String type) {
			try {
				SparqlEndpointType t = valueOf(type);
				return t;
			} catch(Exception e) {
			}
			return null;
		}

		public static List<String> getSupported() {
			List<String> l = new ArrayList<String>();
			for(SparqlEndpointType t : SparqlEndpointType.values() ) {
				l.add(t.name());
			}
			return l;
		}
		
	}
	
	public static enum SqlEndpointType {
		
		//mysql and compatible
		MySQL,
		AmazonRedshift, 
		PostgreSQL;

		public static SqlEndpointType fromString(String type) {
			try {
				SqlEndpointType t = valueOf(type);
				return t;
			} catch(Exception e) {
			}
			return null;
		}
		
		public static List<String> getSupported() {
			List<String> l = new ArrayList<String>();
			for(SqlEndpointType t : SqlEndpointType.values() ) {
				l.add(t.name());
			}
			return l;
		}
		
	}
	
	Map<String, Map<String, List<DatabaseConnection>>> connectionsDao = new HashMap<String, Map<String, List<DatabaseConnection>>>();
	
	//named databases
	public synchronized ResultList listDatabaseConnections(VitalOrganization organization, VitalApp app) throws VitalServiceUnimplementedException, VitalServiceException {
		
		List<DatabaseConnection> list = new ArrayList<DatabaseConnection>();
		
		if(organization == null) {
			
			if(app != null) throw new RuntimeException("Cannot use null organization filter and non-null app");
			
			for( Entry<String, Map<String, List<DatabaseConnection>>> oe : connectionsDao.entrySet() ) {
				
				for(Entry<String, List<DatabaseConnection>> ae : oe.getValue().entrySet() ) {
					
					for(DatabaseConnection c : ae.getValue()) {
						
						try {
							list.add((DatabaseConnection)c.clone());
						} catch (CloneNotSupportedException ex) {
							throw new VitalServiceException(ex);
						}
						
					}
					
				}
				
			}
			
		} else {
			
			Map<String, List<DatabaseConnection>> appsMap = connectionsDao.get(organization.getRaw(Property_hasOrganizationID.class));
			
			if(appsMap != null) {
				
				if(app == null) {
					
					//list all
					for( Entry<String, List<DatabaseConnection>> e : appsMap.entrySet() ) {
						
						for(DatabaseConnection c : e.getValue() ) {
							
							try {
								list.add((DatabaseConnection)c.clone());
							} catch (CloneNotSupportedException ex) {
								throw new VitalServiceException(ex);
							}
							
						}
						
					}
					
				} else {
					
					List<DatabaseConnection> list2 = appsMap.get(app.getRaw(Property_hasAppID.class));
					
					if(list2 != null) {
						synchronized (list2) {
							
							for(DatabaseConnection c : list2) {
								try {
									list.add((DatabaseConnection)c.clone());
								} catch (CloneNotSupportedException e) {
									throw new VitalServiceException(e);
								}
							}
							
						}
					}
					
				}
				
			}
			
		}
		
		ResultList rl = new ResultList();
		for(DatabaseConnection c : list) {
			rl.getResults().add(new ResultElement(c, 1D));
		}
		return rl;
		
	}
	
	public synchronized VitalStatus addDatabaseConnection(VitalOrganization organization, VitalApp app, DatabaseConnection connection) throws VitalServiceUnimplementedException, VitalServiceException {
		
		IProperty nameProp = (IProperty) connection.get(Property_hasName.class);
		if(nameProp == null) return VitalStatus.withError("connection name property not set:");
		
		IProperty typeProp = (IProperty) connection.get(Property_hasEndpointType.class);
		if(typeProp == null) return VitalStatus.withError("endpointType must not be null");
		String type = typeProp.toString();
		
		if(connection instanceof SparqlDatabaseConnection) {

			SparqlEndpointType et = SparqlEndpointType.fromString(type);
			
			if(et == null) throw new RuntimeException("Unknown sparql endpoint type: " + type + ", supported: " + SparqlEndpointType.getSupported());
			
			ExternalSparqlDatabase externalSparqlDatabase = externalSparqlDatabasesImplementations.get(et);
			
			if(externalSparqlDatabase == null) throw new RuntimeException("No implementation for sparql endpoint: " + et.name());
			
			externalSparqlDatabase.validateConfig((SparqlDatabaseConnection)connection);
			
		} else if(connection instanceof SqlDatabaseConnection) {
			
			SqlEndpointType et = SqlEndpointType.fromString(type);
			
			if(et == null) throw new RuntimeException("Unknown sql endpoint type: " + type + ", supported: " + SqlEndpointType.getSupported());
			
			//check if vital-sql jars enabled

			if(!implementedSqlEndpoints.contains(et)) {
				throw new RuntimeException("No implementation for sql endpoint: " + et.name());
			}
			
			ExternalSqlDatabase externalSqlDatabase = externalSqlDatabasesImplementations.get(et);
			
			if(externalSqlDatabase == null) throw new RuntimeException("SQL databases disabled, no vital-sql module");
			
			externalSqlDatabase.validateConfig((SqlDatabaseConnection)connection);

			
		} else if(connection.getClass() == DatabaseConnection.class){
			throw new RuntimeException("connection object type must be a subclass of " + DatabaseConnection.class.getCanonicalName() + " not the type itself");
		} else {
			throw new RuntimeException("Unsupported connection object type: " + connection.getClass().getCanonicalName());
		}
		
		Map<String, List<DatabaseConnection>> appsMap = connectionsDao.get(organization.getRaw(Property_hasOrganizationID.class));
		
		String name = nameProp.toString();
		
		//databases are unique per app only ?
		
		if(StringUtils.isEmpty(name)) return VitalStatus.withError("connection name property must not be empty");
		
		if(appsMap != null) {
			
			List<DatabaseConnection> list = appsMap.get(app.getRaw(Property_hasAppID.class));
			
			if(list != null) {

				for(DatabaseConnection c : list) {
					
					String p = c.get(Property_hasName.class).toString();
					
					if(name.equals(p)) return VitalStatus.withError("Database with name " + name + " already exists");
					
				}
				
			}
		}
		
		if(appsMap == null) {
			appsMap = new HashMap<String, List<DatabaseConnection>>();
			connectionsDao.put((String) organization.getRaw(Property_hasOrganizationID.class), appsMap);
		}
		
		List<DatabaseConnection> list = appsMap.get(app.getRaw(Property_hasAppID.class));
		
		if(list == null) {
			list = new ArrayList<DatabaseConnection>();
			appsMap.put((String) app.getRaw(Property_hasAppID.class), list);
		}
		
		try {
			list.add((DatabaseConnection) connection.clone());
		} catch (CloneNotSupportedException e) {
			throw new VitalServiceException(e);
		}
		
		return VitalStatus.withOKMessage("connection added: " + name);
		
	}
	
	public synchronized VitalStatus removeDatabaseConnection(VitalOrganization organization, VitalApp app, String databaseName) throws VitalServiceUnimplementedException, VitalServiceException {
		
		if(StringUtils.isEmpty(databaseName)) throw new NullPointerException("Null or empty databaseName"); 
		
		Map<String, List<DatabaseConnection>> map = connectionsDao.get(organization.getRaw(Property_hasOrganizationID.class));
		
		if(map == null) return VitalStatus.withError("Connection with name not found: " + databaseName);
		
		List<DatabaseConnection> list = map.get(app.getRaw(Property_hasAppID.class));
		
		if(list == null) return VitalStatus.withError("Connection with name not found: " + databaseName);
		
		for( Iterator<DatabaseConnection> iterator = list.iterator(); iterator.hasNext(); ) {
			
			DatabaseConnection c = iterator.next();
			
			String p = c.get(Property_hasName.class).toString();
			
			if(databaseName.equals(p)) {
				iterator.remove();
				return VitalStatus.withOKMessage("Database connection " + databaseName + " removed");
			}
			
		}
		
		return VitalStatus.withError("Connection with name not found: " + databaseName);
		
	}
	
	public ResultList query(VitalOrganization organization, VitalApp app, VitalQuery query) {
		
		String databaseName = null;
		
		if(query instanceof VitalExternalSparqlQuery) {
			
			VitalExternalSparqlQuery sq = (VitalExternalSparqlQuery) query;
			
			databaseName = sq.getDatabase();
			
			if(StringUtils.isEmpty(databaseName)) throw new RuntimeException("databaseName must not be empty");
			
		} else if(query instanceof VitalExternalSqlQuery ) {
			
			VitalExternalSqlQuery sq = (VitalExternalSqlQuery) query;
			
			databaseName = sq.getDatabase();
			
			if(StringUtils.isEmpty(databaseName)) throw new RuntimeException("databaseName must not be empty");
			
		} else {
			throw new RuntimeException("Unsupported query " + query.getClass().getCanonicalName());
		}


		DatabaseConnection connection = null;
		
		Map<String, List<DatabaseConnection>> appsMap = connectionsDao.get(organization.getRaw(Property_hasOrganizationID.class));
		
		if(appsMap != null) {
			
			List<DatabaseConnection> list = appsMap.get(app.getRaw(Property_hasAppID.class));
			
			if(list != null) {

				for(DatabaseConnection c : list) {
					
					String p = c.get(Property_hasName.class).toString();
					
					if(databaseName.equals(p)) {
						connection = c;
						break;
					}
					
				}
				
			}
		}
		
		if(connection == null) {
			throw new RuntimeException("DatabaseConnection with name " + databaseName + " not found");
		}
		
		IProperty typeProp = (IProperty) connection.get(Property_hasEndpointType.class);
		
		if(query instanceof VitalExternalSparqlQuery) {
			
			SparqlEndpointType et = SparqlEndpointType.fromString(typeProp.toString());
			
			if(et == null) throw new RuntimeException("Unknown sparql endpoint type: " + typeProp.toString() + ", supported: " + SparqlEndpointType.getSupported());
			
			VitalExternalSparqlQuery sq = (VitalExternalSparqlQuery) query;
			
			ExternalSparqlDatabase exd = externalSparqlDatabasesImplementations.get(et);
			
			if(exd == null) throw new RuntimeException("No implementation for sparql endpoint type: " + et.name());
			
			return exd.query((SparqlDatabaseConnection) connection, sq);

		} else if(query instanceof VitalExternalSqlQuery) {
			
			SqlEndpointType et = SqlEndpointType.fromString(typeProp.toString());
			
			if(et == null) throw new RuntimeException("Unknown sql endpoint type: " + typeProp.toString() + ", supported: " + SqlEndpointType.getSupported());
			
			VitalExternalSqlQuery sq = (VitalExternalSqlQuery) query;

			if(!implementedSqlEndpoints.contains(et)) {
				throw new RuntimeException("No implementation for sql endpoint: " + et.name());
			}
			
			ExternalSqlDatabase exd = externalSqlDatabasesImplementations.get(et);
			
			if(exd == null) throw new RuntimeException("SQL databases disabled, no vital-sql module");
			
			return exd.query((SqlDatabaseConnection) connection, sq);
			
		} else {
			throw new RuntimeException("Unsupported query " + query.getClass().getCanonicalName());
		}
		
	}

}
