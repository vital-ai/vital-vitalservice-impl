package ai.vital.vitalservice.dbconnection.impl;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATALINK;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DISTINCT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NULL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.REF;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.STRUCT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.io.StringReader;
import java.sql.Connection;
//import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.dbconnection.ExternalSqlDatabase;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExternalSqlQuery;
import ai.vital.vitalsigns.model.SqlDatabaseConnection;
import ai.vital.vitalsigns.model.SqlResultRow;
import ai.vital.vitalsigns.model.SqlUpdateResponse;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.properties.Property_hasDatabase;
import ai.vital.vitalsigns.model.properties.Property_hasEndpointURL;
import ai.vital.vitalsigns.model.properties.Property_hasPassword;
import ai.vital.vitalsigns.model.properties.Property_hasUpdatedRowsCount;
import ai.vital.vitalsigns.model.properties.Property_hasUsername;
import ai.vital.vitalsigns.model.property.BooleanProperty;
import ai.vital.vitalsigns.model.property.DateProperty;
import ai.vital.vitalsigns.model.property.DoubleProperty;
import ai.vital.vitalsigns.model.property.FloatProperty;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.IntegerProperty;
import ai.vital.vitalsigns.model.property.LongProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

@SuppressWarnings("unchecked")
public abstract class JDBCSQLDatabase extends ExternalSqlDatabase {

	protected static int defaultTimeoutSeconds = 30;
	
	private final static Logger log = LoggerFactory.getLogger(JDBCSQLDatabase.class);
	
	private final static int MAX_ACTIVE_DATASOURCES = 5;
	
	private static final int CONN_POOL_SIZE = 3;
	
	//lru map that keeps a population of basic data sources
//	Map<String, BasicDataSource>
	protected static Map<String, BasicDataSource> dataSources = null;
	
	static  {
		LRUMap lruMap = new LRUMap(MAX_ACTIVE_DATASOURCES){
			
			private static final long serialVersionUID = 1L;

			@Override
			protected boolean removeLRU(LinkEntry entry) {

				String removedEndpoint = (String) entry.getKey();
				log.info("Endpoint evicted: " + removedEndpoint);
				
				BasicDataSource bds = (BasicDataSource) entry.getValue();
				
				try {
					bds.close();
				} catch(Exception e) {
					log.error(e.getLocalizedMessage());
				}
				
				return true;
			}
			
			
		};
		
		dataSources = Collections.synchronizedMap(lruMap);
	}
	
	
	CCJSqlParserManager parserManager = new CCJSqlParserManager();
	
	public JDBCSQLDatabase() {
		
	
	}
	
	@Override
	public void validateConfig(SqlDatabaseConnection connection) {
	}

	
	protected Connection obtainConnection(SqlDatabaseConnection cfg) throws Exception {
		BasicDataSource bds = initDataSource(cfg);
		return bds.getConnection();
	}
	
	@Override
	public ResultList query(SqlDatabaseConnection cfg,
			VitalExternalSqlQuery sq) {

		String sql = sq.getSql();
		if(sql == null || sql.isEmpty()) throw new RuntimeException("No input sql");
		
		long timeout = timeout(sq);
		
		sql = sql.trim();
		
		ResultList response = null;
		
		Statement parsed = null;
		try {
			parsed = parserManager.parse(new StringReader(sql));
		} catch (JSQLParserException e1) {
			response = new ResultList();
			response.setStatus(VitalStatus.withError("SQL parse exception: " + e1.getLocalizedMessage()));
			return response;
		}
		
		
		ExecutorService pool = Executors.newSingleThreadExecutor();

		Future<ResultList> future = null;
		
		QueryCallable qc = null;
		
		try {
			
			qc = new QueryCallable(sql, parsed, cfg);
			
			future = pool.submit(qc);
			response = future.get(timeout, TimeUnit.SECONDS);
			
		} catch(TimeoutException e) {

			log.warn("Query timed out: ", sql);
			
		} catch(Exception e) {
			
			response = new ResultList();
			response.setStatus(VitalStatus.withError("Query exception: " + e.getLocalizedMessage()));
			
			if(future != null) {
				try { future.cancel(true); } catch(Exception ex) {}
			}
			
		} finally {
			
			qc.shutdown();
			
			pool.shutdownNow();
			
		}
		
		if(response == null) {
			response = new ResultList();
			response.setStatus(VitalStatus.withError("Query timed out."));
		}
		
		return response;
	}

	protected long timeout(VitalExternalSqlQuery sq) {

		Integer timeout = sq.getTimeout();
		if(timeout != null) {
			if(timeout.intValue() < 1) {
				throw new RuntimeException("Query timeout seconds must be greater than 0 (" + timeout + ")");
			}
		} else {
			timeout = defaultTimeoutSeconds;
		}
		
		return timeout.longValue();
	}

	protected SqlResultRow toSqlResultRow(ResultSet rs) throws SQLException {
		
		SqlResultRow row = new SqlResultRow();
		row.generateURI((VitalApp)null);

		ResultSetMetaData md = rs.getMetaData();
		
		for(int i = 1; i <= md.getColumnCount(); i++ ) {
			
			String n = md.getColumnLabel(i);
			
			int ctype = md.getColumnType(i);
			
//			JDBCType t = JDBCType.valueOf(ctype);
			
			IProperty val = null;
			
			try {
				
			switch(ctype) {
			
			case ARRAY: {}
			case BIGINT: { val = new LongProperty(rs.getLong(i)); break; }
			case BINARY: {}
			case BIT: { val = new BooleanProperty(rs.getBoolean(i)); break; }
			case BLOB: {}
			case BOOLEAN: { val = new BooleanProperty(rs.getBoolean(i)); break; }
			case CHAR: { val = new StringProperty(rs.getString(i)); break; }
			case CLOB: {}
			case DATE: { val = new DateProperty(rs.getDate(i)); break; }
			case DATALINK: {}
			case DECIMAL: { val = new DoubleProperty(rs.getDouble(i)); break; }
			case DISTINCT: {}
			case DOUBLE: { val = new DoubleProperty(rs.getDouble(i)); break; }
			case FLOAT: { val = new FloatProperty(rs.getFloat(i)); break; }
			case INTEGER: { val = new IntegerProperty(rs.getInt(i)); break; }
			case JAVA_OBJECT: {}
			case LONGNVARCHAR: { val = new StringProperty(rs.getString(i)); break; }
			case LONGVARBINARY: {}
			case LONGVARCHAR: { val = new StringProperty(rs.getString(i)); break; }
			case NCHAR: { val = new StringProperty(rs.getString(i)); break; }
			case NCLOB: {}
			case NULL: { val = null; break; }
			case NUMERIC: { val = new DoubleProperty(rs.getDouble(i)); break; }
			case NVARCHAR: { val = new StringProperty(rs.getString(i)); break; }
			case OTHER: {}
			case REAL: { val = new DoubleProperty(rs.getDouble(i)); break; }
			case REF: {}
//			case REF_CURSOR: {}
			case ROWID: { val = new LongProperty(rs.getLong(i)); break; }
			case SMALLINT: { val = new IntegerProperty(rs.getInt(i)); break; }
			case SQLXML: {}
			case STRUCT: {}
			case TIME: { val = new DateProperty(rs.getTime(i).getTime()); break; }
//			case TIME_WITH_TIMEZONE: { val = new DateProperty(rs.getTime(i).getTime()); break; }
			case TIMESTAMP: { val = new DateProperty(rs.getTimestamp(i).getTime()); break; }
//			case TIMESTAMP_WITH_TIMEZONE: { val = new DateProperty(rs.getTimestamp(i).getTime()); break; }
			case TINYINT: { val = new IntegerProperty(rs.getInt(i)); break; }
			case VARBINARY: {}
			case VARCHAR: { val = new StringProperty(rs.getString(i)); break; }
			default: {
				throw new SQLException("Unhandled sql type: " + ctype);//t.getName()); 
			}
			}
			
			} catch(NullPointerException npe) {
			}

			if(val != null){
				row.setProperty(n, val);
			}
			
		}
		
		return row;
	}

	private BasicDataSource initDataSource(SqlDatabaseConnection config) {

		IProperty endpointProp = (IProperty) config.get(Property_hasEndpointURL.class);
		if(endpointProp == null) throw new RuntimeException("No endpointURL property");
		
		IProperty databaseP = (IProperty)config.get(Property_hasDatabase.class);
		if(databaseP == null) throw new RuntimeException("No database property");
		
		String endpointDatabaseURL = endpointProp.toString();
		
		if(!endpointDatabaseURL.endsWith("/")) endpointDatabaseURL += "/";
		
		endpointDatabaseURL += databaseP.toString(); 
		
		BasicDataSource bds = dataSources.get(endpointDatabaseURL);
		
		if(bds != null) return bds;
		
		bds = new BasicDataSource();
		
		IProperty usernameP = (IProperty)config.get(Property_hasUsername.class);
		String username = usernameP != null ? usernameP.toString() : null;
		if(username == null) throw new RuntimeException("No username property");
		
		IProperty passwordP = (IProperty)config.get(Property_hasPassword.class);
		String password = passwordP != null ? passwordP.toString() : null;
		if(password == null) throw new RuntimeException("No password property");
		
		
        //Set database driver name
        bds.setDriverClassName(getDriverClassName());

        bds.setUrl(endpointDatabaseURL);

        bds.setUsername(username);
        
        bds.setPassword(password);
        
        bds.setInitialSize(1);
		
        bds.setMaxTotal(CONN_POOL_SIZE);
        
		dataSources.put(endpointDatabaseURL, bds);
        
		return bds;
		
	}

	protected abstract String getDriverClassName();

	
	class QueryCallable implements Callable<ResultList> {
	
		String sql;

		Statement parsed;
		
		private SqlDatabaseConnection cfg;
		
		public QueryCallable(String sql, Statement parsed, SqlDatabaseConnection cfg) {
			super();
			this.sql = sql;
			this.parsed = parsed;
			this.cfg = cfg;
			
		}
		
		
		Connection connection;
		
		PreparedStatement stmt = null;
		
		ResultSet rs = null;

		
		
		@Override
		public ResultList call() throws Exception {

			ResultList rl = new ResultList();
			
			try {
				
				connection = obtainConnection(cfg);
				
				stmt = connection.prepareStatement(sql);
				
				if(parsed instanceof Select) {
					
					rs = stmt.executeQuery();
					
					while(rs.next()) {
						
						SqlResultRow row = toSqlResultRow(rs); 
						
						rl.getResults().add(new ResultElement(row, 1D));
						
					}
					
				} else if(
						parsed instanceof Insert || 
						parsed instanceof Update ||
						parsed instanceof Delete
						) {
					
					int affectedRowsCount = stmt.executeUpdate();
					
					SqlUpdateResponse sup = new SqlUpdateResponse();
					sup.generateURI((VitalApp) null);
					sup.set(Property_hasUpdatedRowsCount.class, affectedRowsCount);
					
					rl.getResults().add(new ResultElement(sup, 1D));
					
				} else {
					
					throw new Exception("Unsupported sql command, only select, insert, update, delete supported");
					
				}
				
			} finally {

				cleanup();
			}
			
			return rl;
		}
		
		public void shutdown() {
			cleanup();
		}
		
		protected void cleanup() {
			
			try {
				if(rs != null) {
					rs.close();
				}
			} catch(Exception e) {}
			rs = null;
			try {
				if(stmt != null) stmt.close();
			} catch(Exception e) {}
			stmt = null;
			try {
				if(connection != null) connection.close();
			} catch (Exception e) {}
			connection = null;
			
		}
		
	}
	
}
