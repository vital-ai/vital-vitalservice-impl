package ai.vital.sql.service;

import java.sql.SQLException;

import ai.vital.sql.VitalSqlImplementation;
import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.schemas.SchemasUtils;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.factory.VitalServiceInitWrapper;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;

public class SqlVitalServiceInitWrapper implements VitalServiceInitWrapper {

	private VitalSqlImplementation sqlImpl;

	private VitalServiceSqlConfig vssc;
	
	public SqlVitalServiceInitWrapper(VitalServiceSqlConfig dbConfig) {
		this.vssc = dbConfig;
	}

	@Override
	public SystemSegmentOperationsExecutor createExecutor() {
		return new VitalSqlSystemSegmentExecutor(sqlImpl);
	}

	@Override
	public VitalStatus isInitialized() {
		
		sqlImpl = new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig(vssc)));
		
		try {
			sqlImpl.ping();
		} catch (SQLException e) {
			return VitalStatus.withError("SQL endpoint ping failed: " + e.getLocalizedMessage());
		}
		
		return VitalStatus.withOK();
//		executor = new VitalSqlSystemSegmentExecutor(sqlImpl);
	}

	@Override
	public void initialize() {

		sqlImpl = new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig(vssc)));

//		sqlImpl = new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig((VitalServiceSqlConfig) endpointConfig)));
//		executor = new VitalSqlSystemSegmentExecutor(sqlImpl);
		
	}

	@Override
	public void close() {

		try {
			sqlImpl.close();
		} catch(Exception e) {}
		
	}

	@Override
	public void destroy() throws VitalServiceException {

		VitalSqlImplementation sqlImpl = new VitalSqlImplementation(new VitalSqlDataSource(VitalServiceSql.toInnerConfig(vssc)));
		
		try {
			SchemasUtils.deleteSegmentTables(sqlImpl.getDataSource());
		} catch (SQLException e) {
			throw new VitalServiceException(e.getLocalizedMessage());
		} finally {
			try {
				sqlImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

}
