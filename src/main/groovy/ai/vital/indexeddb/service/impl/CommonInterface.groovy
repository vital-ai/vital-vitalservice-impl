package ai.vital.indexeddb.service.impl

import java.util.List;

import ai.vital.lucene.exception.LuceneException;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.SystemSegment.SystemSegmentOperationsExecutor;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalOrganization
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalTransaction;


/**
 * Common interface that needs to be implemented by both index and db
 *
 */
interface CommonInterface {

	public GraphObject save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, GraphObject graphObject, ResultList segmentsRL) throws Exception
	
	public ResultList save(VitalTransaction transaction, VitalOrganization organization, VitalApp app, VitalSegment targetSegment, List<GraphObject> graphObjectsList, ResultList segmentsRL) throws Exception
	
	
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, URIProperty uri, ResultList segmentsRL) throws Exception 
		
	public VitalStatus delete(VitalTransaction transaction, VitalOrganization organization, VitalApp app, List<URIProperty> uris, ResultList segmentsRL) throws Exception
	
	
	public VitalStatus ping() throws Exception
	
	public void close() throws Exception
	
	
	public int getSegmentSize(VitalOrganization organization, VitalApp app, VitalSegment segment, ResultList segmentsRL) throws Exception;
	
	public boolean supportsSelectQuery(VitalOrganization organization, VitalApp app, VitalSelectQuery sq) throws Exception;
	
	public boolean supportsGraphQuery(VitalOrganization organization, VitalApp app, VitalGraphQuery gq) throws Exception;
	
	public ResultList selectQuery(VitalOrganization organization, VitalApp app, VitalSelectQuery query, ResultList segmentsRL) throws Exception
	
	public ResultList graphQuery(VitalOrganization organization, VitalApp app, VitalGraphQuery query, ResultList segmentsRL) throws Exception
	
	public void deleteAll(VitalOrganization organization, VitalApp app, VitalSegment segment, ResultList segmentsRL) throws Exception;
	
	public SystemSegmentOperationsExecutor getSystemSegmentExecutor()
}
