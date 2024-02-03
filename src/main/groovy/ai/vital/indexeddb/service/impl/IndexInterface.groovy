package ai.vital.indexeddb.service.impl

import java.io.IOException;
import java.util.List;

import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalOrganization
import ai.vital.vitalsigns.model.VitalSegment

// interface for index implementations

interface IndexInterface extends CommonInterface {
	
	VitalStatus initialize(VitalServiceConfig  config) throws IOException
	
	// basic crud operations for graph objects
	
	// ideally excluding Organization, App, Segment since those are not indexed
	// however the lucene disk implementation organizes the index by organization/app/segment
	// so the index implementation will match this structure and thus needs
	// crud operations for Organization, App, Segment that are synced to the ones in the 
	// database.  this aligns a segment stored in the database with an index for that segment.
	
	
	// query operations
	// select
	// graph/path
	
	public void synchronizeTransaction(TransactionWrapper transactionWrapper, ResultList segmentsRL) throws Exception

	public void insertNoCheck(VitalOrganization organization, VitalApp app, VitalSegment segment, List<GraphObject> objects, ResultList listSegments);
	
}
