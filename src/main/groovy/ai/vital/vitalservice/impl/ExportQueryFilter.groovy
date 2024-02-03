package ai.vital.vitalservice.impl

import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.query.VitalExportQuery
import ai.vital.vitalservice.query.VitalSelectQuery

/**
 * A filter that checks if a select query is a special export query and verifies its constraints:
 * - single segment
 * - offset, limit
 * - no query components
 * - no sort properties
 * 
 *
 */
class ExportQueryFilter {

	public static void checkExportQuery(VitalSelectQuery sq) throws VitalServiceException {
		
		if(!( sq instanceof VitalExportQuery)) return
		
		if(sq.segments.size() != 1) throw new VitalServiceException("Exactly 1 segment expected in VitalExportQuery object")
		
		if(sq.offset == null || sq.offset < 0) throw new VitalServiceException("Offset cannot be null or less than 0")
		if(sq.limit == null || sq.limit <= 0) throw new VitalServiceException("Limit cannot be null or less than 1")
		if(sq.sortProperties.size() > 0) throw new VitalServiceException("No query sort properties allowed in VitalExportQuery")
		
		if( sq.topContainer != null ) throw new VitalServiceException("Export query must not have top arc container!")
		
	}
	
}
