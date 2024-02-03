package ai.vital.vitalservice.impl

import ai.vital.lucene.model.LuceneSegment
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.query.ResultList;

class LocalSegmentsQueryFilter {

	public static String checkIfLocalQuery(List<VitalSegment> segments) throws VitalServiceException {
		
		if(segments == null) return
		
		Map<String, LuceneSegment> ns2Segments = VitalSigns.get().getOntologyURI2Segment()
		
		
		int localSegments = 0;
		int otherSegments = 0;
		String localSegment = null;
		for(VitalSegment s : segments) {
			if(ns2Segments.containsKey(s.getID()) || VitalSigns.CACHE_DOMAIN.equals(s.getID())) {
				localSegment = s.getID();
				localSegments++;
			} else {
				otherSegments++;
			}
		}
		
		if(localSegments > 1) {
			throw new VitalServiceException("Cannot query more than 1 domain individuals segments at once")
		} else if(localSegments == 1) {
			if(otherSegments > 0) throw new VitalServiceException("Cannot mix domain individuals and regular segments in queries")
			return localSegment
		}
		
		return null
		
	}
	
}
