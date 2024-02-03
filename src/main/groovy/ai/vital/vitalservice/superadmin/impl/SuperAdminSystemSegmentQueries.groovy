package ai.vital.vitalservice.superadmin.impl

import ai.vital.vitalservice.impl.SystemSegmentQueries;
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalsigns.model.Edge_hasAuthKey;
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceRootKey
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.query.URI
import ai.vital.superadmin.domain.VitalServiceSuperAdminKey;

class SuperAdminSystemSegmentQueries extends SystemSegmentQueries {
	
	public static VitalGraphQuery getSuperAdminKeysQuery(VitalSegment systemSegment, VitalServiceRootKey rootKey) {
		
	return builder.query {
		
		GRAPH {
			
			value inlineObjects: true
			
			value segments: [systemSegment]
			
			ARC {
			
				node_constraint { VitalServiceRootKey.class }
			
				node_constraint { URI.equalTo( URIProperty.withString(rootKey.getURI()) ) }
				
				
				ARC {
					
					edge_constraint { Edge_hasAuthKey.class }
					
					node_constraint { VitalServiceSuperAdminKey.class }
					
				}
				
			}
					
		}
		
	}.toQuery()
	
}

}
