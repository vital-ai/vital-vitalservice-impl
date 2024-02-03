package ai.vital.vitalservice.impl.query;

import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.ValidationStatus;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.property.URIProperty;

public class GraphObjectSaveImpl {

	public static GraphObject save(GraphObject graphObject, Object... args) throws Exception {
		
		ValidationStatus vs = graphObject.validate();
		if(vs.getStatus() != ValidationStatus.Status.ok) {
			throw new RuntimeException("Graph object validation failed: " + vs.getErrors());
		}
		
		VitalService vitalService = VitalSigns.get().getVitalService();
		
		VitalServiceAdmin vitalServiceAdmin = VitalSigns.get().getVitalServiceAdmin();
		
		if(vitalService != null) {
			
			String defaultSegmentName = vitalService.getDefaultSegmentName();
			if(defaultSegmentName == null || defaultSegmentName.isEmpty()) throw new RuntimeException("Default segment name not set!");
			
			ResultList rl = vitalService.get(GraphContext.ServiceWide, URIProperty.withString(graphObject.getURI()));
			
			if(rl.getStatus().getStatus() != VitalStatus.Status.ok) {
				throw new RuntimeException("Error when checking if current object exists: " + graphObject.getURI() + " - " + rl.getStatus().getMessage());
			}
			
			GraphObject first = rl.first();
			
			if(first != null) {
				ResultList saveRS = vitalService.save(graphObject);
				if(saveRS.getStatus().getStatus() != VitalStatus.Status.ok) {
					throw new RuntimeException("Error when updating current object: " + graphObject);
				}
				
				graphObject = saveRS.first();
				
				if(graphObject == null) throw new RuntimeException("Object wasn't saved: " + saveRS.getStatus().getMessage());
				
			} else {
				
				VitalSegment segment = vitalService.getSegment(defaultSegmentName);
				if(segment == null) throw new RuntimeException("Default segment '" + defaultSegmentName + "' not found");
				
				ResultList insertRS = vitalService.insert(segment, graphObject);
				
				if(insertRS.getStatus().getStatus() != VitalStatus.Status.ok) {
					throw new RuntimeException("Error when updating current object: " + graphObject + " - " + insertRS.getStatus().getMessage());
				}
				
				graphObject = insertRS.first();
				
				if(graphObject == null) throw new RuntimeException("Object wasn't saved: " + insertRS.getStatus().getMessage());
				
			}
			
			
		} else if(vitalServiceAdmin != null) {
			
			throw new RuntimeException("Cannot use GraphObject.save() when using admin service is active");
			
		} else {
			throw new RuntimeException("No active vital service");
		}
		
		return graphObject;
	}
	
}
