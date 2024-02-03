package ai.vital.vitalservice.impl.query

import ai.vital.vitalsigns.model.GraphObject;

class GraphObjectSaveAssigner {

	static boolean initialized = false
	
	public static void assingSave() {
		
		if(initialized) return
		
		GraphObject.metaClass."save" = { Object... args ->
			return GraphObjectSaveImpl.save(delegate, args)
		}
		
		initialized = true;
		
	}
	
}
