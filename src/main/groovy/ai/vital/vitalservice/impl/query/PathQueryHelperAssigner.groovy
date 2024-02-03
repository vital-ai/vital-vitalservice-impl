package ai.vital.vitalservice.impl.query

import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.VitalApp

class PathQueryHelperAssigner {

	private static boolean assigned = false
	
	public static assingHelper() {
		
		if(assigned) return
		
		VitalPathQuery.metaClass.static."getDefaultExpandQuery" = { Object... args ->
			
			if(args.length == 2) {
				
				if(!(args[0] instanceof List)) {
					throw new RuntimeException("when 2 args given, the first argument must be a list of VitalSegments")
				}
				
				if(!(args[1] instanceof VitalSelectQuery || args[1] instanceof List)) {
					throw new RuntimeException("when 2 args given, the second argument must be either a list of GraphObjects or VitalSelectQuery (root)")
				}
				
				return PathQueryHelperImpl.getDefaultExpandQuery(args[0], args[1])
				
			} else if(args.length == 3) {
			
				if(!( args[0] instanceof VitalApp) ) {
					throw new RuntimeException("when 3 args given, the first argument must be an optional App")
				}
				
				if(!(args[1] instanceof List)) {
					throw new RuntimeException("when 3 args given, the second argument must be a list of VitalSegments")
				}
				
				if(!(args[2] instanceof VitalSelectQuery || args[2] instanceof List)) {
					throw new RuntimeException("when 3 args given, the second argument must be either a list of GraphObjects or VitalSelectQuery (root)")
				}
				
				return PathQueryHelperImpl.getDefaultExpandQuery(args[0], args[1], args[2])
			
			} else {
			
				throw new RuntimeException("getDefaultExpandQuery expects either 2 or 3 arguments")
			
			}
			
			
		}
		
	}
	
}
