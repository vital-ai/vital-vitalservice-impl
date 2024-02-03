package ai.vital.vitalservice.impl.query

import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;

class ToSparqlAssigner {

	static boolean initialized = false
	
	public static void assignToSparql() {
		
		if(initialized) return;
		
		VitalQuery.metaClass."toSparql" = { Object... args ->
			return ToSparqlImplementation.toSparql(delegate, args);
		}
		
		VitalSelectQuery.metaClass."toSparql" = { Object... args ->
			return ToSparqlImplementation.toSparql(delegate, args);
		}
		
		VitalSelectAggregationQuery.metaClass."toSparql" = { Object... args ->
			return ToSparqlImplementation.toSparql(delegate, args);
		}
		
		VitalPathQuery.metaClass."toSparql" = { Object... args ->
			throw new RuntimeException("Path query does not use sparql")
		}
		
		VitalExportQuery.metaClass."toSparql" = { Object... args ->
			return ToSparqlImplementation.toSparql(delegate, args);
		}
		
		VitalGraphQuery.metaClass."toSparql" = { Object... args ->
			return ToSparqlImplementation.toSparql(delegate, args);
		}
		
		
		initialized = true
		
	}
	
}
