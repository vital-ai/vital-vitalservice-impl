package ai.vital.vitalservice.impl.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.query.Connector;
import ai.vital.vitalservice.query.Destination;
import ai.vital.vitalservice.query.GraphElement;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.Source;
import ai.vital.vitalservice.query.VitalGraphArcContainer;
import ai.vital.vitalservice.query.VitalGraphArcElement;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.classes.ClassMetadata;
import ai.vital.vitalsigns.meta.PathElement;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_TaxonomyEdge;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.property.URIProperty;

public class PathQueryHelperImpl {

	public static VitalPathQuery getDefaultExpandQuery(List<VitalSegment> segments, VitalSelectQuery rootselector) throws Exception {
		return getDefaultExpandQuery(null, segments, rootselector);
	}
	
	public static VitalPathQuery getDefaultExpandQuery(List<VitalSegment> segments, List<GraphObject> rootObjects) throws Exception {
		return getDefaultExpandQuery(null, segments, rootObjects);
	}
	
	public static VitalPathQuery getDefaultExpandQuery(VitalApp app, List<VitalSegment> segments, VitalSelectQuery rootselector) throws Exception {
		
		List<GraphObject> objects = new ArrayList<GraphObject>();
		
		VitalService vitalService = VitalSigns.get().getVitalService();
		
		VitalServiceAdmin vitalServiceAdmin = VitalSigns.get().getVitalServiceAdmin();
		
		if( vitalService != null ) {
			
			ResultList query = vitalService.query(rootselector);
			
			for(GraphObject g : query) {
				
				objects.add(g);
				
			}
			
		}  else if( vitalServiceAdmin != null ) {
			
			if(app == null) throw new RuntimeException("No app provided, required for vitalservice admin");
			
			ResultList query = vitalServiceAdmin.query(app, rootselector);
			
			for(GraphObject g : query) {
				
				objects.add(g);
				
			}
			
		} else {
			throw new RuntimeException("No vital service instance active");
		}
		
		return getDefaultExpandQuery(app, segments, objects);
		
		
	}
	
	public static VitalPathQuery getDefaultExpandQuery(VitalApp app, List<VitalSegment> segments, List<GraphObject> objects) throws Exception {
		
		if(objects == null || objects.size() < 1) throw new RuntimeException("Null or empty objects list");
		
		Set<Class<? extends GraphObject>> classes = new HashSet<Class<? extends GraphObject>>();
		
		//collect base classes
		List<URIProperty> rootURIs = new ArrayList<URIProperty>();
		
		for(GraphObject g : objects) {
			
			classes.add(g.getClass());
			rootURIs.add(URIProperty.withString(g.getURI()));
			
		}
		
		
		VitalPathQuery vpq = new VitalPathQuery();
		vpq.setRootURIs(rootURIs);
		
		List<VitalGraphArcContainer> arcs = new ArrayList<VitalGraphArcContainer>();
		vpq.setMaxdepth(0);
		vpq.setArcs(arcs);
		vpq.setSegments(segments);
		
		
		Map<String, PathElement> uniquePathElements = new HashMap<String, PathElement>();
		
		for(Class<? extends GraphObject> cls : classes) {
			
			List<List<PathElement>> paths = VitalSigns.get().getClassesRegistry().getPaths(cls, true);
			
			//convert each path element into arc
			for(List<PathElement> path : paths) {
				
				boolean valid = true;
				
				for(PathElement pe : path) {
					
					if( pe.isHyperedge() || pe.isReversed() ) {
						valid = false;
						break;
					}
					
					String edgeTypeURI = pe.getEdgeTypeURI();
					
					ClassMetadata cm = VitalSigns.get().getClassesRegistry().getClass(edgeTypeURI);
					
					if(cm == null) throw new RuntimeException("Edge class not found for URI: " + edgeTypeURI);
					
					Class<? extends GraphObject> clazz = cm.getClazz();
					
					if(!VITAL_TaxonomyEdge.class.isAssignableFrom(clazz)) {
						valid = false;
					}
					
				}
				
				if(!valid) continue;
				
				for(PathElement pe : path) {
					
					String sig = pathSignature(pe);
					
					if(!uniquePathElements.containsKey(sig)) uniquePathElements.put(sig, pe);
					
				}
				
			}
			
		}
		
		if(uniquePathElements.size() < 1) throw new RuntimeException("No forward path elements found for the following classes: " + classes);
		
		for(PathElement pe : uniquePathElements.values() ) {
		
			//forward
			VitalGraphArcContainer arc = new VitalGraphArcContainer(QueryContainerType.and, new VitalGraphArcElement(Source.PARENT_SOURCE, Connector.EDGE, Destination.CURRENT));
			VitalGraphCriteriaContainer cc = new VitalGraphCriteriaContainer(QueryContainerType.and);
			
			ClassMetadata edgeCM = VitalSigns.get().getClassesRegistry().getClass(pe.getEdgeTypeURI());
			if(edgeCM == null) throw new RuntimeException("Edge class not found for URI: " + pe.getEdgeTypeURI());
			
			ClassMetadata nodeCM = VitalSigns.get().getClassesRegistry().getClass(pe.getDestNodeTypeURI());
			if(nodeCM == null) throw new RuntimeException("Node class not found for URI: " + pe.getDestNodeTypeURI());
			
			cc.add(new VitalGraphQueryTypeCriterion(GraphElement.Connector, edgeCM.getClazz()));
			cc.add(new VitalGraphQueryTypeCriterion(GraphElement.Destination, nodeCM.getClazz()));
			
			arc.add(cc);
			arcs.add(arc);
			
		}
		
		return vpq;
		
		
	}
	
	static String pathSignature(PathElement pe) {
		return pe.isHyperedge() + "__" + pe.isReversed() + "__" + pe.getEdgeClass() + "__" + pe.getDestNodeTypeURI();
	}
	
}
