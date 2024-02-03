package ai.vital.vitalservice.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.factory.VitalServiceFactory;
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
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.meta.EdgesResolver;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.vitalsigns.model.VITAL_Edge;
import ai.vital.vitalsigns.model.VITAL_Edge_PropertiesHelper;
import ai.vital.vitalsigns.model.VITAL_HyperEdge;
import ai.vital.vitalsigns.model.VITAL_HyperEdge_PropertiesHelper;
import ai.vital.vitalsigns.model.VITAL_Node;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalSegment;

public class ServiceWideEdgesResolver implements EdgesResolver {

	@Override
	public List<GraphObject> getDestGraphObjectsForSrcURI(String srcUri,
			Class<? extends VITAL_HyperEdge> hyperEdgeType, VITAL_Container... containers) {
		return getEndpointGraphObjectsForGivenURI(srcUri, true, hyperEdgeType, containers);
	}

	private List<GraphObject> getEndpointGraphObjectsForGivenURI(String srcUri,
			boolean forward, Class<? extends VITAL_HyperEdge> hyperEdgeType,
			VITAL_Container[] containers) {
		if(srcUri == null) throw new NullPointerException("Null source URI");
		if(hyperEdgeType == null) throw new NullPointerException("Hyper edge type must not be null");
		containersCheck(containers);
		
//		VitalPathQuery vpq = new VitalPathQuery();
//		vpq.setMaxdepth(1);
//		vpq.setRootURIs(Arrays.asList(URIProperty.withString(arg0)));
//		VitalGraphArcContainer c = new VitalGraphArcContainer(QueryContainerType.and, new VitalGraphArcElement(Source.PARENT_SOURCE, Connector.HYPEREDGE, Destination.));
//		vpq.setArcs(Arrays.asList(a));
//		return null;
		
		throw new RuntimeException("servicewide hyperedges access not supported yet!");
	}

	private void containersCheck(VITAL_Container[] containers) {
		if(containers != null && containers.length > 0) throw new RuntimeException("ServiceWide edges resolver does not support external containers");
		
	}

	@Override
	public List<VITAL_Node> getDestNodesForSrcURI(String srcUri,
			Class<? extends VITAL_Edge> edgeType, VITAL_Container... containers) {
		return getEndpointNodesForGivenURI(srcUri, true, edgeType, containers);

	}

	private class ServiceContext {
		
		VitalService service;
		
		VitalServiceAdmin serviceAdmin;
		
		VitalApp app;
		
		public ServiceContext() {

			service = VitalSigns.get().getVitalService();
			
			if(service == null) {
				
				serviceAdmin = VitalSigns.get().getVitalServiceAdmin();
				if(serviceAdmin == null) throw new RuntimeException("No active service nor adminservice service in vitalsigns");
				app = VitalSigns.get().getCurrentApp();
				
				if(app == null) throw new RuntimeException("No current app set in vitalsigns");
				
			}
			
		}
		
		ResultList query(VitalQuery query) throws VitalServiceUnimplementedException, VitalServiceException {
			
			if(service != null) {
				return service.query(query);
			} else {
				return serviceAdmin.query(app, query);
			}
			
		}
		
		List<VitalSegment> listSegments() throws VitalServiceUnimplementedException, VitalServiceException {
			if(service != null) {
				return service.listSegments();
			} else {
				return serviceAdmin.listSegments(app);
			}
		}
		
	}
	
	private List<VITAL_Node> getEndpointNodesForGivenURI(String srcUri,
			boolean forward, Class<? extends VITAL_Edge> edgeType,
			VITAL_Container[] containers) {
		
		if(srcUri == null) throw new NullPointerException("Null source URI");
		if(edgeType == null) throw new NullPointerException("Null edge type");
		containersCheck(containers);
		
		//excute a simple path query
		ServiceContext ctx = new ServiceContext();
		
		VitalPathQuery vpq = new VitalPathQuery();
		vpq.setRootURIs(Arrays.asList(URIProperty.withString(srcUri)));
		
		VitalGraphArcElement el = new VitalGraphArcElement(forward ? Source.PARENT_SOURCE : Source.CURRENT, Connector.EDGE, forward ? Destination.CURRENT : Destination.PARENT_SOURCE);
		VitalGraphArcContainer arc = new VitalGraphArcContainer(QueryContainerType.and, el);
		VitalGraphCriteriaContainer cc = new VitalGraphCriteriaContainer(QueryContainerType.and);
		cc.add(new VitalGraphQueryTypeCriterion(GraphElement.Connector, edgeType));
		arc.add(cc);
		vpq.setArcs(Arrays.asList(arc));
		
		try {
			
			vpq.setSegments(ctx.listSegments());
			
			List<VITAL_Node> nodes = new ArrayList<VITAL_Node>();
			
			for(GraphObject g : ctx.query(vpq) ) {
				if(g instanceof VITAL_Node) {
					if(g.getURI().equals(srcUri)) continue;
					nodes.add((VITAL_Node) g);
				}
			}
			
			
			return nodes;
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}


	@Override
	public List<VITAL_Edge> getEdgesForDestURI(String destUri,
			VITAL_Container... containers) {
		return getEdgesForDestURI(destUri, null, false, containers);
	}

	@Override
	public List<VITAL_Edge> getEdgesForDestURI(String destUri,
			Class[] edgesFilter, boolean directClass,
			VITAL_Container... containers) {
		return getEdgesForSrcURIAndDestURIImpl( null, destUri, edgesFilter, directClass, containers);
	}

	private List<VITAL_Edge> getEdgesForSrcURIAndDestURIImpl(String srcUri,
			String destUri, Class[] edgesFilter, boolean directClass,
			VITAL_Container... containers) {
		
		List<VITAL_Edge> edges = new ArrayList<VITAL_Edge>();
		containersCheck(containers);
		
		
		ServiceContext ctx = new ServiceContext();
		
		VitalSelectQuery vsq = VitalSelectQuery.createInstance();
		try {
			vsq.setSegments(ctx.listSegments());
			vsq.setOffset(0);
			vsq.setLimit(10000);
			VitalGraphCriteriaContainer cc = vsq.getCriteriaContainer();
			cc.setType(QueryContainerType.and);
			
			if(srcUri != null) {
				cc.add(new VITAL_Edge_PropertiesHelper().getEdgeSource().equalTo(URIProperty.withString(srcUri)));
			}
			
			if(destUri != null) {
				cc.add(new VITAL_Edge_PropertiesHelper().getEdgeDestination().equalTo(URIProperty.withString(destUri)));
			}
			
			if(edgesFilter != null && edgesFilter.length > 0) {

				if(edgesFilter.length == 1) { 

					VitalGraphQueryTypeCriterion tc = new VitalGraphQueryTypeCriterion(edgesFilter[0]);
					if(directClass) {
						tc.setExpandTypes(false);
					} else {
						tc.setExpandTypes(true);
					}
					
					cc.add(tc);
					
				} else {
					throw new RuntimeException("Up to single edge filter expected!");
//					VitalGraphCriteriaContainer typesContainer = new VitalGraphCriteriaContainer(QueryContainerType.or);
				}
				
			}
			
			for( GraphObject o : ctx.query(vsq) ) {
				if(o instanceof VITAL_Edge) {
					edges.add((VITAL_Edge) o);
				}
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return edges;
		
		
		
		
	}

	@Override
	public List<VITAL_Edge> getEdgesForSrcURI(String srcUri,
			VITAL_Container... containers) {
		return getEdgesForSrcURI(srcUri, null, false, containers);
	}

	@Override
	public List<VITAL_Edge> getEdgesForSrcURI(String srcUri, Class[] edgesFilter, boolean directClass,
		VITAL_Container... containers) {
			return getEdgesForSrcURIAndDestURI(srcUri, null, edgesFilter, directClass, containers); 
	}	
		

	
	@Override
	public List<VITAL_Edge> getEdgesForSrcURIAndDestURI(String srcUri,
			String destUri, Class[] edgesFilter, boolean directClass,
			VITAL_Container... containers) {
		return getEdgesForSrcURIAndDestURIImpl( srcUri, destUri, edgesFilter, directClass, containers);
	}
	
	@Override
	public List<GraphObject> getEdgesWithNodesForSrcURI(String arg0,
			VITAL_Container... arg1) {
		throw new RuntimeException("NOT IMPLEMENTED!");
	}

	@Override
	public List<VITAL_HyperEdge> getHyperEdgesForDestURI(String destUri,
			VITAL_Container... containers) {
		return getHyperEdgesForDestURI(destUri, null, false, containers);
	}

	@Override
	public List<VITAL_HyperEdge> getHyperEdgesForDestURI(String destUri,
			Class[] hyperEdgesFilter, boolean directClass,
			VITAL_Container... containers) {
		return getHyperEdgesForSrcURIAndDestURIImpl( destUri, null, hyperEdgesFilter, directClass, containers);
	}
		
	
	private List<VITAL_HyperEdge> getHyperEdgesForSrcURIAndDestURIImpl(String srcUri,
			String destUri, Class[] hyperEdgesFilter, boolean directClass,
			VITAL_Container... containers) {
			
		List<VITAL_HyperEdge> hyperEdges = new ArrayList<VITAL_HyperEdge>();
		containersCheck(containers);
		
		
		ServiceContext ctx = new ServiceContext();
		
		VitalSelectQuery vsq = VitalSelectQuery.createInstance();
		try {
			vsq.setSegments(ctx.listSegments());
			vsq.setOffset(0);
			vsq.setLimit(10000);
			VitalGraphCriteriaContainer cc = vsq.getCriteriaContainer();
			cc.setType(QueryContainerType.and);
			
			if(srcUri != null) {
				cc.add(new VITAL_HyperEdge_PropertiesHelper().getHyperEdgeSource().equalTo(URIProperty.withString(srcUri)));
			}
			
			if(destUri != null) {
				cc.add(new VITAL_HyperEdge_PropertiesHelper().getHyperEdgeDestination().equalTo(URIProperty.withString(destUri)));
			}
			
			if(hyperEdgesFilter != null && hyperEdgesFilter.length > 0) {

				if(hyperEdgesFilter.length == 1) { 

					VitalGraphQueryTypeCriterion tc = new VitalGraphQueryTypeCriterion(hyperEdgesFilter[0]);
					if(directClass) {
						tc.setExpandTypes(false);
					} else {
						tc.setExpandTypes(true);
					}
					
					cc.add(tc);
					
				} else {
					throw new RuntimeException("Up to single edge filter expected!");
//					VitalGraphCriteriaContainer typesContainer = new VitalGraphCriteriaContainer(QueryContainerType.or);
				}
				
			}
			
			for( GraphObject o : ctx.query(vsq) ) {
				if(o instanceof VITAL_HyperEdge) {
					hyperEdges.add((VITAL_HyperEdge) o);
				}
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return hyperEdges;
		
		
	}

	

	@Override
	public List<VITAL_HyperEdge> getHyperEdgesForSrcURI(String srcUri,
		VITAL_Container... containers) {
		return getHyperEdgesForSrcURI(srcUri, null, false, containers);
	}

	@Override
	public List<VITAL_HyperEdge> getHyperEdgesForSrcURI(String srcUri, Class[] hyperEdgesFilter, boolean directClass,
		VITAL_Container... containers) {
		return getHyperEdgesForSrcURIAndDestURI(srcUri, null, hyperEdgesFilter, directClass, containers);
	}

	@Override
	public List<VITAL_HyperEdge> getHyperEdgesForSrcURIAndDestURI(String srcUri,
			String destUri, Class[] hyperEdgesFilter, boolean directClass,
			VITAL_Container... containers) {
		return getHyperEdgesForSrcURIAndDestURIImpl( srcUri, destUri, hyperEdgesFilter, directClass, containers);
	}

	@Override
	public List<GraphObject> getHyperEdgesWithGraphObjectsForSrcURI(
			String arg0, VITAL_Container... arg1) {
		throw new RuntimeException("NOT IMPLEMENTED!");
	}

	@Override
	public List<GraphObject> getSourceGraphObjectsForDestURI(String destUri,
			Class<? extends VITAL_HyperEdge> hyperEdgeType, VITAL_Container... containers) {
		if(destUri == null) throw new NullPointerException("Null destination URI");
		if(hyperEdgeType == null) throw new NullPointerException("Hyper edge type must not be null");
		containersCheck(containers);
		
//		VitalPathQuery vpq = new VitalPathQuery();
//		vpq.setMaxdepth(1);
//		vpq.setRootURIs(Arrays.asList(URIProperty.withString(arg0)));
//		VitalGraphArcContainer c = new VitalGraphArcContainer(QueryContainerType.and, new VitalGraphArcElement(Source.PARENT_SOURCE, Connector.HYPEREDGE, Destination.));
//		vpq.setArcs(Arrays.asList(a));
//		return null;
		
		throw new RuntimeException("servicewide hyperedges access not supported yet!");
	}

	@Override
	public List<VITAL_Node> getSourceNodesForDestURI(String destUri,
			Class<? extends VITAL_Edge> edgeType, VITAL_Container... containers) {
		return getEndpointNodesForGivenURI(destUri, false, edgeType, containers);
	}

	@Override
	public void registerObjects(List<GraphObject> arg0) {}

}
