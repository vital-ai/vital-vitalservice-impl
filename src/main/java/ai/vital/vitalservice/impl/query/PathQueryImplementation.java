package ai.vital.vitalservice.impl.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.query.Connector;
import ai.vital.vitalservice.query.Destination;
import ai.vital.vitalservice.query.GraphElement;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.Source;
import ai.vital.vitalservice.query.VitalGraphArcContainer;
import ai.vital.vitalservice.query.VitalGraphArcElement;
import ai.vital.vitalservice.query.VitalGraphBooleanContainer;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.classes.ClassMetadata;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Edge;
import ai.vital.vitalsigns.model.VITAL_Edge_PropertiesHelper;
import ai.vital.vitalsigns.model.VITAL_Node;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.properties.PropertyMetadata;

public class PathQueryImplementation {

	private VitalPathQuery pathQuery;

	private List<String> rootURIs = new ArrayList<String>();
	
	private LinkedHashMap<String, GraphObject> results = new LinkedHashMap<String, GraphObject>();
	
	List<List<VitalSelectQuery>> containers = new ArrayList<List<VitalSelectQuery>>();
	
	private PathQueryExecutor executor;
	
	public static abstract class PathQueryExecutor {
		
		protected VitalOrganization organization;
		protected VitalApp app;
		public PathQueryExecutor(VitalOrganization organization, VitalApp app) {
			super();
			this.organization = organization;
			this.app = app;
		}
		
		public abstract ResultList get(List<URIProperty> rootURIs) throws VitalServiceUnimplementedException, VitalServiceException;
		
		public abstract ResultList query(VitalSelectQuery rootSelect) throws VitalServiceUnimplementedException, VitalServiceException ;
		
	}
	
	public static class VitalServicePathQueryExecutor extends PathQueryExecutor {

		private VitalService service;

		public VitalServicePathQueryExecutor(VitalService service) {
			super(service.getOrganization(), service.getApp());
			this.service = service;
		}

		@Override
		public ResultList get(List<URIProperty> rootURIs) throws VitalServiceUnimplementedException, VitalServiceException {
			return service.get(GraphContext.ServiceWide, rootURIs);
		}

		@Override
		public ResultList query(VitalSelectQuery select) throws VitalServiceUnimplementedException, VitalServiceException {
			return service.query(select);
		}
		
	}
	
	public static class VitalServiceAdminPathQueryExector extends PathQueryExecutor {

		private VitalServiceAdmin adminService;

		public VitalServiceAdminPathQueryExector(VitalServiceAdmin adminService,
				VitalApp app) {
			super(adminService.getOrganization(), app);
			this.adminService = adminService;
		}

		@Override
		public ResultList get(List<URIProperty> rootURIs)
				throws VitalServiceUnimplementedException,
				VitalServiceException {
			return adminService.get(app, GraphContext.ServiceWide, rootURIs);
		}

		@Override
		public ResultList query(VitalSelectQuery rootSelect)
				throws VitalServiceUnimplementedException,
				VitalServiceException {
			return adminService.query(app, rootSelect);
		}
		
	}
	
	public PathQueryImplementation(VitalPathQuery pathQuery, PathQueryExecutor executor) {
		this.pathQuery = pathQuery;
		this.executor = executor;
	}
	
	private void ex(String m) { throw new RuntimeException(m); }

	/*
	public static class PathInfo {
		VitalSelectQuery nodesQuery;
		VitalSelectQuery edgesQuery;
		boolean forwardNotReverse;
	}
	*/
	
	public ResultList execute() throws VitalServiceException, VitalServiceUnimplementedException {
		
		if(pathQuery.getArcs().size() < 1) ex("No arcs set in a path query");
		
		if(pathQuery.getSegments().size() < 1) ex("No segments in a path query");
		
		//each path is expanded independently
		for(VitalGraphArcContainer arc : pathQuery.getArcs()) {
			
			List<WrappedContainer> splitArc = splitArc(arc);
			WrappedContainer nodesContainer = splitArc.get(0);
			WrappedContainer edgesContainer = splitArc.get(1);
			
//			if(nodesContainer.nodesCriteria < 1) ex("No node constraints found in an ARC");
			if(edgesContainer.edgesCriteria < 1) ex("No edge constraints found in an ARC");
			
			//top arcs?
			VitalSelectQuery sq1 = null;
			if(nodesContainer.nodesCriteria > 1) {
				sq1 = new VitalSelectQuery();
				sq1.setLimit(10000);
				sq1.setOffset(0);
				sq1.setSegments(pathQuery.getSegments());
				
				VitalGraphArcContainer c = new VitalGraphArcContainer(QueryContainerType.and, arc.getArc());
				c.add(nodesContainer.container);
				sq1.setTopContainer(c);
				
			}
			
			VitalSelectQuery sq2 = new VitalSelectQuery();
			sq2.setLimit(10000);
			sq2.setOffset(0);
			sq2.setSegments(pathQuery.getSegments());
			VitalGraphArcContainer c2 = new VitalGraphArcContainer(QueryContainerType.and, arc.getArc());
			c2.add(edgesContainer.container);
			sq2.setTopContainer(c2);
			
			containers.add(Arrays.asList(sq1, sq2));
			
		}
		
		if(pathQuery.getRootArc() != null && pathQuery.getRootURIs() != null) ex("cannot use both root arc and rooturis at the same time");
		
		if(pathQuery.getRootArc() == null && ( pathQuery.getRootURIs() == null || pathQuery.getRootURIs().size() < 1)) ex("Expected either root arc or uris list");
		
		if(pathQuery.getRootURIs() != null) {
			
			ResultList list = executor.get(pathQuery.getRootURIs());
			
			for(GraphObject g : list) {
				
				rootURIs.add(g.getURI());
				
				results.put(g.getURI(), g);
				
			}
			
		} else {
			
			List<WrappedContainer> splitArc = splitArc(pathQuery.getRootArc());
			
			WrappedContainer nodesContainer = splitArc.get(0);
			WrappedContainer edgesContainer = splitArc.get(1);
			
			if(nodesContainer.nodesCriteria < 1) ex("No node constraints found in ROOT");
			if(edgesContainer.edgesCriteria > 0) ex("ROOT arc must not have edge constraints");
			
			
			//execute root select query
			VitalSelectQuery rootSelect = new VitalSelectQuery();
			rootSelect.setSegments(pathQuery.getSegments());
			rootSelect.setOffset(0);
			rootSelect.setLimit(10000);
//			rootSelect.setPayloads(false);
//			rootSelect.s
			rootSelect.setTopContainer(pathQuery.getRootArc());
			
			ResultList rl = executor.query(rootSelect);
			
			if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new RuntimeException("Error when querying for root node: " + rl.getStatus().getMessage());
			
			for(GraphObject go : rl) {
				
				rootURIs.add(go.getURI());
				
				results.put(go.getURI(), go);
				
			}
			
		}
		
		if(results.size() < 1) {
			return doReturn();
		}
		
		
		processPaths();
			
		return doReturn();
		
	}


	private void filterGraphElementSymbol(VitalSelectQuery sq1) {
		
		VitalGraphArcContainer topContainer = sq1.getTopContainer();
		
		topContainer.setArc(new VitalGraphArcElement(Source.CURRENT, Connector.EMPTY, Destination.EMPTY));
		
		for(VitalGraphQueryContainer<?> c : topContainer) {
			
			if(c instanceof VitalGraphCriteriaContainer) {
				
				filterCC((VitalGraphCriteriaContainer) c);
				
			} else if(c instanceof VitalGraphQueryPropertyCriterion){
				
				VitalGraphQueryPropertyCriterion pc = (VitalGraphQueryPropertyCriterion) c;
				pc.setSymbol(GraphElement.Source);
				
			} else {
				throw new RuntimeException("Unexpected child of a top arc container of a select query: " + c);
			}
			
		}
		
	}

	private void filterCC(VitalGraphCriteriaContainer c) {
		
		for(VitalGraphQueryElement el : c) {
			if(el instanceof VitalGraphCriteriaContainer) {
				filterCC((VitalGraphCriteriaContainer) el);
			} else if( el instanceof VitalGraphQueryPropertyCriterion) {
				VitalGraphQueryPropertyCriterion pc = (VitalGraphQueryPropertyCriterion) el;
				pc.setSymbol(GraphElement.Source);
				
			} else {
				throw new RuntimeException("Unexpected child of a criteria container in a select query: " + el);
			}
		}
		
	}

	private void processPaths() throws VitalServiceException, VitalServiceUnimplementedException {

		Set<String> currentRootURIs = new HashSet<String>(rootURIs);
		
		int currentDepth = 1;
		
		for( ; (pathQuery.getMaxdepth() < 1 || currentDepth <= pathQuery.getMaxdepth()) && currentRootURIs.size() > 0 ; currentDepth++ ) {

			Set<String> newRoots = new HashSet<String>();
			
			for(List<VitalSelectQuery> pair : containers) {
			
				//first select edges
				VitalSelectQuery edgesQ = null;
				try {
					edgesQ = (VitalSelectQuery) pair.get(1).clone();
				} catch (CloneNotSupportedException e) {
					ex(e.getLocalizedMessage());
				}
				
	
				VitalGraphCriteriaContainer cc = edgesQ.getCriteriaContainer();
				
				VITAL_Edge_PropertiesHelper h = new VITAL_Edge_PropertiesHelper();
				
				boolean forward = edgesQ.getTopContainer().getArc().source == Source.PARENT_SOURCE;
				
				VitalGraphQueryPropertyCriterion pc = forward ? h.getEdgeSource() : h.getEdgeDestination();
				
				List<URIProperty> uris = new ArrayList<URIProperty>();
				for(String root : currentRootURIs) {
					uris.add(new URIProperty(root));
				}
				pc.oneOf(uris);
				
				cc.add(pc);
				
				Set<String> newRootsCandidates = new HashSet<String>();

				filterGraphElementSymbol(edgesQ);
				
				ResultList rl = executor.query(edgesQ);
						
				if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new RuntimeException("Error when querying for path connectors: " + rl.getStatus().getMessage());
				
				for(GraphObject g : rl) {
					
					if(g instanceof VITAL_Edge) {
						VITAL_Edge e = (VITAL_Edge) g;
						
						if(forward) {
							newRootsCandidates.add(e.getDestinationURI());
						} else {
							newRootsCandidates.add(e.getSourceURI());
						}
						
						if(!results.containsKey(e.getURI())) {
							results.put(e.getURI(), e);
						}
						
					} else {
						ex("Expected only edges");
					}
					
				}
				
				if(newRootsCandidates.size() < 1) continue;
				
				if(pair.get(0) != null) {
					
					//do select nodes to verify the input candidates
					VitalSelectQuery nodesQ = null;
					try {
						nodesQ = (VitalSelectQuery) pair.get(0).clone();
					} catch(Exception e) {
						ex(e.getLocalizedMessage());
					}
					
					VitalGraphCriteriaContainer nc = nodesQ.getCriteriaContainer();
					
					List<URIProperty> nodeUris = new ArrayList<URIProperty>();
					for(String u : newRootsCandidates) {
						nodeUris.add(new URIProperty(u));
					}
					
					VitalGraphQueryPropertyCriterion uc = new VitalGraphQueryPropertyCriterion(VitalGraphQueryPropertyCriterion.URI);
					uc.oneOf(nodeUris);
					nc.add(uc);
					
					filterGraphElementSymbol(nodesQ);
					
					ResultList nodesRL = executor.query(nodesQ);
					
					if(nodesRL.getStatus().getStatus() != VitalStatus.Status.ok) throw new RuntimeException("Error when querying for path endpoints: " + nodesRL.getStatus().getMessage());
					
					for(GraphObject g : nodesRL) {
						
						if(g instanceof VITAL_Node) {
							
							if(!results.containsKey(g.getURI())) {
								results.put(g.getURI(), g);
								//detect loops, only new objects added as new roots
								newRoots.add(g.getURI());
							}
							
						} else {
							
							ex("expected vital nodes only in target select query results");
							
						}
						
					}
					
				} else {
					
					List<URIProperty> targetUris = new ArrayList<URIProperty>(newRootsCandidates.size());
					for(String u : newRootsCandidates) {
						targetUris.add(URIProperty.withString(u));
					}
					
					ResultList objects = executor.get(targetUris);
					
					for(GraphObject g : objects) {
						
						if(g instanceof VITAL_Node) {
							
							
							if(!results.containsKey(g.getURI())) {
								results.put(g.getURI(), g);
								//detect loops, only new objects added as new roots
								newRoots.add(g.getURI());
							}
							
						} else {
							
							ex("expected vital nodes only in edge targets");
							
						}
						
					}
					
					
				}
				
			}
			
			currentRootURIs = newRoots;
			
		}
		
	}

	private List<WrappedContainer> splitArc(VitalGraphArcContainer arc) {

		List<VitalGraphCriteriaContainer> rootContainers = new ArrayList<VitalGraphCriteriaContainer>();
		
		for( VitalGraphQueryContainer<?> c : arc ) {
			
			if(c instanceof VitalGraphArcContainer) ex("Nested ARCs forbidden");
			
			if(c instanceof VitalGraphBooleanContainer) ex("ARC boolean containers forbidden");
			
			if(c instanceof VitalGraphCriteriaContainer) {
				
				rootContainers.add((VitalGraphCriteriaContainer) c);
				
			}
			
		}
		
		if(rootContainers.size() < 1) ex("No criteria containers found in an ARC");
		
		VitalGraphCriteriaContainer topContainer = null;
		
		if(rootContainers.size() == 1) {
			
			topContainer = rootContainers.get(0);
			
			if(topContainer.getType() != QueryContainerType.and) ex("Top criteria container must be of type AND");
			
		} else {
			
			topContainer = new VitalGraphCriteriaContainer(QueryContainerType.and);
			
			topContainer.addAll(rootContainers);
			
		}
		
		
		return splitTopContainer(topContainer);
		
		
		
	}

	public static class WrappedContainer {
		
		VitalGraphCriteriaContainer container = new VitalGraphCriteriaContainer(QueryContainerType.and);
		int edgesCriteria = 0;
		int nodesCriteria = 0;
		
	}
	
	private List<WrappedContainer> splitTopContainer(VitalGraphCriteriaContainer topContainer) {

//		int topEdges = 0;
//		int topNodes = 0;

		WrappedContainer nodesContainer = new WrappedContainer();
		WrappedContainer edgesContainer = new WrappedContainer();
		
		
		for( VitalGraphQueryElement el : topContainer ) {
			
			if(el instanceof VitalGraphQueryTypeCriterion) {
				
				Class<? extends GraphObject> c = ((VitalGraphQueryTypeCriterion)el).getType();
				
				if(VITAL_Node.class.isAssignableFrom(c)) {
					nodesContainer.container.add(el);
					nodesContainer.nodesCriteria++;
				} else if(VITAL_Edge.class.isAssignableFrom(c)) {
					edgesContainer.container.add(el);
					edgesContainer.edgesCriteria++;
				} else {
					ex("only node/edge constraints allowed, invalid type: " + c);
				}
				
			} else if(el instanceof VitalGraphQueryPropertyCriterion) {
				
				String pURI = ((VitalGraphQueryPropertyCriterion) el).getPropertyURI();
				
				PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(pURI);
				if(pm == null) ex("Property with URI not found: " + pURI);
				
				boolean isNodeClass = false;
				boolean isEdgeClass = false;
				String s = "";
				for( ClassMetadata domain : pm.getDomains() ) {
					if(VITAL_Node.class.isAssignableFrom(domain.getClazz())) {
						isNodeClass = true;
					} else if(VITAL_Edge.class.isAssignableFrom(domain.getClazz())) {
						isEdgeClass = true;
					}
					if(s.length() > 0) {
						s += ", ";
					}
					s += domain.getClazz().getCanonicalName();
				}
				
				if(isNodeClass && isEdgeClass) ex("Ambiguous property - both edge and node domain: " + pURI + " " + s);
				if(!isNodeClass && !isEdgeClass) ex("Property not a node nor edge property: " + pURI);
				
				if(isNodeClass) {
					nodesContainer.container.add(el);
					nodesContainer.nodesCriteria++;
				} else if(isEdgeClass) {
					edgesContainer.container.add(el);
					edgesContainer.edgesCriteria++;
				}
				
			} else if(el instanceof VitalGraphCriteriaContainer) {
				
				VitalGraphCriteriaContainer cc = (VitalGraphCriteriaContainer) el;
				
				WrappedContainer wrapped = new WrappedContainer();
				wrapped.container = cc;
				
				analyzeContainer(wrapped, cc);
				
				
				if(wrapped.nodesCriteria > 0 && wrapped.edgesCriteria > 0) ex("Edges and nodes criteria cannot be mixed in same container (except root)");
				if(wrapped.nodesCriteria == 0 && wrapped.edgesCriteria == 0) ex("No nodes or edges criteria found in a sub criteria container");
				if(wrapped.nodesCriteria > 0) {
					nodesContainer.container.add(cc);
					nodesContainer.nodesCriteria++;
				} else {
					edgesContainer.container.add(cc);
					edgesContainer.edgesCriteria++;
				}
				
				
			} else {
				ex("unexpected child of a criteria container");
			}
			
		}
		
		//don't throw it here yet
//		if(nodesContainer.nodesCriteria < 1) ex("No node constraints found in an ARC");
//		if(edgesContainer.edgesCriteria < 1) ex("No edge constraints found in an ARC");
		
		return Arrays.asList(nodesContainer, edgesContainer);
		
		
	}

	private void analyzeContainer(WrappedContainer wrapped, VitalGraphCriteriaContainer cc) {

		for( VitalGraphQueryElement el : cc ) {
			
			if(el instanceof VitalGraphQueryTypeCriterion) {
				
				Class<? extends GraphObject> c = ((VitalGraphQueryTypeCriterion)el).getType();
				
				if(VITAL_Node.class.isAssignableFrom(c)) {
					wrapped.nodesCriteria++;
				} else if(VITAL_Edge.class.isAssignableFrom(c)) {
					wrapped.edgesCriteria++;
				} else {
					ex("only node/edge constraints allowed, invalid type: " + c);
				}
				
			} else if(el instanceof VitalGraphQueryPropertyCriterion) {
				
				String pURI = ((VitalGraphQueryPropertyCriterion) el).getPropertyURI();
				
				PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(pURI);
				if(pm == null) ex("Property with URI not found: " + pURI);
				
				boolean isNodeClass = false;
				boolean isEdgeClass = false;
				String s = "";
				for( ClassMetadata domain : pm.getDomains() ) {
					if(VITAL_Node.class.isAssignableFrom(domain.getClazz())) {
						isNodeClass = true;
					} else if(VITAL_Edge.class.isAssignableFrom(domain.getClazz())) {
						isEdgeClass = true;
					}
					if(s.length() > 0) {
						s += ", ";
					}
					s += domain.getClazz().getCanonicalName();
				}
				
				if(isNodeClass && isEdgeClass) ex("Ambiguous property - both edge and node domain: " + pURI + " " + s);
				if(!isNodeClass && !isEdgeClass) ex("Property not a node nor edge property: " + pURI);
				
				if(isNodeClass) {
					wrapped.nodesCriteria++;
				} else if(isEdgeClass) {
					wrapped.edgesCriteria++;
				}
				
			} else if(el instanceof VitalGraphCriteriaContainer) {
				
				analyzeContainer(wrapped, (VitalGraphCriteriaContainer) el);
				
			} else {
				ex("unexpected child of a criteria container");
			}
			
		}
		
	}


	private ResultList doReturn() {

		ResultList rl = new ResultList();
		rl.setTotalResults(results.size());
		rl.setLimit(Integer.MAX_VALUE);
		rl.setOffset(0);
		rl.setStatus(VitalStatus.OK);
		
		for(Entry<String, GraphObject> e : results.entrySet()) {
			
			rl.getResults().add(new ResultElement(e.getValue(), 1D));
			
		}
		
		return rl;
		
	}
	
}
