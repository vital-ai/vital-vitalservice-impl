package ai.vital.service.lucene.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Version;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.lucene.model.LuceneSegment;
import ai.vital.service.lucene.model.LuceneSegmentConfig;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSortProperty;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.utils.StringUtils;

public abstract class LuceneServiceImpl {

	// map that keeps all open segments, segment URI -> lucene segment
	protected Map<String, LuceneSegment> segments = Collections.synchronizedMap(new HashMap<String, LuceneSegment>());

//	protected Map<LuceneSegment, Long> unsortedSegmentTimestamps = new HashMap<LuceneSegment, Long>()
//	protected TreeMap<LuceneSegment, Long> sortedSegmentTimestamps = new TreeMap<LuceneSegment, Long>(unsortedSegmentTimestamps)
	
	protected IndexWriterConfig getIndexWriterConfig() { return  new IndexWriterConfig(Version.LUCENE_47, new WhitespaceAnalyzer(Version.LUCENE_47)); }
	
	// no longer aware of organization/app, everything keyed with segmentURI
	public void addSegment(VitalSegment sc) throws LuceneException {
		
		if(segments.containsKey(sc.getURI())) throw new LuceneException("Segment index with URI: " + sc.getURI() + " id: " + sc.getRaw(Property_hasSegmentID.class) + " already exists");
		
		LuceneSegment segment = new LuceneSegment(null, null, sc, getConfig(sc));
		
		try {
			segment.open();
		} catch(IOException e) {
			throw new LuceneException(e);
		} 
		
		segments.put(sc.getURI(), segment);
		
	}
	
	public void removeSegment(VitalSegment s, boolean deleteData) throws LuceneException {
		
		String segmentID = (String) s.getRaw(Property_hasSegmentID.class);
		
		LuceneSegment segment = segments.remove(s.getURI());
		
		if(segment == null) throw new LuceneException("Segment with URI: " + s.getURI() + ", id:" + segmentID + " not found");
		
		try {
			segment.close();
		} catch (IOException e) {
			throw new LuceneException(e);
		}
		
		if(deleteData) {
			try {
				segment.deleteData();
			} catch (IOException e) {
				throw new LuceneException(e);
			}
		}
		
		
	}
	
	public abstract void initializeRootIndices() throws LuceneException;
	
	protected LuceneServiceImpl() {
		
	}
	

	protected abstract Map<String, LuceneSegment> openInitialSegments();
	
	public void open() throws LuceneException {
		
		segments.putAll( openInitialSegments() );
		
	}
	
	protected abstract LuceneSegmentConfig getConfig(VitalSegment segment);
	
	public List<String> getAllSegments() {
		synchronized (segments) {
			return new ArrayList<String>(segments.keySet());
		}
	}
	
	public GraphObject get(List<VitalSegment> segmentsList, URIProperty uri) throws LuceneException, IOException  {

		GraphObject g = null;
		
		for(VitalSegment segment : segmentsList) {
			
			LuceneSegment ls = segments.get(segment.getURI());
			
			g = ls.getGraphObject(uri.get());
			
			if(g != null) break;
			
		}
		
		return g;
		
	}

	/**
	 * inserts or updates a graph object into given segment, it only check other segments if the list is provided
	 * @param targetSegment
	 * @param graphObject
	 * @return
	 * @throws LuceneException
	 */
	public GraphObject save(VitalSegment targetSegment, GraphObject graphObject, List<VitalSegment> segmentsPool) throws LuceneException {

		if(StringUtils.isEmpty(graphObject.getURI())) throw new NullPointerException("graph object's URI cannot be null or empty");
		if(targetSegment == null) throw new NullPointerException("target segment cannot be null");
		if(targetSegment.getURI() == null) throw new NullPointerException("target segment URI cannot be null");

		LuceneSegment segment = segments.get(targetSegment.getURI());

		if( segment == null ) throw new LuceneException("Segment not found, URI: " + targetSegment.getURI() + ", id: " + targetSegment.getRaw(Property_hasSegmentID.class));
		
		if(segmentsPool != null && segmentsPool.size() > 0) {

			for(VitalSegment s : segmentsPool) {
				
				if(s.getURI().equals(targetSegment.getURI())) continue;
				
				LuceneSegment ls = segments.get(s.getURI());
				if( ls == null ) throw new LuceneException("Segment not found, URI: " + s.getURI() + ", id: " + s.getRaw(Property_hasSegmentID.class));
				
				try {
					if(ls.containsURI(graphObject.getURI())) throw new LuceneException("Object with URI: " + graphObject.getURI() + " already found in another segment: " + s.getRaw(Property_hasSegmentID.class));
				} catch (IOException e) {
					throw new LuceneException(e);
				}
				
			}
			
		}
		
	
		try {
			graphObject = segment.insertOrUpdate(graphObject);		
		} catch(IOException e) {
			throw new LuceneException(e);
		}
		
		return graphObject;
	}

	
	public ResultList save(VitalSegment targetSegment, List<GraphObject> graphObjectsList, List<VitalSegment> segmentsPool) throws LuceneException {

		if(targetSegment == null) throw new NullPointerException("target segment cannot be null");
		if(targetSegment.getURI() == null) throw new NullPointerException("target segment URI cannot be null");
		
		Set<String> uris = new HashSet<String>();
				
		for(int i = 0 ; i < graphObjectsList.size(); i++) {
			GraphObject g = graphObjectsList.get(i);
			if(g == null) throw new NullPointerException("one of graph object is null, index: " + i);
			if(g.getURI() == null) throw new NullPointerException("one of graph objects\'s URI is null or empty, index: " + i);
			if(uris.contains(g.getURI())) throw new NullPointerException("more than 1 graph object with same uri in input collection: " + g.getURI());
			uris.add(g.getURI());
		}
		
		LuceneSegment segment = segments.get(targetSegment.getURI());
		
		if( segment == null ) throw new LuceneException("Segment not found, URI: " + targetSegment.getURI() + ", id: " + targetSegment.getRaw(Property_hasSegmentID.class));

		if(segmentsPool != null && segmentsPool.size() > 0) {

			for(VitalSegment s : segmentsPool) {
				
				if(s.getURI().equals(targetSegment.getURI())) continue;
				
				LuceneSegment ls = segments.get(s.getURI());
				if( ls == null ) throw new LuceneException("Segment not found, URI: " + s.getURI() + ", id: " + s.getRaw(Property_hasSegmentID.class));
				
				int total = 0;
				try {
					total = ls.containsURIs(uris);
				} catch (IOException e) {
					throw new LuceneException(e);
				}
				
				if(total > 0) throw new LuceneException("${total} object(s) already found in another segment: ${s.ID}");
				
			}
			
		}
		
		/*
		for(LuceneSegment s : segments.values()) {
			
			if(s == segment) {
				continue;
			}
			
			try {
				
				int total = s.containsURIs(uris);
				
				if( total > 0 ) throw new LuceneException("${total} object(s) already found in another segment: ${s.ID}")
				
			} catch(IOException e) {
				throw new LuceneException(e)
			}
			
			
			
		}
		*/
		
		Collection<GraphObject> out = null;
		
		try {
			out = segment.insertOrUpdateBatch(graphObjectsList);
		} catch(IOException e) {
			throw new LuceneException(e);
		}
		
		ResultList l = new ResultList();
		for(GraphObject g : graphObjectsList) {
			
			l.getResults().add(new ResultElement(g, 1d));
			
		}
		
		l.setTotalResults(graphObjectsList.size());
		
		return l;
		
	}


	public VitalStatus delete(List<VitalSegment> segmentsPool, URIProperty uri, boolean stopAtFirstSuccess) throws LuceneException {

		if(segmentsPool == null || segmentsPool.isEmpty()) throw new RuntimeException("Segments pool cannot be null nor empty");
		
		//check which segment contains uri first
		
		String u = uri.get();
		
		//special case
		if(u.startsWith(URIProperty.MATCH_ALL_PREFIX)) {
			
			throw new LuceneException("Match all case should be handled at the higher level");
			
			/*
			String segmentURI = uri.get().substring(URIProperty.MATCH_ALL_PREFIX.length());
			if(segmentURI.length() < 1) throw new LuceneException("No segment URI provided in special match-all case!");
			
			LuceneSegment ls = segments.get(segmentURI);
			
			if(ls == null) throw new LuceneException("Lucene segment not found, URI: " + segmentURI);
			
			int totalDocs = ls.totalDocs();
			try {
				ls.deleteAll();
			} catch (IOException e) {
				throw new LuceneException(e);
			}
			
			VitalStatus status = VitalStatus.withOKMessage("All segment ${segmentID} objects deleted: " + totalDocs);
			status.setSuccesses(totalDocs);
			return status;
			*/
			
		}
		
		if(segmentsPool == null || segmentsPool.size() < 1) throw new RuntimeException("Segments list cannot be null or empty");

		boolean deleted = false;
		
		for(VitalSegment s : segmentsPool) {
			
			LuceneSegment ls = segments.get(s.getURI());
			if(ls == null) throw new RuntimeException("Segment not found: " + s.getURI() + " id: " + s.getRaw(Property_hasSegmentID.class));
			
			try {
				if( ls.containsURI(u) ) {
					ls.delete(u);
					deleted = true;
					if(stopAtFirstSuccess) break;
				}
			} catch (IOException e) {
				throw new LuceneException(e);
			}
			
		}
		
		if(deleted) return VitalStatus.withOK();
		return VitalStatus.withError("Graph object not found: " + u);
	}

	
	public VitalStatus delete(List<VitalSegment> segmentsPool, List<URIProperty> uris) throws LuceneException  {

		if(segmentsPool == null || segmentsPool.isEmpty()) throw new RuntimeException("Segments pool cannot be null nor empty");
		
		Set<String> urisSet = new HashSet<String>();
		
		for(URIProperty u : uris) {
			if( ! urisSet.add(u.get()) ) throw new RuntimeException("URI : " + u.get() + " listed more than once");
		}
		
		VitalStatus status = VitalStatus.withOK();
		
		int totalDeleted = 0;
		
		for(VitalSegment s : segmentsPool) {
			
			LuceneSegment ls = segments.get(s.getURI());
			
			if(ls == null) throw new RuntimeException("Segment not found: " + s.getURI() + " id: " + s.getRaw(Property_hasSegmentID.class)); 
			
			try {
				int containsURIs = ls.containsURIs(urisSet);
				if(containsURIs > 0) {
					ls.deleteBatch(urisSet);
				}
				totalDeleted += containsURIs;
			} catch (IOException e) {
				throw new LuceneException(e);
			}
			
		}
		
		status.setSuccesses(totalDeleted);
		status.setErrors(urisSet.size() - totalDeleted);
		
		return VitalStatus.withOK();
		
	}
			
	public VitalStatus deleteAll(VitalSegment targetSegment) throws LuceneException {
		
		LuceneSegment lSegment = segments.get(targetSegment.getURI());
		
		if(lSegment == null ) throw new RuntimeException("Segment not found, URI: " + targetSegment.getURI() + " id: " + targetSegment.getRaw(Property_hasSegmentID.class));
		
		try {
			
			int totalDocs = lSegment.totalDocs();
			
			lSegment.close();
			lSegment.deleteData();
			lSegment.open();
			
			VitalStatus status = VitalStatus.withOKMessage("All segment ${segmentID} objects deleted: " + totalDocs);
			status.setSuccesses(totalDocs);
			return status;
			
		} catch (IOException e) {
			throw new LuceneException(e);
		}
	}

	
	public ResultList selectQuery(VitalSelectQuery query)
			throws LuceneException {

		// if(query instanceof VitalSparqlQuery) throw new RuntimeException("Sparql query not implemented by lucene endpoint");
				
		// quick implementation of export query - it adds a single component and sort property
		if(query instanceof VitalExportQuery) {
			
			VitalExportQuery mainQ = (VitalExportQuery) query;
			
			//clone the export query!
			query = new VitalExportQuery();
			//query.components = [new VitalPropertyConstraint(VitalCoreOntology.NS + 'isActive', false, VitalPropertyConstraint.Comparator.EQ, true)]
			query.setSortProperties(Arrays.asList(new VitalSortProperty(VitalSortProperty.INDEXORDER, null, false)));
			query.setOffset(mainQ.getOffset());
			query.setLimit(mainQ.getLimit());
			query.setSegments(mainQ.getSegments());
			
		}
		
		boolean aggQ = query instanceof VitalSelectAggregationQuery;
		
		// if(!supportsSelectQuery(query)) throw new LuceneException("Query not supported.");
				
				
		if(query.getSegments() == null || query.getSegments().size() < 1) throw new NullPointerException("query segments list cannot be null or empty");
				
		//main select query implementation
		
		List<LuceneSegment> luceneSegments = new ArrayList<LuceneSegment>();
		
		for(VitalSegment segment : query.getSegments()) {

			LuceneSegment ls = segments.get(segment.getURI());
			
			if( ls == null ) throw new LuceneException("segment with URI: " + segment.getURI() + " not found, id: " + segment.getRaw(Property_hasSegmentID.class));
			
			if(aggQ) {
				if( ! ( ls.getConfig().isStoreObjects() || ls.getConfig().isStoreNumericFields() ) ) {
					throw new LuceneException("segment with URI: " + segment.getURI() + " does not support aggregation function (no stored number properties), id: " + segment.getRaw(Property_hasSegmentID.class));
				}
			}
			
			luceneSegments.add(ls);
			
		}
		
		ResultList results = LuceneServiceQueriesImpl.selectQuery(luceneSegments, query);

		return results;
			
	}

	public ResultList graphQuery(VitalOrganization organization, VitalApp app, VitalGraphQuery query)
			throws LuceneException {

		if(query.getSegments() == null || query.getSegments().size() < 1) throw new NullPointerException("query segments list cannot be null or empty");
		
		/*
		if(query instanceof VitalPathQuery) {
			
			VitalPathQuery vpq = query
			if(vpq.rootUris == null || vpq.rootUris.size() < 1) throw new NullPointerException("query root URIs list cannot be null or empty")
		}
		*/
		
		
//		if(query.pathsElements == null || query.pathsElements.size() < 1) throw new NullPointerException("query path elements list cannot be null or empty");
		
		//if(!supportsGraphQuery(query)) throw new LuceneException("Query not supported");		
		
		//main select query implementation
		
		List<LuceneSegment> luceneSegments = new ArrayList<LuceneSegment>();
		
		for(VitalSegment segment : query.getSegments()) {

			LuceneSegment ls = segments.get(segment.getURI());
			
			if( ls == null ) throw new LuceneException("segment with URI: " + segment.getURI() + " not found, id: " + segment.getRaw(Property_hasSegmentID.class));
			
			luceneSegments.add(ls);
			
		}
		
		try {
			
			ResultList results = LuceneServiceQueriesImpl.handleQuery(organization, app, query, luceneSegments);
			
			return results;
			
		} catch(Exception e) {
			throw new LuceneException(e);
		}
				
	}

	public void close() {
		
		for(LuceneSegment segment : segments.values()) {
			
			try { segment.close(); } catch(Exception e) {}
			
		}
		
		segments.clear();
		
	}	


	public int getSegmentSize(VitalSegment segment) throws LuceneException {

		LuceneSegment lSegment = segments.get(segment.getURI());
		if(lSegment == null ) throw new RuntimeException("Segment not found, URI: " + segment.getURI() + " id: " + segment.getRaw(Property_hasSegmentID.class));		
		
		try {
			return lSegment.getSegmentSize();
		} catch (IOException e) {
			throw new LuceneException(e);
		}
				
	}


	public boolean supportsSelectQuery(VitalOrganization organization, VitalApp app, VitalSelectQuery sq) {
		
		//check segments to see if stored 
		
		if(sq instanceof VitalSelectAggregationQuery || sq.getDistinct()) {
			
			/*
			VitalSelectAggregationQuery vsaq = sq
			
			if(vsaq.getAggregationType() == AggregationType.count) {
				
				if(vsaq.getVitalProperty() == null) {
					
					return true 
					
				}
				
			}
			*/
			
			for(VitalSegment s : sq.getSegments()) {
			
				LuceneSegment seg = segments.get(s.getURI());
				
				if(seg == null) throw new RuntimeException("Segment not found: " + s.getURI() + ", id: " + s.getRaw(Property_hasSegmentID.class));
				
				
				if(sq.getDistinct() && !seg.getConfig().isStoreObjects()) {
					return false;
				}
				
				if( ! ( seg.getConfig().isStoreObjects()|| seg.getConfig().isStoreNumericFields() ) ) {
					//1 of segments does not support it
					return false;
				}
				
			}
			
		}
		
		return true;
		
	}
	
	public boolean supportsGraphQuery(VitalOrganization organization, VitalApp app, VitalGraphQuery gq) {
		
		return true;
		
	}
	
	public VITAL_GraphContainerObject getExistingObjects(List<VitalSegment> segmentsPool, List<String> uris) throws LuceneException {

		if(segmentsPool == null || segmentsPool.isEmpty()) throw new RuntimeException("Segments pool must not be null nor empty");
		
		VITAL_GraphContainerObject c = new VITAL_GraphContainerObject();
		c.setURI("urn:x");

		for(VitalSegment s : segmentsPool) {
			
			String segmentID = (String) s.getRaw(Property_hasSegmentID.class);
			
			LuceneSegment segment = segments.get(s.getURI());
			
			if(segment == null) throw new RuntimeException("Segment not found: " + s.getURI() + ", id: " + s.getRaw(Property_hasSegmentID.class));

			try {
				for(String u : segment.containsURIsList(uris) ) {
					
					c.setProperty(u, new StringProperty(segmentID));
					
				}
			} catch (IOException e) {
				throw new LuceneException(e);
			}
			
			
		}
		
		return c;
		
	}
	
	
	
	public VitalStatus bulkExport(VitalSegment s, OutputStream outputStream, String datasetURI)
			throws VitalServiceException {

		LuceneSegment segment = segments.get(s.getURI());
		
		if(segment == null) throw new RuntimeException("Segment not found: " + s.getURI() + ", id: " + s.getRaw(Property_hasSegmentID.class));
		
		return segment.bulkExport(outputStream, datasetURI);
				
	}

	public VitalStatus bulkImport(VitalSegment s, InputStream inputStream, String datasetURI)
			throws VitalServiceUnimplementedException, VitalServiceException {

		LuceneSegment segment = segments.get(s.getURI());
		
		if(segment == null) throw new RuntimeException("Segment not found: " + s.getURI() + ", id: " + s.getRaw(Property_hasSegmentID.class));
		
		return segment.bulkImport(inputStream, datasetURI);
		
	}
	
}
