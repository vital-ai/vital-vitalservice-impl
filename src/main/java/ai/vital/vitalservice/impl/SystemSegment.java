package ai.vital.vitalservice.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ai.vital.lucene.model.LuceneSegment;
import ai.vital.lucene.model.LuceneSegmentType;
import ai.vital.service.lucene.model.LuceneSegmentConfig;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.auth.VitalAuthKeyValidation;
import ai.vital.vitalservice.auth.VitalAuthKeyValidation.VitalAuthKeyValidationException;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.CompactStringSerializer;
import ai.vital.vitalsigns.model.Edge_hasApp;
import ai.vital.vitalsigns.model.Edge_hasAuthKey;
import ai.vital.vitalsigns.model.Edge_hasOrganization;
import ai.vital.vitalsigns.model.Edge_hasProvisioning;
import ai.vital.vitalsigns.model.Edge_hasSegment;
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalAuthKey;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasKey;
import ai.vital.vitalsigns.model.properties.Property_hasOrganizationID;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.utils.StringUtils;

/**
 * Represents a system segments that stored organizations, apps, segments, sessions and auth keys, potentially transaction objects
 * To avoid low level implementations
 *
 */
public class SystemSegment {

	// from now on the 
	LuceneSegment cachedSegment = null;
	
	/**
	 * These should be implemented as low level calls 
	 *
	 */
	
	public static interface SystemSegmentOperationsExecutor {
		
		VitalSegment getSegmentResource(VitalSegment segment);
		
		ResultList query(VitalOrganization organization, VitalApp app, VitalQuery query, ResultList segmentsPoolRS);

		GraphObject getGraphObject(VitalOrganization organization, VitalApp app, VitalSegment segment, String uri, ResultList segmentsPoolRS);

		VitalSegment createSegmentResource(VitalSegment segment, ResultList segmentsPoolRS);

		void saveGraphObjects(VitalOrganization org, VitalApp app,
				VitalSegment systemSegment, List<GraphObject> asList, ResultList segmentsPoolRS);

		void deleteGraphObjects(VitalOrganization org, VitalApp app,
				VitalSegment systemSegment, List<URIProperty> toDelete, ResultList segmentsPoolRS);

		void deleteSegmentResource(VitalSegment segment, ResultList segmentsPoolRS);

		/**
		 * result list is empty
		 * @param segmentRL
		 * @param segment
		 * @param provisioning 
		 */
		void prepareSegmentResources(ResultList segmentRL, VitalSegment segment, VitalProvisioning provisioning);

		/**
		 * 
		 * @param s
		 * @param segmentsRL
		 * @return
		 */
		int getSegmentSize(VitalSegment s, ResultList segmentsRL);

		/**
		 * Returns all objects of the segment as a list
		 * @param segment
		 * @param segmentsRL
		 * @param bos 
		 * @return
		 */
		void bulkExport(VitalSegment segment, ResultList segmentsRL, ByteArrayOutputStream bos);
		
	}
	
	protected static VitalOrganization org = VitalOrganization.withId("vital");
	
	protected static VitalApp app = VitalApp.withId("system");
	
	protected VitalSegment systemSegment = null;
	
	protected ResultList systemSegmentFull = null;
	
	static String SYSTEM_SEGMENT_URI = "http://vital.ai/vital/system/" + VitalSegment.class.getSimpleName() + "/systemsegment";
	
	protected SystemSegmentOperationsExecutor executor;
	
	public static boolean DEFAULT_USE_CACHE = true;
	
	public SystemSegmentOperationsExecutor getExecutor() {
		return executor;
	}

	public SystemSegment(SystemSegmentOperationsExecutor executor) {
		this(executor, DEFAULT_USE_CACHE);
	}
	
	public SystemSegment(SystemSegmentOperationsExecutor executor, boolean cache) {
		super();
		this.executor = executor;
		
		systemSegment = new VitalSegment();
		
		systemSegment.setURI(SYSTEM_SEGMENT_URI);
		
		systemSegment.set(Property_hasSegmentID.class, "systemsegment");
	
		systemSegmentFull = new ResultList();
		
		this.executor.prepareSegmentResources(systemSegmentFull, systemSegment, null);
		
		if(cache) {
			VitalOrganization organization = VitalOrganization.withId("vital");
			VitalApp app = VitalApp.withId("system");
			VitalSegment segmentObj = VitalSegment.withId("systemsegmentcache");
			
			LuceneSegmentConfig cfg = new LuceneSegmentConfig(LuceneSegmentType.memory, true, true, null);
			cachedSegment = new LuceneSegment(organization, app, segmentObj, cfg);
			try {
				cachedSegment.open();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			if(systemSegmentExists()) {
				reloadCache();
			}	
		}
	}

	protected void reloadCache() {
		
		if(cachedSegment == null) return;
		
		try {
			cachedSegment.deleteAll();
			
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			executor.bulkExport(systemSegment, systemSegmentFull, bos);
			
			VitalStatus status = cachedSegment.bulkImport(new ByteArrayInputStream(bos.toByteArray()));
			if(status.getStatus() != VitalStatus.Status.ok) {
				throw new RuntimeException("System segment cache reload failed: " + status.getMessage());
			}
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}

	public boolean systemSegmentExists() {
		// VitalSegment
		VitalSegment seg = executor.getSegmentResource(systemSegment);
		return seg != null;
	}
	
	public void createSystemSegment() throws VitalServiceException {
		
		if(systemSegmentExists()) throw new VitalServiceException("System segment already exists");
		
		executor.createSegmentResource(systemSegment, systemSegmentFull);
	}
	
	protected ResultList handleQuery(VitalQuery query) throws VitalServiceException {

		ResultList rl = null;
		
		if(cachedSegment != null) {
			
			rl = ai.vital.service.lucene.impl.LuceneServiceQueriesImpl.handleQuery(org, app, query, Arrays.asList(cachedSegment));
			
		} else {
			
			rl = executor.query(org, app, query, systemSegmentFull);
			
		}
		
		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
		
		if(query instanceof VitalGraphQuery) {
			
			rl = unpackGraphMatch(rl);
		}
		
		return rl;
		
	}
	
	/**
	 * returns the organization
	 * @param organizationID
	 * @return
	 * @throws VitalServiceException 
	 */
	
	public VitalOrganization getOrganization(String organizationID) throws VitalServiceException {
		
		VitalSelectQuery organizationQuery = SystemSegmentQueries.getOrganizationQuery(systemSegment, organizationID);
		
		ResultList rl = handleQuery(organizationQuery);
		
		// ResultList rl = executor.query(org, app, organizationQuery, systemSegmentFull);
		
		for(GraphObject g : rl) {
			if(g instanceof VitalOrganization) return (VitalOrganization) g;
		}
		
		return null;
	}
	
	public List<VitalOrganization> listOrganizations() throws VitalServiceException {
		
		List<VitalOrganization> organizations = new ArrayList<VitalOrganization>();
		
		ResultList rl = handleQuery(SystemSegmentQueries.getOrganizationsListQuery(systemSegment));
		
		// ResultList rl = executor.query(org, app, SystemSegmentQueries.getOrganizationsListQuery(systemSegment), systemSegmentFull);
		//		
		// if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
		
		for(GraphObject g : rl) {
			if(g instanceof VitalOrganization) {
				organizations.add((VitalOrganization) g);
			}
		}
		
		return organizations;
		
	}
	
	public List<VitalApp> listApps(VitalOrganization organization) throws VitalServiceException {
		
		// check organization ? 
		organization = checkOrganization(organization);
		
		List<VitalApp> apps = new ArrayList<VitalApp>();
		
		VitalGraphQuery appsQuery = SystemSegmentQueries.getAppsQuery(systemSegment, organization);
		
		ResultList rl = handleQuery(appsQuery);
		
		// ResultList rl = executor.query(org, app, SystemSegmentQueries.getAppsQuery(systemSegment, organization), systemSegmentFull);
		//		
		// if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
		//		
		// rl = unpackGraphMatch(rl);
		
		for(GraphObject g : rl) {
			if(g instanceof VitalApp) {
				apps.add((VitalApp) g);
			}
		}
		
		return apps;
	}
	
	public VitalOrganization checkOrganization(VitalOrganization organization) throws VitalServiceException {

		String id = (String) organization.getRaw(Property_hasOrganizationID.class);
		
		VitalSelectQuery sq = SystemSegmentQueries.getOrganizationQuery(systemSegment, id);
		
		ResultList rl = handleQuery(sq);
		
		// ResultList rl = executor.query(org, app, sq, systemSegmentFull);
		// if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
		
		organization = (VitalOrganization) rl.first();
		
		if(organization == null) throw new VitalServiceException("Organization not found, ID: " + id);
		
		return organization;
	}

	/**
	 * Ensures the organization -> app arc exists
	 * @param organization
	 * @param app
	 * @return
	 * @throws VitalServiceException 
	 */
	
	public VitalApp checkApp(VitalOrganization organization, VitalApp app) throws VitalServiceException {

		List<VitalApp> apps = listApps(organization);
		for(VitalApp a : apps) {
			if(a.getURI().equals(app.getURI())) {
				return a;
			}
		}
		
		throw new VitalServiceException("App not found, URI: " + app.getURI() + ", organization URI: " + organization.getURI()); 
	}
	
	/**
	 * Lists segments with linked provisioning objects etc 
	 * @param organization
	 * @param app2
	 * @return
	 * @throws VitalServiceException 
	 */
	
	public ResultList listSegments(VitalOrganization organization, VitalApp app2) throws VitalServiceException {
		
		app2 = checkApp(organization, app2);
		
		VitalGraphQuery segmentsQuery = SystemSegmentQueries.getSegmentsQuery(systemSegment, organization, app2);
		
		ResultList rl = handleQuery(segmentsQuery);
		
		// ResultList rl = executor.query(org, app, segmentsQuery, systemSegmentFull);
		//		
		// if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
		//		
		// rl = unpackGraphMatch(rl);
		
		return rl;
	}
	
	public List<VitalSegment> listSegmentsOnly(VitalOrganization organization, VitalApp app) throws VitalServiceException {
		
		List<VitalSegment> r = new ArrayList<VitalSegment>();
		
		for( Iterator<VitalSegment> iterator = listSegments(organization, app).iterator(VitalSegment.class); iterator.hasNext(); ) {
			
			r.add(iterator.next());
			
		}
		
		return r;
		
	}
	
	public VitalSegment getSegment(VitalOrganization organization, VitalApp app, String segmentID) throws VitalServiceException {
		
		for(VitalSegment s : listSegmentsOnly(organization, app) ) {
			String sid = (String) s.getRaw(Property_hasSegmentID.class);
			if(segmentID.equals(sid)) return s;
		}

		return null;
	}
	

	protected ResultList unpackGraphMatch(ResultList rl) throws VitalServiceException {

		ResultList r = new ResultList();
		
		for(GraphObject g : rl) {
			
			if(g instanceof GraphMatch) {
				
				for(Object obj : g.getPropertiesMap().entrySet()) {

					Entry<String, IProperty> p = (Entry<String, IProperty>) obj;
					
					IProperty unwrapped = p.getValue().unwrapped();

					if(unwrapped instanceof StringProperty) {
						GraphObject x = CompactStringSerializer.fromString((String) unwrapped.rawValue());
						if(x != null) r.getResults().add(new ResultElement(x, 1D));
					}
					
				}
				
			} else {
				throw new VitalServiceException("Expected graph match objects only");
			}
			
		}
		
		return r;
	}

	public VitalSegment addSegment(VitalOrganization organization,
			VitalApp app2, VitalSegment segment) throws VitalServiceException {
		return addSegment(organization, app2, segment, null);
	}
	
	public VitalSegment addSegment(VitalOrganization organization,
			VitalApp app2, VitalSegment segment, VitalProvisioning provisioning) throws VitalServiceException {

		//this will ensure the org/app exist
		ResultList listSegments = listSegments(organization, app2);
		
		String segmentID = (String) segment.getRaw(Property_hasSegmentID.class);
		
		for( Iterator<VitalSegment> iterator = listSegments.iterator(VitalSegment.class); iterator.hasNext(); ) {
			
			VitalSegment existingSegment = iterator.next();
			
			String sid = (String) existingSegment.getRaw(Property_hasSegmentID.class);
			if(segmentID.equals(sid)) {
				throw new VitalServiceException("Segment with ID: " + segmentID + " already exists");
			}
			
		}
		
		GraphObject g = executor.getGraphObject(org, app, systemSegment, segment.getURI(), systemSegmentFull);
		
		if( g != null ) {
			throw new VitalServiceException("Object with URI: " + segment.getURI() + " already exists in system segment, type: " + g.getClass().getCanonicalName());
		}
		
		
		//persist edge and segmetn
		Edge_hasSegment edge = (Edge_hasSegment) new Edge_hasSegment().addSource(app2).addDestination(segment).generateURI(app);
		
		ResultList segmentRL = new ResultList();
		executor.prepareSegmentResources(segmentRL, segment, provisioning);
		
		List<GraphObject> toPersist = new ArrayList<GraphObject>();
		toPersist.add(edge);
		for(GraphObject x : segmentRL) {
			toPersist.add(x);
		}
		
		executor.saveGraphObjects(org, app, systemSegment, toPersist, systemSegmentFull);
		
		reloadCache();
		
		return executor.createSegmentResource(segment, segmentRL);
		
	}

	public VitalApp addApp(VitalOrganization organization, VitalApp app2) throws VitalServiceException {

		
		List<VitalApp> listApps = listApps(organization);

		// TODO
		Integer appsLimit = 100;

		if(appsLimit != null && appsLimit.intValue() > 0 && listApps.size() >= appsLimit.intValue()) {
			throw new VitalServiceException("The applications count limit reached: " + appsLimit.intValue());
		}
		
		String appID = (String) app2.getRaw(Property_hasAppID.class);
		
		for(VitalApp a : listApps) {
			
			String aid = (String) a.getRaw(Property_hasAppID.class);
			if(appID.equals(aid)) {
				throw new VitalServiceException("App with ID: " + appID + " already exists");
			}
			
		}
		
		GraphObject g = executor.getGraphObject(organization, app, systemSegment, app2.getURI(), systemSegmentFull) ;
		
		if( g != null ) {
			throw new VitalServiceException("Object with URI: " + app2.getURI() + " already exists in system segment, type: " + g.getClass().getCanonicalName());
		}
		
		
		Edge_hasApp edge = (Edge_hasApp) new Edge_hasApp().addSource(organization).addDestination(app2).generateURI(app);
		
		executor.saveGraphObjects(org, app, systemSegment, Arrays.asList((GraphObject)edge, app2), systemSegmentFull);
		
		reloadCache();
		
		return app2;
	}
	
	
	public VitalOrganization addOrganization(VitalServiceRootKey rk, VitalOrganization organization) throws VitalServiceException {
		
		rk = checkRootKey(rk);
		
		String organizationID = (String) organization.getRaw(Property_hasOrganizationID.class);
		
		for(VitalOrganization o : listOrganizations()) {
			
			String oid = (String) o.getRaw(Property_hasOrganizationID.class);
			
			if(oid.equals(organizationID)) {
				throw new VitalServiceException("Organization with ID: " + organizationID + " alread exists");
			}
			
		}
		
		GraphObject g = executor.getGraphObject(org, app, systemSegment, organization.getURI(), systemSegmentFull) ;
		
		if( g != null ) {
			throw new VitalServiceException("Object with URI: " + app.getURI() + " already exists in system segment, type: " + g.getClass().getCanonicalName());
		}
		
		Edge_hasOrganization edge = (Edge_hasOrganization) new Edge_hasOrganization().addSource(rk).addDestination(organization).generateURI(app);
		
		executor.saveGraphObjects(org, app, systemSegment, Arrays.asList((GraphObject)organization, edge), systemSegmentFull);
		
		reloadCache();
		
		return organization;
		
		
	}

	public void removeApp(VitalOrganization organization, VitalApp app2) throws VitalServiceException {

		organization = checkOrganization(organization);
		
		VitalGraphQuery appsQuery = SystemSegmentQueries.getAppsQuery(systemSegment, organization);
		
		ResultList rl = handleQuery(appsQuery);
		
//		ResultList rl = executor.query(org, app, SystemSegmentQueries.getAppsQuery(systemSegment, organization), systemSegmentFull);
//		
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		rl = unpackGraphMatch(rl);
		
		//check if app exists
		GraphObject g = rl.get(app2.getURI());
		if(g == null) throw new VitalServiceException("App with URI: " + app2.getURI() + " not found");
		
		Edge_hasApp edge = null;
		
		for(GraphObject x : rl) {
			if(x instanceof Edge_hasApp) {
				Edge_hasApp e = (Edge_hasApp) x;
				if(e.getDestinationURI().equals(app2.getURI())) {
					edge = e;
					break;
				}
			}
		}
		
		Set<String> toDelete = new HashSet<String>();
		toDelete.add(app2.getURI());
		if(edge != null) {
			toDelete.add(edge.getURI());
		}
		
		
		VitalGraphQuery fullAppObject = SystemSegmentQueries.getFullAppObject(systemSegment, organization, app2);
		
//		ResultList rl2 = executor.query(org, app, fullAppObject, systemSegmentFull);
//		if(rl2.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		rl2 = unpackGraphMatch(rl2);
		
		ResultList rl2 = handleQuery(fullAppObject);
		
		List<VitalSegment> segmentsList = new ArrayList<VitalSegment>();
		
		for(GraphObject x : rl2) {
			
			if(x instanceof VitalSegment) {
				segmentsList.add((VitalSegment) x);
			}
			
			toDelete.add(x.getURI());
		}
		
		executor.deleteGraphObjects(org, app, systemSegment, toURIsList(toDelete), systemSegmentFull);
		
		for(VitalSegment s : segmentsList) {
			executor.deleteSegmentResource(s, rl2);
		}
		
		reloadCache();
		
		
		
	}

	protected List<URIProperty> toURIsList(Set<String> toDelete) {

		List<URIProperty> l = new ArrayList<URIProperty>();
		
		for(String s : toDelete) {
			l.add(URIProperty.withString(s));
		}
		
		return l;
	}

	public void removeSegment(VitalOrganization organization,
			VitalApp app2, VitalSegment segment) throws VitalServiceException {

		app2 = checkApp(organization, app2);
		
		VitalGraphQuery segmentQuery = SystemSegmentQueries.getSegmentQuery(systemSegment, organization, app2, segment);
		
		ResultList rl = handleQuery(segmentQuery);
//		ResultList rl = executor.query(org, app, segmentQuery, systemSegmentFull);
//		
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		rl = unpackGraphMatch(rl);
		
		GraphObject x = rl.get(segment.getURI());
		if(x == null) throw new VitalServiceException("Segment with URI: " + segment.getURI() + " not found");
		
		List<URIProperty> toDelete = new ArrayList<URIProperty>();
		
		for(GraphObject g : rl) {
			if(g instanceof Edge_hasSegment || g instanceof VitalSegment || g instanceof VitalProvisioning || g instanceof Edge_hasProvisioning) {
				toDelete.add(URIProperty.withString(g.getURI()));
			}
		}
		
		executor.deleteGraphObjects(org, app, systemSegment, toDelete, systemSegmentFull);
		
		executor.deleteSegmentResource(segment, rl);
		
		reloadCache();
		
	}

	public void removeOrganization(VitalOrganization organization) throws VitalServiceException {
	
		organization = checkOrganization(organization);
		
		VitalGraphQuery organizationToRootKeyQuery = SystemSegmentQueries.getOrganizationToRootKeyQuery(systemSegment, organization);
		
		ResultList rl = handleQuery(organizationToRootKeyQuery);
		
//		ResultList rl = executor.query(org, app, organizationToRootKeyQuery, systemSegmentFull);
//		
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		rl = unpackGraphMatch(rl);
		
		Set<String> toDelete = new HashSet<String>();
		
		toDelete.add(organization.getURI());
		
		for( GraphObject g : rl) {

			toDelete.add(g.getURI());
			
		}
		
		for(VitalApp app : listApps(organization)) {
			
			removeApp(organization, app);
			
		}
		
		
		executor.deleteGraphObjects(org, app, systemSegment, toURIsList(toDelete), systemSegmentFull);
		
		reloadCache();
		
		
	}
	
	public VitalSegment getExistingSegment(VitalOrganization organization,
			VitalApp arg0, VitalSegment arg1) throws VitalServiceException {
		VitalSegment segment = executor.getSegmentResource(arg1);
		if(segment == null) throw new VitalServiceException("Segment resource not found: " + arg1.getURI());
		return segment;
	}

	public List<VitalServiceRootKey> listVitalServiceRootKeys() throws VitalServiceException {
		
		VitalSelectQuery rootKeysQuery = SystemSegmentQueries.getRootKeysQuery(systemSegment);
		
//		ResultList rl = executor.query(org, app, rootKeysQuery, systemSegmentFull);
//		
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
		
		ResultList rl = handleQuery(rootKeysQuery);
		
		List<VitalServiceRootKey> r = new ArrayList<VitalServiceRootKey>();
		
		for( Iterator<VitalServiceRootKey> iterator = rl.iterator(VitalServiceRootKey.class, true); iterator.hasNext(); ) {
			r.add(iterator.next());
		}
		
		return r;
	}

	public VitalServiceRootKey addVitalServiceRootKey(VitalServiceRootKey rootKey) throws VitalServiceException {

		if( listVitalServiceRootKeys().size() > 0 ) throw new VitalServiceException("Root key already exists");
		
		checkKeyUnique(rootKey);
		
		String thisKey = (String) rootKey.getRaw(Property_hasKey.class);
		if(StringUtils.isEmpty(thisKey)) throw new VitalServiceException("root key value must be set");
		
		GraphObject g = executor.getGraphObject(org, app, systemSegment, rootKey.getURI(), systemSegmentFull);
		if(g != null) {
			throw new VitalServiceException("Object with URI: " + rootKey.getURI() + " already exists in system segment, type: " + g.getClass().getCanonicalName());
		}
		
		
		executor.saveGraphObjects(org, app, systemSegment, Arrays.asList((GraphObject)rootKey), systemSegmentFull);
		
		reloadCache();
		
		return rootKey;
		
	}
	

	public Map<String, VitalSegment> listSegmentID2Map(
			VitalOrganization organization, VitalApp app2) throws VitalServiceException {

		Map<String, VitalSegment> m = new HashMap<String, VitalSegment>();
		
		for(VitalSegment s : listSegmentsOnly(organization, app2)) {
			
			String sid = (String) s.getRaw(Property_hasSegmentID.class);
			
			m.put(sid, s);
			
		}
		
		return m;
	}

	public VitalSegment getSegmentByURI(VitalOrganization organization,
			VitalApp app2, VitalSegment targetSegment) throws VitalServiceException {

		String sid = (String) targetSegment.getRaw(Property_hasSegmentID.class);
		
		for(VitalSegment s : listSegmentsOnly(organization, app2)) {
			if(s.getURI().equals(targetSegment.getURI())) {
				String x = (String) s.getRaw(Property_hasSegmentID.class);
				if(!x.equals(sid)) {
					throw new VitalServiceException("Segment with URI found but different ID: input " + sid + ", expected: " + x);
				}
				return s;
			}
		}
		
		return null;
	}

	public VitalApp getApp(VitalOrganization organization, String appID) throws VitalServiceException {

		for(VitalApp app : listApps(organization) ) {
			if(appID.equals(app.getRaw(Property_hasAppID.class))) {
				return app;
			}
		}
		
		return null;
		
	}

	public ResultList listAllOrganizationSegments(VitalOrganization organization) throws VitalServiceException {

		//check organization ? 
		organization = checkOrganization(organization);
		
		VitalGraphQuery allSegmentsQuery = SystemSegmentQueries.getAllSegmentsQuery(systemSegment, organization);
//		ResultList rl = executor.query(org, app, allSegmentsQuery, systemSegmentFull);
//		
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		return unpackGraphMatch(rl);
		return handleQuery(allSegmentsQuery);
		
	}

	//this method is only required for super admin transactions
	public ResultList listAllSegments() throws VitalServiceException {
		
		VitalGraphQuery allSegmentsQueryGlobal = SystemSegmentQueries.getAllSegmentsQueryGlobal(systemSegment);
		
//		ResultList rl = executor.query(org, app, allSegmentsQueryGlobal, systemSegmentFull);
//		
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		return unpackGraphMatch(rl);
		
		return handleQuery(allSegmentsQueryGlobal);
		
	}

	public VitalServiceRootKey getVitalServiceRootKey(String key) throws VitalServiceException {
		
		for(VitalServiceRootKey rk : listVitalServiceRootKeys() ) {

			String k = (String) rk.getRaw(Property_hasKey.class);
			
			if(key.equals(k)) return rk;
			
		}
		
		return null;
	}
	
	public VitalServiceRootKey checkRootKey(VitalServiceRootKey rootKey) throws VitalServiceException{
		
		VitalServiceRootKey k = getVitalServiceRootKey((String) rootKey.getRaw(Property_hasKey.class));
		if(k == null) throw new VitalServiceException("Root key not found");
		return k;
		
	}
	
	

	public VitalStatus addVitalServiceKey(VitalOrganization organization,
			VitalApp app2, VitalServiceKey serviceKey) throws VitalServiceException {

		checkKeyUnique(serviceKey);
		
		if(serviceKey == null) throw new NullPointerException("Service key must not be empty");
		if(StringUtils.isEmpty(serviceKey.getURI())) throw new VitalServiceException("Service key URI must not be null nor empty");
		
		app2 = checkApp(organization, app2);

		GraphObject g = executor.getGraphObject(org, app, systemSegment, serviceKey.getURI(), systemSegmentFull);
		if(g != null) {
			throw new VitalServiceException("Object with URI: " + serviceKey.getURI() + " already exists in system segment, type: " + g.getClass().getCanonicalName());
		}
		
		// TODO
		Integer serviceKeysLimit = 100;

		if(serviceKeysLimit != null && serviceKeysLimit.intValue() > 0) {
			
			VitalGraphQuery appServiceKeys = SystemSegmentQueries.getAppServiceKeys(systemSegment, app2);
			
//			ResultList rl = executor.query(org, app, appServiceKeys, systemSegmentFull);
//			if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//			
//			rl = unpackGraphMatch(rl);
			
			ResultList rl = handleQuery(appServiceKeys);
			
			int c = 0;
			
			for( Iterator<VitalServiceKey> iterator = rl.iterator(VitalServiceKey.class, true); iterator.hasNext(); ) {
				iterator.next();
				c++;
			}
			
			if(c >= serviceKeysLimit.intValue()) throw new VitalServiceException("Service keys limit reached: " + serviceKeysLimit.intValue());
			
		}
		
		
		
		Edge_hasAuthKey edge = (Edge_hasAuthKey) new Edge_hasAuthKey().addSource(app2).addDestination(serviceKey).generateURI(app);
		
		executor.saveGraphObjects(org, app, systemSegment, Arrays.asList((GraphObject) serviceKey, edge), systemSegmentFull);
		
		reloadCache();
		
		return VitalStatus.withOKMessage("Service key added");
	}

	public List<VitalServiceKey> listVitalServiceKeys(
			VitalOrganization organization, VitalApp app2) throws VitalServiceException {

		app2 = checkApp(organization, app2);
		
		VitalGraphQuery appServiceKeys = SystemSegmentQueries.getAppServiceKeys(systemSegment, app2);
		
//		ResultList rl = executor.query(org, app, appServiceKeys, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		rl = unpackGraphMatch(rl);
		
		ResultList rl = handleQuery(appServiceKeys);
		
		List<VitalServiceKey> r = new ArrayList<VitalServiceKey>();
		
		for(Iterator<VitalServiceKey> iterator = rl.iterator(VitalServiceKey.class, true); iterator.hasNext(); ) {
			VitalServiceKey next = iterator.next();
			r.add(next);
		}
		
		return r;
		
	}

	public VitalStatus removeVitalServiceKey(VitalOrganization organization,
			VitalApp app2, VitalServiceKey serviceKey) throws VitalServiceException {

		app2 = checkApp(organization, app2);
		
		VitalGraphQuery appServiceKeys = SystemSegmentQueries.getAppServiceKeys(systemSegment, app2);
		
//		ResultList rl = executor.query(org, app, appServiceKeys, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());		
//		
//		rl = unpackGraphMatch(rl);
		
		ResultList rl = handleQuery(appServiceKeys);
		
		String inputKey = (String) serviceKey.getRaw(Property_hasKey.class);
		
		VitalServiceKey key = null;
		
		for(Iterator<VitalServiceKey> iterator = rl.iterator(VitalServiceKey.class, true); iterator.hasNext(); ) {
			VitalServiceKey next = iterator.next();
			if(next.getRaw(Property_hasKey.class).equals(inputKey)) {
				key = next;
				break;
			}
		}
		
		if(key == null) throw new VitalServiceException("Key not found");
		
		List<URIProperty> toDelete = new ArrayList<URIProperty>();
		toDelete.add(URIProperty.withString(key.getURI()));
		
		for(Iterator<Edge_hasAuthKey> iterator = rl.iterator(Edge_hasAuthKey.class, true); iterator.hasNext(); ) {
			
			Edge_hasAuthKey edge = iterator.next();
			
			if(edge.getDestinationURI().equals(key.getURI())) {
				toDelete.add(URIProperty.withString(edge.getURI()));
			}
			
		}
		
		executor.deleteGraphObjects(org, app, systemSegment, toDelete, systemSegmentFull);
	
		reloadCache();
		
		return VitalStatus.withOKMessage("Service key deleted");
	}

	public VitalStatus addVitalServiceAdminKey(VitalOrganization organization,
			VitalServiceAdminKey serviceAdminKey) throws VitalServiceException {
		
		checkKeyUnique(serviceAdminKey);
		
		if(serviceAdminKey == null) throw new NullPointerException("Service admin key must not be empty");
		if(StringUtils.isEmpty(serviceAdminKey.getURI())) throw new VitalServiceException("Service admin key URI must not be null nor empty");
		
		organization = checkOrganization(organization);

		GraphObject g = executor.getGraphObject(org, app, systemSegment, serviceAdminKey.getURI(), systemSegmentFull);
		if(g != null) {
			throw new VitalServiceException("Object with URI: " + serviceAdminKey.getURI() + " already exists in system segment, type: " + g.getClass().getCanonicalName());
		}

		// TODO
		Integer adminKeysLimit = 100;


		if(adminKeysLimit != null && adminKeysLimit.intValue() > 0) {
			
			VitalGraphQuery organizationServiceAdminKeys = SystemSegmentQueries.getOrganizationServiceAdminKeys(systemSegment, organization);
			
//			ResultList rl = executor.query(org, app, organizationServiceAdminKeys, systemSegmentFull);
//			if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//			
//			rl = unpackGraphMatch(rl);
			
			ResultList rl = handleQuery(organizationServiceAdminKeys);
			
			int c = 0;
			for( Iterator<VitalServiceAdminKey> iterator = rl.iterator(VitalServiceAdminKey.class, true); iterator.hasNext(); ) {
				iterator.next();
				c++;
			}
			
			if(c >= adminKeysLimit.intValue()) throw new VitalServiceException("Admin keys limit reached: " + adminKeysLimit.intValue());
			
		}
		
		
	
		Edge_hasAuthKey edge = (Edge_hasAuthKey) new Edge_hasAuthKey().addSource(organization).addDestination(serviceAdminKey).generateURI(app);
		
		executor.saveGraphObjects(org, app, systemSegment, Arrays.asList((GraphObject) serviceAdminKey, edge), systemSegmentFull);
		
		reloadCache();
		
		return VitalStatus.withOKMessage("Service key added");
	}

	protected void checkKeyUnique(VitalAuthKey k) throws VitalServiceException {

		if(k.getURI() == null) throw new VitalServiceException("key uri not set");
		
		String key = (String) k.getRaw(Property_hasKey.class);
		
		try {
			VitalAuthKeyValidation.validateKey(key);
		} catch (VitalAuthKeyValidationException e) {
			throw new VitalServiceException("Key validation failed: " + e.getLocalizedMessage());
		}
		
		VitalSelectQuery sq = SystemSegmentQueries.getAuthKeyQuery(systemSegment, key);
		
//		ResultList rl = executor.query(org, app, sq, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
		
		ResultList rl = handleQuery(sq);
		
		if(rl.getResults().size() > 0) throw new VitalServiceException("Key string already found in the database: " + key);
		
	}

	public VitalStatus removeVitalServiceAdminKey(
			VitalOrganization organization, VitalServiceAdminKey serviceAdminKey) throws VitalServiceException {
		
		organization = checkOrganization(organization);
		
		VitalGraphQuery organizationServiceAdminKeys = SystemSegmentQueries.getOrganizationServiceAdminKeys(systemSegment, organization);
		
		ResultList rl = handleQuery(organizationServiceAdminKeys);
		
//		ResultList rl = executor.query(org, app, organizationServiceAdminKeys, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());		
//		
//		rl = unpackGraphMatch(rl);
		
		String inputKey = (String) serviceAdminKey.getRaw(Property_hasKey.class);
		
		VitalServiceAdminKey key = null;
		
		for(Iterator<VitalServiceAdminKey> iterator = rl.iterator(VitalServiceAdminKey.class, true); iterator.hasNext(); ) {
			VitalServiceAdminKey next = iterator.next();
			if(next.getRaw(Property_hasKey.class).equals(inputKey)) {
				key = next;
				break;
			}
		}
		
		if(key == null) throw new VitalServiceException("Key not found");
		
		List<URIProperty> toDelete = new ArrayList<URIProperty>();
		toDelete.add(URIProperty.withString(key.getURI()));
		
		for(Iterator<Edge_hasAuthKey> iterator = rl.iterator(Edge_hasAuthKey.class, true); iterator.hasNext(); ) {
			
			Edge_hasAuthKey edge = iterator.next();
			
			if(edge.getDestinationURI().equals(key.getURI())) {
				toDelete.add(URIProperty.withString(edge.getURI()));
			}
			
		}
		
		executor.deleteGraphObjects(org, app, systemSegment, toDelete, systemSegmentFull);
		
		reloadCache();
		
		return VitalStatus.withOKMessage("Service admin key deleted");
	}

	public List<VitalServiceAdminKey> listVitalServiceAdminKeys(
			VitalOrganization organization) throws VitalServiceException {
		
		organization = checkOrganization(organization);
		
		VitalGraphQuery organizationServiceAdminKeys = SystemSegmentQueries.getOrganizationServiceAdminKeys(systemSegment, organization);
		
//		ResultList rl = executor.query(org, app, organizationServiceAdminKeys, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		rl = unpackGraphMatch(rl);
		
		ResultList rl = handleQuery(organizationServiceAdminKeys);
		
		List<VitalServiceAdminKey> r = new ArrayList<VitalServiceAdminKey>();
		
		for(Iterator<VitalServiceAdminKey> iterator = rl.iterator(VitalServiceAdminKey.class, true); iterator.hasNext(); ) {
			VitalServiceAdminKey next = iterator.next();
			r.add(next);
		}
		
		return r;
	}

	public List<VitalAuthKey> listKeys(String key) throws VitalServiceException {
		
		
		VitalSelectQuery sq = SystemSegmentQueries.getAuthKeyQuery(systemSegment, key);
		
//		ResultList rl = executor.query(org, app, sq, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
		
		ResultList rl = handleQuery(sq);
		
		List<VitalAuthKey> keys = new ArrayList<VitalAuthKey>();
		
		for( Iterator<VitalAuthKey> iterator = rl.iterator(VitalAuthKey.class, false); iterator.hasNext(); ) {
			
			VitalAuthKey k = iterator.next();
			
			keys.add(k);
		}
		
		return keys;
				
	}
	
	
	public ResultList getServiceKeyAppOrg(VitalServiceKey serviceKey) throws VitalServiceException {
		
		VitalGraphQuery gq = SystemSegmentQueries.getServiceKeyAppOrg(systemSegment, serviceKey);
		
//		ResultList rl = executor.query(org, app, gq, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//
//		rl = unpackGraphMatch(rl);

		ResultList rl = handleQuery(gq);
		
		return rl;
	}

	public ResultList getServiceAdminKeyOrg(VitalServiceAdminKey serviceAdminKey) throws VitalServiceException {

		VitalGraphQuery gq = SystemSegmentQueries.getServiceAdminKeyOrg(systemSegment, serviceAdminKey);
		
//		ResultList rl = executor.query(org, app, gq, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//
//		rl = unpackGraphMatch(rl);
		
		ResultList rl = handleQuery(gq);
		
		return rl;
	}
	
	public void close() {
		
		if(cachedSegment != null) {
			
			try {
				cachedSegment.close();
			} catch(Exception e){}
			
		}
		
	}
	
}
