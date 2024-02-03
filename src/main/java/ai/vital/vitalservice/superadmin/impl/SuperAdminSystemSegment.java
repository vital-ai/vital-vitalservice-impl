package ai.vital.vitalservice.superadmin.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import ai.vital.superadmin.domain.VitalServiceSuperAdminKey;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.impl.SystemSegment;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalsigns.model.Edge_hasAuthKey;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.properties.Property_hasKey;
import ai.vital.vitalsigns.model.property.URIProperty;

public class SuperAdminSystemSegment extends SystemSegment {

	public SuperAdminSystemSegment(SystemSegmentOperationsExecutor executor,
			boolean cache) {
		super(executor, cache);
	}

	public SuperAdminSystemSegment(SystemSegmentOperationsExecutor executor) {
		super(executor);
	}

public List<VitalServiceSuperAdminKey> listVitalServiceSuperAdminKeys(VitalServiceRootKey rootKey) throws VitalServiceException {
		
		rootKey = checkRootKey(rootKey);
		
		VitalGraphQuery keysQuery = SuperAdminSystemSegmentQueries.getSuperAdminKeysQuery(systemSegment, rootKey);
		
//		ResultList rl = executor.query(org, app, keysQuery, systemSegmentFull);
//		
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());
//		
//		rl = unpackGraphMatch(rl);
		
		ResultList rl = handleQuery(keysQuery);
		
		List<VitalServiceSuperAdminKey> r = new ArrayList<VitalServiceSuperAdminKey>();
		
		for( Iterator<VitalServiceSuperAdminKey> iterator = rl.iterator(VitalServiceSuperAdminKey.class, true); iterator.hasNext(); ) {
			r.add(iterator.next());
		}
		
		return r;
	}

	
	public VitalStatus removeVitalServiceSuperAdminKey(
			VitalServiceRootKey rootKey, VitalServiceSuperAdminKey serviceSuperAdminKey) throws VitalServiceException {
		
		rootKey = checkRootKey(rootKey);
		
		VitalGraphQuery superAdminKeysQuery = SuperAdminSystemSegmentQueries.getSuperAdminKeysQuery(systemSegment, rootKey);
		
		
//		ResultList rl = executor.query(org, app, superAdminKeysQuery, systemSegmentFull);
//		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new VitalServiceException("Error when querying system segment: " + rl.getStatus().getMessage());		
//		
//		rl = unpackGraphMatch(rl);
		
		ResultList rl = handleQuery(superAdminKeysQuery);
		
		String inputKey = (String) serviceSuperAdminKey.getRaw(Property_hasKey.class);
		
		VitalServiceSuperAdminKey key = null;
		
		for(Iterator<VitalServiceSuperAdminKey> iterator = rl.iterator(VitalServiceSuperAdminKey.class, true); iterator.hasNext(); ) {
			VitalServiceSuperAdminKey next = iterator.next();
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
		
		return VitalStatus.withOKMessage("Service super admin key deleted");
	}
	
	
	public VitalServiceSuperAdminKey addVitalServiceSuperAdminKey(VitalServiceRootKey rootKey,
			VitalServiceSuperAdminKey superAdminKey) throws VitalServiceException {

		checkKeyUnique(superAdminKey);
		
		rootKey = checkRootKey(rootKey);

		GraphObject g = executor.getGraphObject(org, app, systemSegment, superAdminKey.getURI(), systemSegmentFull);
		if(g != null) {
			throw new VitalServiceException("Object with URI: " + superAdminKey.getURI() + " already exists in system segment, type: " + g.getClass().getCanonicalName());
		}
		
		Edge_hasAuthKey edge = (Edge_hasAuthKey) new Edge_hasAuthKey().addSource(rootKey).addDestination(superAdminKey).generateURI(app);
		
		executor.saveGraphObjects(org, app, systemSegment, Arrays.asList((GraphObject)superAdminKey, edge), systemSegmentFull);
		
		reloadCache();
		
		return superAdminKey;
		
	}
	

}
