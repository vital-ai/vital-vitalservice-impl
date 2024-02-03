package ai.vital.prime.service.model


import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry

class SegmentProvisioningUtil {

	// simple encapsulation of getting a vital service model object
	// in and out of dynamodb table.
	// vital service model objects are statically defined
	// so can "hardcode" mapping, such as User.name to a field in the User table
	
	// model object to Item
	
	// Item to model object
	
	static List<String> mainKeys = ['Node', 'Edge', 'HyperNode', 'HyperEdge']
	
	public static SegmentProvisioning provisioningConfigFromString(String s) {
		
		Config cfg = ConfigFactory.parseString(s)
		
		SegmentProvisioning r = new SegmentProvisioning()
		
		String p = "Properties";
		
		Config pc = cfg.getConfig(p);
		r.Properties_read = pc.getLong("read");
		r.Properties_write = pc.getLong("write");
		r.Properties_string_index_read = pc.getLong("string_index_read");
		r.Properties_string_index_write = pc.getLong("string_index_write");
		r.Properties_number_index_read = pc.getLong("number_index_read");
		r.Properties_number_index_write = pc.getLong("number_index_write");		
		
		for(String k : mainKeys) {
			
			Config c = cfg.getConfig(k);
			
			r["${k}_stored"] = c.getBoolean('stored')
			r["${k}_indexed"] = c.getBoolean('indexed')
			
			r["${k}_read"] = c.getLong('read')
			r["${k}_write"] = c.getLong('write')
			
		}
				
		for(Entry e : cfg.entrySet()) {
			String k = e.getKey()
			if(k.contains('.')) continue
			if( !mainKeys.contains(e.getKey()) ) throw new RuntimeException("Unknown provisioning key: ${e.getKey()}")
		}
		
		return r
		
	}
	
	public static String provisioningConfigToString(SegmentProvisioning r) {
		
		
		Map out = [:]
		
		
		Map<String, Object> pv = new LinkedHashMap<String, Object>();
		
		pv.put("read", r.getProperties_read());
		pv.put("write", r.getProperties_write());
		pv.put("number_index_read", r.getProperties_number_index_read());
		pv.put("number_index_write", r.getProperties_number_index_write());
		
		pv.put("string_index_read", r.getProperties_string_index_read());
		pv.put("string_index_write", r.getProperties_string_index_write());
		
		out.put("Properties", pv);
		
		
		
		
		for(String k : mainKeys) {
			
			Map v = new LinkedHashMap()
			
			v.put('stored', r["${k}_stored"])
			v.put('indexed', r["${k}_indexed"])
			
			v.put('read', r["${k}_read"])
			v.put('write', r["${k}_write"])
			
			out.put(k, v)
			
		}
		
		Config cfg = ConfigFactory.parseMap(out)
		
		ConfigRenderOptions defaults = ConfigRenderOptions.defaults();
		defaults.setComments(false);
		defaults.setFormatted(true);
		defaults.setJson(false);
		String newConf = cfg.root().render(defaults);
		
		return newConf
		
	}
	
	
}
