package ai.vital.vitalservice.impl.query;

import java.util.ArrayList;
import java.util.List;

import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.VitalApp;

public class ToSparqlImplementation {

	public static List<String> toSparql(VitalQuery query, Object... args) throws Exception {
	
		//cloned
		VitalQuery cloned = (VitalQuery) query.clone();
		cloned.setReturnSparqlString(true);
		
		VitalService vitalService = VitalSigns.get().getVitalService();
		
		VitalServiceAdmin vitalServiceAdmin = VitalSigns.get().getVitalServiceAdmin();
		
		ResultList rl = null;
		
		if(vitalService != null) {
			
			rl = vitalService.query(cloned);

			
		} else if(vitalServiceAdmin != null) {
			
			if(args == null || args.length < 1) throw new RuntimeException("VitalAdminService is active, toSparql requires app parameter");
			
			if(!(args[0] instanceof VitalApp)) throw new RuntimeException("VitalAdminService is active, toSparql() first parameter is expected to be an App");

			VitalApp app = (VitalApp) args[0];
			
			rl = vitalServiceAdmin.query(app, cloned);
			
		} else {
			throw new RuntimeException("No active service, toSparql() won't run");
		}
		
		if(rl.getStatus().getStatus() != VitalStatus.Status.ok) {
			throw new RuntimeException(rl.getStatus().getMessage());
		}
		
		String[] s = rl.getStatus().getMessage().split("\n###\n");
		List<String> r = new ArrayList<String>();
		for(String x : s) {
			x = x.trim();
			if(!x.isEmpty()) r.add(x);
		}
		return r;
		
	}
}
