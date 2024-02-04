package ai.vital.vitalservice.command

import groovy.cli.picocli.CliBuilder
import org.apache.commons.io.FileUtils;

import ai.vital.vitalservice.BaseDowngradeUpgradeOptions
import ai.vital.vitalservice.EndpointType
import ai.vital.vitalservice.ServiceOperations
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.factory.VitalServiceFactory;

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import ai.vital.vitalservice.impl.UpgradeDowngradeProcedure
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalSegment

class AbstractCommand {

	public static boolean exit = true
	
	static void error(String m) {
		System.err.println(m)
		if(exit)
			System.exit(1)
		else throw new RuntimeException(m)
	}
	
	
	static def CMD_DATA_MIGRATE = 'datamigrate'
	
	static def getDataMigrateCLI(String cmd) {
		
		def cli = new CliBuilder(usage: cmd + ' ' + CMD_DATA_MIGRATE + ' [options]', stopAtNonOption: false)
		cli.with {
			h longOpt: 'help', 'display usage', args: 0, required: false
			i longOpt: "input", "overrides source segment in a builder", args: 1, required: false
			o longOpt: "output", "overrides destination segment in a builder", args: 1, required: false
			dss longOpt: 'delete-source-segment', '[true, false] overrides deleteSourceSegment in a builder', args: 1, required: false
			b longOpt: "builder", "builder file, .groovy or .builder extension", args: 1, required: false
			owlfile longOpt: "owl-file", "older owl file name option, required in builderless mode", args: 1, required: false
			d longOpt: "direction", "[upgrade, dowgrade], required in builderless mode", args:1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			a longOpt: 'appID', 'app ID', args:1, required: true
		}
		
		return cli
		
	}
	
	public static void handleDataMigrate(String cmd, def options, VitalServiceAdmin serviceAdmin, VitalApp app) {
		
		File builderFile = options.b ? new File(options.b) : null
		
		String inputSegment = options.i ? options.i : null
		String outputSegment = options.o ? options.o : null
		
		String owlfile = options.owlfile ? options.owlfile : null
		
		boolean overwrite = options.ow ? true : false
		
		Boolean dss = options.dss ? Boolean.parseBoolean(options.dss) : null
		
		println "app: ${app.appID}"
		println "builder file: ${builderFile?.absolutePath}"
		println "input segment: ${inputSegment}"
		println "output segment: ${outputSegment}"
		println "delete source segment ? ${dss}"
		
		String direction = options.d ? options.d : null;
		
		ServiceOperations ops = null;
		
		if(builderFile) {
		
			println "using builder file"
		
			if(owlfile) println ("ignoring owlfile")
			if(direction) println ("ignoring direction param")
			
			if(!(builderFile.name.endsWith(".groovy") || builderFile.name.endsWith(".builder") )) {
				error("builder file name invalid: ${builderFile.absolutePath}, must be a .groovy or .builder file")
				return
			}
			
			if(!builderFile.isFile()) {
				error("builder file does not exist or not a file: ${builderFile.absolutePath}")
				return
			}
			
				
			ops = UpgradeDowngradeProcedure.parseUpgradeDowngradeBuilder(FileUtils.readFileToString(builderFile, "UTF-8"))
			
			BaseDowngradeUpgradeOptions opts = null;
			
			if( ops.getDowngradeOptions() ) {
				opts = ops.getDowngradeOptions()
			} else if(ops.getUpgradeOptions()) {
				opts = ops.getUpgradeOptions()
			} else {
				error("No UPGRADE/DOWNGRADE options")
				return;
			}
			
			if(opts.destinationPath || opts.sourcePath) {
				error("$cmd does not accept source/destination segment")
				return
			}
			
			if(inputSegment) {
				println "overriding builder source segment: ${inputSegment}"
				opts.setSourceSegment(inputSegment)
			} else {
				if(!opts.getSourceSegment()) {
					error("no source path in builder nor cli param")
					return
				}
				inputSegment = opts.getSourceSegment()
			}
			
			if(outputSegment) {
				println "overriding builder destination segment: ${outputSegment}"
				opts.setDestinationSegment(outputSegment)
			} else {
				if(!opts.getDestinationSegment()) {
					error("no destination path in builder nor cli param")
					return
				}
				outputSegment = opts.getDestinationSegment()
			}
			
			if(dss != null) {
				println "overriding deleteSourceSegment flag: ${dss}"
				opts.setDeleteSourceSegment(dss.booleanValue())
			}

		} else {
		
			println "no builder file, on-the-fly migration"

			if(inputSegment == null) {
				error("no input segment, required in builderless mode")
				return
			}
			
			if(outputSegment == null) {
				error("no output segment, required in builderless mode")
				return
			}
			
			if(!direction) {
				error("no direction set, required in builderless mode")
				return
			}
			
			if(!(direction == 'upgrade' || direction == 'downgrade')) {
				error("invalid direction: $direction, valid values: 'upgrade', 'downgrade'")
				return
			}
			
			if(!owlfile) {
				error("no owlfile set, required in builderless mode")
				return
			}
			
			String builderContent = """\

${direction.toUpperCase()} {

	value sourceSegment: '${inputSegment}'

	value destinationSegment: '${outputSegment}'

	value oldOntologyFileName: '${owlfile}'

	value deleteSourceSegment: ${dss != null ? dss.booleanValue() : false}

}
"""

			ops = UpgradeDowngradeProcedure.parseUpgradeDowngradeBuilder(builderContent)
		
			
		}
		
		VitalStatus status = serviceAdmin.doOperations(app, ops)
		
		println "" + status.status + " - " + status.message
		
	}
	
	
	protected static Config endpointConfigPart(def options, EndpointType expected) {
		
		String profile = options.prof ? options.prof : null
		if(profile != null) {
			println "Using vitalservice profile: ${profile}"
			
			if( ! VitalServiceFactory.getAvailableProfiles().contains(profile) ) {
				System.err.println("Service profile '$profile' not found")
				System.exit(1)
			}
			
		} else {
			profile = VitalServiceFactory.DEFAULT_PROFILE
			println "using default vitalservice profile: ${profile}"
		}
		
		File confFile = VitalServiceFactory.getConfigFile()
		println "Config file location: ${confFile.absolutePath}"
		Config cfg = ConfigFactory.parseFile(confFile)
		
		EndpointType et = EndpointType.fromString(cfg.getString("profile.${profile}.type"))
		if( expected != et) {
			System.err.println "invalid endpoint type: ${et.name}, expected: ${expected.name}"
			System.exit(1)
		}
		
		return cfg.getConfig("profile.${profile}")
		
	}
	
	protected static void handleListApps(def options, VitalServiceAdmin serviceAdmin) {
		
		println "Listing apps..."
		
		List<VitalApp> apps = serviceAdmin.listApps()
		
		println "Apps count: ${apps.size()}"
		
		for(int i = 0; i < apps.size(); i++) {
			
			VitalApp app = apps[i];
			
			println "${i+1}. ${app.appID}  -  ${app.name}"
			
		}
		
	}
	
	protected static void handleAddApp(def options, VitalServiceAdmin serviceAdmin) {
		
		String appID = options.a
		
		String name = options.n
		
		VitalApp app = getApp(serviceAdmin, appID)
		
		if(app != null) {
			error "App already exists: ${appID}"
			return
		}
		
		println "Adding appID: ${appID} name: ${name}"
		app = VitalApp.withId(appID)
		app.name = name
		
		VitalStatus status = serviceAdmin.addApp(app)
		
		println "Status: ${status}"
		
	} 
	
	protected static void handleRemoveApp(def options, VitalServiceAdmin serviceAdmin) {
		
		String appID = options.a
		
		VitalApp app = getApp(serviceAdmin, appID);
		
		if(app == null) return
		
		VitalStatus status = serviceAdmin.removeApp(app);
		
		println "Status: ${status}"

		
	}
	
	protected static void handleListSegments(def options, VitalServiceAdmin serviceAdmin) {
		
		String appID = options.a
		
		println "Listing segments for app: ${appID}"
		
		VitalApp app = getApp(serviceAdmin, appID)
		
		if(app == null) return
		
		List<VitalSegment> segments = serviceAdmin.listSegments(app)
		
		println "Segments count: ${segments.size()}"
		
		for(int i = 0 ; i < segments.size(); i++) {
			VitalSegment segment = segments[i]
			println "${i+1}. " + printBasicSegment(segment, '')
		}
		
	}
	
	protected static void handleListSegmentsService(VitalService service) {
		
		println "Listing segments for service app: ${service.getApp().appID}"
		
		List<VitalSegment> segments = service.listSegments()
		
		println "Segments count: ${segments.size()}"
		
		for(int i = 0 ; i < segments.size(); i++) {
			VitalSegment segment = segments[i]
			println "${i+1}. " + printBasicSegment(segment, '')
		}
		
		
	}
	
	protected static void handleAddSegment(def options, VitalServiceAdmin serviceAdmin) {
		
		String segmentID = options.s
		String appID = options.a
		Boolean readOnly = options.ro
		
		println "SegmentID: ${segmentID}"
		println "AppID: ${appID}"
		println "ReadOnly: ${readOnly}"
		
		VitalApp app = getApp(serviceAdmin, appID)
		
		if(app == null) return
		
		VitalSegment segment = VitalSegment.withId(segmentID)
		segment.readOnly = readOnly
		
		try {
			serviceAdmin.addSegment(app, segment, true)
			println "Segment added"
		} catch(Exception e) {
			error(e.localizedMessage)
		}
	
		
	}
	
	
	protected static void handleRemoveSegment(def options, VitalServiceAdmin serviceAdmin) {
		
		String segmentID = options.s
		String appID = options.a
		Boolean deleteData = options.d
				
		println "Removing segment ${segmentID} from app: ${appID}, delete data: ${deleteData} ..."
				
		VitalApp app = getApp(serviceAdmin, appID)
				
		if(app == null) return
		
		
		VitalSegment segment = serviceAdmin.getSegment(app, segmentID)
		
		if(segment == null) {
			error "Segment not found, appID: ${appID} segmentID: ${segmentID}"
			return
		}
						
		VitalStatus status = serviceAdmin.removeSegment(app, segment, deleteData)
						
		println "${status}"
		
	}
	
	
	static String printBasicSegment(VitalSegment segment, String indent) {
		return ( indent + "${segment.class.simpleName} ${segment.segmentID}   ReadOnly? ${segment.readOnly} URI: ${segment.URI}")
	}
	
	protected static VitalApp getApp(VitalServiceAdmin serviceAdmin, String appID) {
		
		VitalApp app = serviceAdmin.getApp(appID)
		
		if(app == null) {
			println "App not found: ${appID}"
			return null
		}
		
		return app
		
	}
	
}
