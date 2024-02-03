package ai.vital.lucene.command

import java.util.List;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.command.AbstractCommand;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import java.util.Map.Entry
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ai.vital.vitalservice.EndpointType;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.lucene.model.LuceneSegmentType;
import ai.vital.service.lucene.impl.LuceneServiceDiskImpl;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.factory.VitalServiceFactory.ServiceConfigWrapper
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey;


class VitalLuceneCommand extends AbstractCommand {

	static String CMD_HELP = 'help'
	
	static String CMD_INIT = 'init'
	
	static String CMD_LIST_APPS = 'listapps'
	
	static String CMD_REMOVE_APP = 'removeapp'
	
	static String CMD_ADD_APP = 'addapp'
	
	static String CMD_LIST_SEGMENTS = 'listsegments'
	
	static String CMD_ADD_SEGMENT = 'addsegment'
	
	static String CMD_REMOVE_SEGMENT = 'removesegment'
	
	static String VL = 'vitallucene'
	
	static Map cmd2CLI = [:]
	
	static {
		
		def initCLI = new CliBuilder(usage: "${VL} ${CMD_INIT} [options]", stopAtNonOption: false)
		initCLI.with {
			f longOpt: 'force', 'override existing directory', args: 0, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			rk longOpt: 'root-key', 'optional root key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_INIT, initCLI)
		
		def listAppsCLI = new CliBuilder(usage: "${VL} ${CMD_LIST_APPS} [options]", stopAtNonOption: false)
		listAppsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_APPS, listAppsCLI)
		
		def addAppCLI = new CliBuilder(usage: "${VL} ${CMD_ADD_APP} [options]", stopAtNonOption: false)
		addAppCLI.with {
			a longOpt: 'app', 'app ID', args: 1, required: true
			n longOpt: 'name', 'app name', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_ADD_APP, addAppCLI)
		
		def removeAppCLI = new CliBuilder(usage: "${VL} ${CMD_REMOVE_APP} [options]", stopAtNonOption: false)
		removeAppCLI.with {
			a longOpt: 'app', 'app ID', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_REMOVE_APP, removeAppCLI)
		
		def listSegmentsCLI = new CliBuilder(usage: "${VL} ${CMD_LIST_SEGMENTS} [options]", stopAtNonOption: false)
		listSegmentsCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
//			s longOpt: 'segment', 'segment ID', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_SEGMENTS, listSegmentsCLI)
		
		def removeSegmentCLI = new CliBuilder(usage: "${VL} ${CMD_REMOVE_SEGMENT} [options]", stopAtNonOption: false)
		removeSegmentCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			d longOpt: 'deleteData', 'delete data', args: 0, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_REMOVE_SEGMENT, removeSegmentCLI)
		
		def addSegmentCLI = new CliBuilder(usage: "${VL} ${CMD_ADD_SEGMENT} [options]", stopAtNonOption: false)
		addSegmentCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			ro longOpt: 'readOnly', 'read only', args: 0, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_ADD_SEGMENT, addSegmentCLI)
		
		
		def datamigrateCLI = getDataMigrateCLI(VL)
		cmd2CLI.put(CMD_DATA_MIGRATE, datamigrateCLI)
	}	
	
	public static void main(String[] args) {
		
		
		String cmd = args.length > 0 ? args[0] : null
	
		boolean printHelp = args.length == 0 || cmd == CMD_HELP
		
		if(printHelp) {
			usage();
			return;
		}
		
		String[] params = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0]
		
		def cli = cmd2CLI.get(cmd)
		
		if(!cli) {
			System.err.println "unknown command: ${cmd}"
			usage()
			return
		} 
		
		def options = cli.parse(params)
		if(!options) {
			return
		}
		
		
		Config cfg = endpointConfigPart(options, EndpointType.LUCENEDISK)
		
		
		String profile = options.prof ? options.prof : VitalServiceFactory.DEFAULT_PROFILE
		
		println "Profile: $profile"
		
		if(cmd == CMD_INIT) {
			
			ServiceConfigWrapper wrappedConfig = VitalServiceFactory.getProfileConfig(profile)
			
			VitalServiceLuceneDiskConfig ldConfig = (VitalServiceLuceneDiskConfig) wrappedConfig.serviceConfig
			
			File rootPath = new File(ldConfig.rootPath)
			
			boolean force = options.f
					
			println "force initialization ? ${force}"
			
			println "lucene disk impl. root: ${rootPath.absolutePath}"
			
			String rootKey = options.rk ? options.rk : null
			
			if(rootPath.exists() && !force) {
				error "root path exists but force option is not set - exiting..."
				return
			}
			
			println "initializing..."

			if( VitalServiceFactory.isInitialized(ldConfig).status == VitalStatus.Status.ok ) {
				error "Service is already initialized, exiting"
				return
			}
			
			
			VitalServiceRootKey rk = null;
			if(rootKey) {
				println "Initlializing with root key: $rootKey"
				rk = (VitalServiceRootKey) new VitalServiceRootKey().generateURI((VitalApp)null)
				rk.key = rootKey
			} else {
				println "Initializing with random key"
			}
			
			rk = VitalServiceFactory.initService(ldConfig, rk, null)
			
			if(!rootKey) {
				println "Generated root key: ${rk.key}"
			}			
			
			println "DONE"
			
			return 
		}
		
		VitalServiceAdmin serviceAdmin = null
		VitalServiceAdminKey key = new VitalServiceAdminKey()
		key.generateURI((VitalApp) null)
		key.key = 'abcd-efgh-ijkl'
		serviceAdmin = VitalServiceFactory.openAdminService(key, profile)
		 
		if(cmd == CMD_LIST_APPS) {
		
			handleListApps(options, serviceAdmin)
			
		} else if(cmd == CMD_ADD_APP) {
		
			handleAddApp(options, serviceAdmin)
			
		} else if(cmd == CMD_REMOVE_APP) {

			handleRemoveApp(options, serviceAdmin)		
			
		} else if(cmd == CMD_LIST_SEGMENTS) {
		
			handleListSegments(options, serviceAdmin)
			
		} else if(cmd == CMD_ADD_SEGMENT) {

			handleAddSegment(options, serviceAdmin)
			
		} else if(cmd == CMD_REMOVE_SEGMENT) {
		
			handleRemoveSegment(options, serviceAdmin)
			
		} else if(cmd == CMD_DATA_MIGRATE) {
		
			String appID = options.a
			
			VitalApp app = getApp(serviceAdmin, appID)
			
			if(app == null) return
		
			handleDataMigrate(VL, options, serviceAdmin, app)	
		
		} else {
			
			error "Unhandled command: ${cmd}"	
		
		}

	}
	
	static void usage() {
		
		println "usage: ${VL} <command> [options] ..."
		
		println "usage: ${VL} ${CMD_HELP} (prints usage)"
		
		for(Entry e : cmd2CLI.entrySet()) {
			e.value.usage()
		}

	}

	
}
