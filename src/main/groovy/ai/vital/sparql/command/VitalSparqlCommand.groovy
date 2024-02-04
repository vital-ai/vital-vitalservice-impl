package ai.vital.sparql.command

import groovy.cli.picocli.CliBuilder

import java.util.Map;
import java.util.Map.Entry

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;

import ai.vital.allegrograph.service.VitalServiceAllegrographConfigCreator;
import ai.vital.allegrograph.service.admin.VitalServiceAdminAllegrograph;
import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig;
import ai.vital.vitalservice.EndpointType;
import ai.vital.triplestore.allegrograph.AllegrographWrapper;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.admin.VitalServiceAdmin
import ai.vital.vitalservice.command.AbstractCommand;
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.factory.VitalServiceFactory.ServiceConfigWrapper
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalServiceAdminKey
import ai.vital.vitalsigns.model.VitalServiceRootKey

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory


class VitalSparqlCommand extends AbstractCommand {

	static String CMD_HELP = 'help'
	
	static String CMD_INIT = 'init'
	
	static String CMD_LIST_APPS = 'listapps'
	
	static String CMD_REMOVE_APP = 'removeapp'
	
	static String CMD_ADD_APP = 'addapp'
	
	static String CMD_LIST_SEGMENTS = 'listsegments'
	
	static String CMD_ADD_SEGMENT = 'addsegment'
	
	static String CMD_REMOVE_SEGMENT = 'removesegment'
	
	static String VSP = 'vitalsparql'
	
	static Map cmd2CLI = [:]
	
	static {
		
		def listAppsCLI = new CliBuilder(usage: "${VSP} ${CMD_LIST_APPS} [options]", stopAtNonOption: false)
		listAppsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_APPS, listAppsCLI)
		
		def initCLI = new CliBuilder(usage: "${VSP} ${CMD_INIT}", stopAtNonOption: false)
		initCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			rk longOpt: 'root-key', 'optional root key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_INIT, initCLI)
		

		def addAppCLI = new CliBuilder(usage: "${VSP} ${CMD_ADD_APP} [options]", stopAtNonOption: false)
		addAppCLI.with {
			a longOpt: 'app', 'app ID', args: 1, required: true
			n longOpt: 'name', 'app name', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_ADD_APP, addAppCLI)
		
		def removeAppCLI = new CliBuilder(usage: "${VSP} ${CMD_REMOVE_APP} [options]", stopAtNonOption: false)
		removeAppCLI.with {
			a longOpt: 'app', 'app ID', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_REMOVE_APP, removeAppCLI)

		
		def listSegmentsCLI = new CliBuilder(usage: "${VSP} ${CMD_LIST_SEGMENTS} [options]", stopAtNonOption: false)
		listSegmentsCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
//			s longOpt: 'segment', 'segment ID', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_SEGMENTS, listSegmentsCLI)
		
		def removeSegmentCLI = new CliBuilder(usage: "${VSP} ${CMD_REMOVE_SEGMENT} [options]", stopAtNonOption: false)
		removeSegmentCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			d longOpt: 'deleteData', 'delete data', args: 0, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_REMOVE_SEGMENT, removeSegmentCLI)
		
		def addSegmentCLI = new CliBuilder(usage: "${VSP} ${CMD_ADD_SEGMENT} [options]", stopAtNonOption: false)
		addSegmentCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			ro longOpt: 'readOnly', 'read only', args: 0, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_ADD_SEGMENT, addSegmentCLI)
		
		def datamigrateCLI = getDataMigrateCLI(VSP)
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
		
		
		Config cfg = endpointConfigPart(options, EndpointType.ALLEGROGRAPH)
		
		String profile = options.prof ? options.prof : VitalServiceFactory.DEFAULT_PROFILE		
		
		if(cmd == CMD_INIT) {
			
			ServiceConfigWrapper wrappedConfig = VitalServiceFactory.getProfileConfig(profile)
			
			VitalServiceAllegrographConfig ldConfig = (VitalServiceAllegrographConfig) wrappedConfig.serviceConfig
			
			String rootKey = options.rk ? options.rk : null
			
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

		VitalServiceAdminKey key = new VitalServiceAdminKey()
		key.generateURI((VitalApp) null)
		key.key = 'abcd-efgh-ijkl'
		VitalServiceAdmin serviceAdmin = VitalServiceFactory.openAdminService(key, profile)
		
		
		if(!(serviceAdmin instanceof VitalServiceAdminAllegrograph)) {
			System.err.println("Expected instanceof ${VitalServiceAdminAllegrograph.class.canonicalName}")
			System.exit(1)
		}
		
		
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
			
			VitalApp app = getApp(appID)
			
			if(app == null) return
		
			handleDataMigrate(VSP, options, serviceAdmin, app)
			
		} else {
			
			println "Unhandled command: ${cmd}"	
		
		}

		
		
//		LuceneServiceDiskImpl.initializeRoot(File)
	}
	
	static void usage() {
		
		println "usage: ${VSP} <command> [options] ..."
		
		println "usage: ${VSP} ${CMD_HELP} (prints usage)"
		
		for(Entry e : cmd2CLI.entrySet()) {
			e.value.usage()
		}

	}

}
