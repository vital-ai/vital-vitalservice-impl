package ai.vital.prime.command

import groovy.cli.picocli.CliBuilder

import java.util.Map;
import java.util.Map.Entry

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;

import ai.vital.vitalservice.EndpointType
import ai.vital.vitalservice.VitalService;
import ai.vital.prime.service.VitalServicePrime
import ai.vital.prime.service.admin.VitalServiceAdminPrime;
import ai.vital.prime.service.config.VitalServicePrimeConfig;
import ai.vital.prime.service.model.SegmentProvisioning;
import ai.vital.prime.service.model.SegmentProvisioningUtil;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.admin.VitalServiceAdmin
import ai.vital.vitalservice.command.AbstractCommand;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.factory.VitalServiceFactory.ServiceConfigWrapper
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.DomainModel
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;


import ai.vital.vitalsigns.command.patterns.JarFileInfo;
import ai.vital.vitalsigns.command.patterns.JsonSchemaFileInfo;
import ai.vital.vitalsigns.command.patterns.OwlFileInfo;
import ai.vital.vitalsigns.conf.VitalSignsConfig.DomainsSyncLocation;
import ai.vital.vitalsigns.conf.VitalSignsConfig.DomainsSyncMode;
import ai.vital.vitalsigns.conf.VitalSignsConfig.DomainsVersionConflict;
import ai.vital.vitalsigns.domains.DomainsSyncImplementation;

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

class VitalPrimeCommand extends AbstractCommand {

	static String CMD_HELP = 'help'
	
	static String CMD_INIT = 'init'
	
	
	static String CMD_LIST_SERVICE_ADMIN_KEYS = 'listserviceadminkeys'
	
	static String CMD_ADD_SERVICE_ADMIN_KEY = 'addserviceadminkey'
	 
	static String CMD_REMOVE_SERVICE_ADMIN_KEY = 'removeserviceadminkey'
	 
	static String CMD_LIST_SERVICE_KEYS = 'listservicekeys'
	
	static String CMD_ADD_SERVICE_KEY = 'addservicekey'
	
	static String CMD_REMOVE_SERVICE_KEY = 'removeservicekey' 
	
	
	static String CMD_LIST_APPS = 'listapps'
	
	static String CMD_REMOVE_APP = 'removeapp'
	
	static String CMD_ADD_APP = 'addapp'
	
	static String CMD_LIST_SEGMENTS = 'listsegments'
	
	static String CMD_ADD_SEGMENT = 'addsegment'
	
	static String CMD_REMOVE_SEGMENT = 'removesegment'
	
	static String CMD_REBUILDINDEXES = 'rebuildindexes'
	
	static String CMD_REINDEX_SEGMENT = 'reindexsegment'
	
	static String CMD_VERIFYINDEXES = 'verifyindexes'
	
	static String CMD_STATUS = 'status'
	
	static String CMD_SHUTDOWN = 'shutdown'
	
	
	static String CMD_GET = 'get'
	
	static String CMD_UPDATE = 'update'
	
	static String CMD_INSERT = 'insert'
	
	static String CMD_DELETE = 'delete'
	
	
	static String CMD_LIST_MODELS = 'listmodels'
	
	static String CMD_DEPLOY = 'deploy'
	
	static String CMD_UNDEPLOY = 'undeploy'
	
	static String CMD_LOAD = 'load'
	
	static String CMD_UNLOAD = 'unload'
	
	static String CMD_SYNCMODELS = 'syncmodels'
	
	
	static String VP = 'vitalprime'
	
	
	public final static String DOMAIN_MANAGER_DATASCRIPT = "commons/scripts/DomainsManagerScript";
	
	
	static Map cmd2CLI = new LinkedHashMap()
	
	static VitalServiceAdmin serviceAdmin
	
	static Set<String> serviceLevelCommands = new HashSet<String>([
		CMD_LIST_SEGMENTS,
		CMD_STATUS,
		CMD_GET,
		CMD_UPDATE,
		CMD_INSERT,
		CMD_DELETE
	])
	
	static Set<String> serviceKeyManagementCommands = new HashSet<String>([
		CMD_ADD_SERVICE_KEY,
		CMD_REMOVE_SERVICE_KEY,
		CMD_LIST_SERVICE_KEYS
	])
	
	static {
		
		def initCLI = new CliBuilder(usage: "${VP} ${CMD_INIT} [options]", stopAtNonOption: false)
		initCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			rk longOpt: 'root-key', 'optional root key, xxxx-xxxx-xxxx format', args: 1, required: false
			ak longOpt: 'admin-key', 'optional admin key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_INIT, initCLI)
		
		
		def listServiceAdminKeysCLI = new CliBuilder(usage: "${VP} ${CMD_LIST_SERVICE_ADMIN_KEYS} [options]", stopAtNonOption: false)
		listServiceAdminKeysCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			rk longOpt: 'root-key', 'root key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_SERVICE_ADMIN_KEYS, listServiceAdminKeysCLI)
		
		def addServiceAdminKeyCLI = new CliBuilder(usage: "${VP} ${CMD_ADD_SERVICE_ADMIN_KEY} [options]", stopAtNonOption: false)
		addServiceAdminKeyCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			rk longOpt: 'root-key', 'root key, xxxx-xxxx-xxxx format', args: 1, required: true
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format, random if not set', args: 1, required: false
		}
		cmd2CLI.put(CMD_ADD_SERVICE_ADMIN_KEY, addServiceAdminKeyCLI)
		
		def removeServiceAdminKeyCLI = new CliBuilder(usage: "${VP} ${CMD_REMOVE_SERVICE_ADMIN_KEY} [options]", stopAtNonOption: false)
		removeServiceAdminKeyCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			rk longOpt: 'root-key', 'root key, xxxx-xxxx-xxxx format', args: 1, required: true
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_REMOVE_SERVICE_ADMIN_KEY, removeServiceAdminKeyCLI)
		
		def listServiceKeysCLI = new CliBuilder(usage: "${VP} ${CMD_LIST_SERVICE_KEYS} [options]", stopAtNonOption: false)
		listServiceKeysCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
			a longOpt: 'app', 'app ID', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_SERVICE_KEYS, listServiceKeysCLI)
		
		def addServiceKeyCLI = new CliBuilder(usage: "${VP} ${CMD_ADD_SERVICE_KEY} [options]", stopAtNonOption: false)
		addServiceKeyCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
			a longOpt: 'app', 'app ID', args: 1, required: true
			sk longOpt: 'service-key', 'service key, xxxx-xxxx-xxxx format, random if not set', args: 1, required: false
		}
		cmd2CLI.put(CMD_ADD_SERVICE_KEY, addServiceKeyCLI)
		
		def removeServiceKeyCLI = new CliBuilder(usage: "${VP} ${CMD_REMOVE_SERVICE_KEY} [options]", stopAtNonOption: false)
		removeServiceKeyCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
			a longOpt: 'app', 'app ID', args: 1, required: true
			sk longOpt: 'service-key', 'service key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_REMOVE_SERVICE_KEY, removeServiceKeyCLI)
		
		
		def listAppsCLI = new CliBuilder(usage: "${VP} ${CMD_LIST_APPS} [options]", stopAtNonOption: false)
		listAppsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_APPS, listAppsCLI)

		def addAppCLI = new CliBuilder(usage: "${VP} ${CMD_ADD_APP} [options]", stopAtNonOption: false)
		addAppCLI.with {
			a longOpt: 'app', 'app ID', args: 1, required: true
			n longOpt: 'name', 'app name', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_ADD_APP, addAppCLI)
		
		def removeAppCLI = new CliBuilder(usage: "${VP} ${CMD_REMOVE_APP} [options]", stopAtNonOption: false)
		removeAppCLI.with {
			a longOpt: 'app', 'app ID', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_REMOVE_APP, removeAppCLI)

		
		def listSegmentsCLI = new CliBuilder(usage: "${VP} ${CMD_LIST_SEGMENTS} [options]", stopAtNonOption: false)
		listSegmentsCLI.with {
			a longOpt: 'appID', 'app ID, required with admin-key', args:1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, mutually exclusive with service-key, xxxx-xxxx-xxxx format', args: 1, required: false
			sk longOpt: 'service-key', 'service key, mutually exclusive with admin-key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_SEGMENTS, listSegmentsCLI)
		
		def removeSegmentCLI = new CliBuilder(usage: "${VP} ${CMD_REMOVE_SEGMENT} [options]", stopAtNonOption: false)
		removeSegmentCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			d longOpt: 'deleteData', 'delete data', args: 0, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_REMOVE_SEGMENT, removeSegmentCLI)
		
		def addSegmentCLI = new CliBuilder(usage: "${VP} ${CMD_ADD_SEGMENT} [options]", stopAtNonOption: false)
		addSegmentCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			ro longOpt: 'readOnly', 'read only', args: 0, required: false
			t longOpt: 'type', 'optional inner segment (endpoint) type, required if prime hosts more than 1 endpoint', args: 1, required: false 
			p longOpt: 'provisioningFile', "optional  provisioning config file - used when vitalprime hosts ${EndpointType.DYNAMODB.name} or ${EndpointType.INDEXDB.name} with DynamoDB backend", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_ADD_SEGMENT, addSegmentCLI)
		
		
		def reindexSegmentCLI = new CliBuilder(usage: "${VP} ${CMD_REINDEX_SEGMENT} [options]", stopAtNonOption: false)
		reindexSegmentCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_REINDEX_SEGMENT, reindexSegmentCLI)
		
		def verifyIndexesCLI = new CliBuilder(usage: "${VP} ${CMD_VERIFYINDEXES} (no options)", stopAtNonOption: false)
		verifyIndexesCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_VERIFYINDEXES, verifyIndexesCLI)
		
		def rebuildIndexesCLI = new CliBuilder(usage: "${VP} ${CMD_REBUILDINDEXES} (no options)", stopAtNonOption: false)
		rebuildIndexesCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_REBUILDINDEXES, rebuildIndexesCLI) 

		def statusCLI = new CliBuilder(usage: "${VP} ${CMD_STATUS} (no options)", stopAtNonOption: false)
		statusCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, mutually exclusive with service-key, xxxx-xxxx-xxxx format', args: 1, required: false
			sk longOpt: 'service-key', 'service key, mutually exclusive with admin-key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_STATUS, statusCLI)
		
		def shutdownCLI = new CliBuilder(usage: "${VP} ${CMD_SHUTDOWN} (no options)", stopAtNonOption: false)
		shutdownCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_SHUTDOWN, shutdownCLI)		
		
		def getCLI = new CliBuilder(usage: "${VP} ${CMD_GET} [options]", stopAtNonOption: false)
		getCLI.with {
			a longOpt: 'appID', 'app ID, required with admin-key', args:1, required: false
			u longOpt: 'uri', 'graph object URI', args: 1, required: true
			o longOpt: 'output', 'optional output block file .vital[.gz], by default prints to console', args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, mutually exclusive with service-key, xxxx-xxxx-xxxx format', args: 1, required: false
			sk longOpt: 'service-key', 'service key, mutually exclusive with admin-key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_GET, getCLI)
		
		def updateCLI = new CliBuilder(usage: "${VP} ${CMD_UPDATE} [options]", stopAtNonOption: false)
		updateCLI.with {
			a longOpt: 'appID', 'app ID, required with admin-key', args:1, required: false
			s longOpt: 'segment', 'segment ID', args: 1, required: true
			i longOpt: 'input', 'input block file with single block and single graph object .vital[.gz]', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, mutually exclusive with service-key, xxxx-xxxx-xxxx format', args: 1, required: false
			sk longOpt: 'service-key', 'service key, mutually exclusive with admin-key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_UPDATE, updateCLI)
		
		def insertCLI = new CliBuilder(usage: "${VP} ${CMD_INSERT} [options]", stopAtNonOption: false)
		insertCLI.with {
			a longOpt: 'appID', 'app ID, required with admin-key', args:1, required: false
			s longOpt: 'segment', 'segment ID', args: 1, required: true
			i longOpt: 'input', 'input block file with single block and single graph object .vital[.gz]', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, mutually exclusive with service-key, xxxx-xxxx-xxxx format', args: 1, required: false
			sk longOpt: 'service-key', 'service key, mutually exclusive with admin-key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_INSERT, insertCLI)
		
		def deleteCLI = new CliBuilder(usage: "${VP} ${CMD_DELETE} [options]", stopAtNonOption: false)
		deleteCLI.with {
			a longOpt: 'appID', 'app ID, required with admin-key', args:1, required: false
			u longOpt: 'uri', 'graph object URI', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, mutually exclusive with service-key, xxxx-xxxx-xxxx format', args: 1, required: false
			sk longOpt: 'service-key', 'service key, mutually exclusive with admin-key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_DELETE, deleteCLI) 
		
		
		
		
		def listModelsCLI = new CliBuilder(usage: "${VP} ${CMD_LIST_MODELS} [options]", stopAtNonOption: false)
		listModelsCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_MODELS, listModelsCLI)
		
		
		def deployCLI = new CliBuilder(usage: "${VP} ${CMD_DEPLOY} [options]", stopAtNonOption: false)
		deployCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			art longOpt: 'artifact', 'artifact file name (jar/owl/js) or file path in $VITAL_HOME/domain-groovy-jar/, $VITAL_HOME/domain-json-schema/ or $VITAL_HOME/domain-ontology/', args: 1, required: true
			sa longOpt: 'singleartifact', 'deploy this single artifact only'
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_DEPLOY, deployCLI)
		
		def undeployCLI = new CliBuilder(usage: "${VP} ${CMD_UNDEPLOY} [options]", stopAtNonOption: false)
		undeployCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			art longOpt: 'artifact', 'artifact file name (jar/owl/js) or file path in $VITAL_HOME/domain-groovy-jar/, $VITAL_HOME/domain-json-schema/ or $VITAL_HOME/domain-ontology/', args: 1, required: true
			sa longOpt: 'singleartifact', 'undeploy this single artifact only'
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_UNDEPLOY, undeployCLI)
		
		def loadCLI = new CliBuilder(usage: "${VP} ${CMD_LOAD} [options]", stopAtNonOption: false)
		loadCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			j longOpt: 'jar', 'remote jar name', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_LOAD, loadCLI)
		
		def unloadCLI = new CliBuilder(usage: "${VP} ${CMD_UNLOAD} [options]", stopAtNonOption: false)
		unloadCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			j longOpt: 'jar', 'remote jar name', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_UNLOAD, unloadCLI)
		
		def syncCLI = new CliBuilder(usage: "${VP} ${CMD_SYNCMODELS} [options]", stopAtNonOption: false)
		syncCLI.with {
			a longOpt: 'appID', 'app ID', args:1, required: true
			d longOpt: 'direction', "direction, one of, ${DomainsSyncMode.pull} (default), ${DomainsSyncMode.push}, ${DomainsSyncMode.both}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
		}
		cmd2CLI.put(CMD_SYNCMODELS, syncCLI)
		
		
		def datamigrateCLI = getDataMigrateCLI(VP)
		cmd2CLI.put(CMD_DATA_MIGRATE, datamigrateCLI)
		
		ExternalServiceCommandsImplementation.initCommands(cmd2CLI)
		
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
		
		Config cfg = endpointConfigPart(options, EndpointType.VITALPRIME)
				
		String profile = options.prof ? options.prof : VitalServiceFactory.DEFAULT_PROFILE
		
		println "command: ${cmd}"		
		
		if(cmd == CMD_INIT) {
			
			
			ServiceConfigWrapper wrappedConfig = VitalServiceFactory.getProfileConfig(profile)
			
			VitalServicePrimeConfig ldConfig = (VitalServicePrimeConfig) wrappedConfig.serviceConfig
			
			String rootKey = options.rk ? options.rk : null
			
			String adminKey = options.ak ? options.ak : null
			
			println "initializing..."

			if( VitalServiceFactory.isInitialized(ldConfig).status == VitalStatus.Status.ok ) {
				error "Service is already initialized, exiting"
				return
			}
			
			
			VitalServiceRootKey rk = null;
			if(rootKey) {
				println "Initlializing with root key: ${rootKey}"
				rk = (VitalServiceRootKey) new VitalServiceRootKey().generateURI((VitalApp)null)
				rk.key = rootKey
			} else {
				println "Initializing with random key"
			}
			
			
			VitalServiceAdminKey adminKeyObj = new VitalServiceAdminKey()
			adminKeyObj = (VitalServiceAdminKey) adminKeyObj.generateURI((VitalApp)null)
			if(adminKey) {
				println "Initializing with admin key: ${adminKey}"
			} else {
				adminKey = RandomStringUtils.randomAlphabetic(4) + '-' + RandomStringUtils.randomAlphabetic(4) + '-' + RandomStringUtils.randomAlphabetic(4)
				println "Initializing with random admin key: ${adminKey}"
			}
			
			adminKeyObj.key = adminKey
			
			rk = VitalServiceFactory.initService(ldConfig, rk, adminKeyObj)
			
			if(!rootKey) {
				println "Generated root key: ${rk.key}"
			}			
			
			println "DONE"

			return
			
		} else if(cmd == CMD_ADD_SERVICE_ADMIN_KEY) {
		
			VitalServiceRootKey rootKey = new VitalServiceRootKey().generateURI((VitalApp)null)	
			rootKey.key = (String) options.rk
			
			String adminKeyString = options.ak ? options.ak : null
			
			VitalServiceAdminKey adminKey = new VitalServiceAdminKey().generateURI((VitalApp)null)
			if(adminKeyString != null) {
				println "Adding provided admin key: ${adminKeyString}"
			} else {
				adminKeyString = RandomStringUtils.randomAlphabetic(4) + '-' + RandomStringUtils.randomAlphabetic(4) + '-' + RandomStringUtils.randomAlphabetic(4)
				println "Adding random admin key: ${adminKeyString}"
			}
			adminKey.key = adminKeyString
			
			try {
				VitalStatus status = VitalServiceFactory.addVitalServiceAdminKey(VitalServiceFactory.getProfileConfig(profile).serviceConfig, rootKey, adminKey)
				println "Status: ${status}"
			} catch(VitalServiceException e) {
				error e.localizedMessage
			}
			
			
			return
			
		} else if(cmd == CMD_LIST_SERVICE_ADMIN_KEYS) {
		
			VitalServiceRootKey rootKey = new VitalServiceRootKey().generateURI((VitalApp)null)
			rootKey.key = (String) options.rk

			try {
				
				List<VitalServiceAdminKey> keys = VitalServiceFactory.listVitalServiceAdminKeys(VitalServiceFactory.getProfileConfig(profile).serviceConfig, rootKey)
						
				println "Total keys: ${keys.size()}"
				int c = 0
				for(VitalServiceAdminKey key : keys) {
					c++
					println "${c}.   ${key.key}"
				}
				
			} catch(Exception e) {
				error e.localizedMessage
			}
			
			return
			
					
		} else if(cmd == CMD_REMOVE_SERVICE_ADMIN_KEY) {
		
			VitalServiceRootKey rootKey = new VitalServiceRootKey().generateURI((VitalApp)null)
			rootKey.key = (String) options.rk
			
			VitalServiceAdminKey adminKey = new VitalServiceAdminKey().generateURI((VitalApp)null)
			adminKey.key = (String) options.ak
			
			println "Removing service admin key: ${adminKey.key}"

			try {
				VitalStatus status = VitalServiceFactory.removeVitalServiceAdminKey(VitalServiceFactory.getProfileConfig(profile).serviceConfig, rootKey, adminKey)
				println "Status: ${status}"
			} catch(VitalServiceException e) {
				error e.localizedMessage
			}
			
			
			return
			
		}

		
		VitalService service = null
		VitalServiceAdmin serviceAdmin = null
		
		String ak = options.ak ? options.ak : null
		
		String _sk = null
		
		if(serviceLevelCommands.contains(cmd)) {
		
			_sk = options.sk ? options.sk : null

			if(!_sk && !ak) {
				error("--service-key or --admin-key required")
				return
			}
			
			if(ak && cmd != CMD_STATUS && !options.a) {
				error("--appID is required with --admin-key")
				return
			}
				
		} else {
		
			if(!ak) {
				error("--admin-key required")
				return
			}
		
		}
		
		if(_sk && ak) {
			error("--service-key and --admin-key are mutually exclusive")
			return
		}
		
		if(!_sk && !ak) {
			error("--service-key or --admin-key required")
			return
		}
		
		if(ak) {
			
			VitalServiceAdminKey key = new VitalServiceAdminKey()
			key.generateURI((VitalApp)null)
			key.key = ak 
			serviceAdmin = VitalServiceFactory.openAdminService(key, profile)
					
			if(!(serviceAdmin instanceof VitalServiceAdminPrime)) {
				error "Expected instanceof ${VitalServiceAdminPrime.class.canonicalName}"
				return
			}
			
		} else {
		
			if(options.a && !serviceKeyManagementCommands) {
				error "--appID may only be used with --admin-key"
				return
			}
		
			VitalServiceKey key = new VitalServiceKey()
			key.generateURI((VitalApp) null)
			key.key = _sk
		
			service = VitalServiceFactory.openService(key, profile)

			if(!(service instanceof VitalServicePrime)) {
				error "Expected instanceof ${VitalServicePrime.class.canonicalName}"
				return
			}
					
		}
		
		
		VitalServiceAdminPrime primeServiceAdmin = (VitalServiceAdminPrime) serviceAdmin
		VitalServicePrime primeService = (VitalServicePrime) service
		
		if(cmd == CMD_ADD_SERVICE_KEY) {
			
			String appID = options.a

			println "AppID: ${appID}"
			VitalApp app = getApp(serviceAdmin, appID)
			if(app == null) return
			
			String sk = options.sk ? options.sk : null
			
			if(sk != null) {
				println "Addinh new provided service key: ${sk}"
			} else {
				sk = RandomStringUtils.randomAlphabetic(4) + '-' + RandomStringUtils.randomAlphabetic(4) + '-' + RandomStringUtils.randomAlphabetic(4)
				println "Adding random service key: ${sk}"
			}
			
			VitalServiceKey serviceKey = new VitalServiceKey().generateURI((VitalApp)null)
			serviceKey.key = sk

			try {			
				VitalStatus status = serviceAdmin.addVitalServiceKey(app, serviceKey)
				println "Status: ${status}"
			} catch(VitalServiceException e) {
				error e.localizedMessage
			}
			
		} else if(cmd == CMD_LIST_SERVICE_KEYS) {
		
			String appID = options.a
			println "AppID: ${appID}"

			VitalApp app = getApp(serviceAdmin, appID)
			if(app == null) return
					
			try {
				
				List<VitalServiceKey> keys = serviceAdmin.listVitalServiceKeys(app)
	
				println "Total service keys: ${keys.size()}"
				int c = 0
				for(VitalServiceKey sk : keys) {
					c++
					println "${c}.   ${sk.key}"
				}
			} catch(VitalServiceException e) {
				error e.localizedMessage
			}
			
		} else if(cmd == CMD_REMOVE_SERVICE_KEY) {
		
		
			String appID = options.a
			println "AppID: ${appID}"
			
			VitalApp app = getApp(serviceAdmin, appID)
			if(app == null) return
			
			println "Removing service key: ${options.sk}"
			
			VitalServiceKey serviceKey = new VitalServiceKey().generateURI((VitalApp)null)
			serviceKey.key = (String) options.sk

			try {			
				VitalStatus status = serviceAdmin.removeVitalServiceKey(app, serviceKey)
				println "Status: ${status}"
			} catch(VitalServiceException e) {
				error e.localizedMessage
			}
			
		} else if(cmd == CMD_LIST_APPS) {
		
			handleListApps(options, serviceAdmin)
			
		} else if(cmd == CMD_ADD_APP) {
		
			handleAddApp(options, serviceAdmin)
			
		} else if(cmd == CMD_REMOVE_APP) {

			handleRemoveApp(options, serviceAdmin)
					
		} else if(cmd == CMD_LIST_SEGMENTS) {

			if(serviceAdmin != null) {
				
				handleListSegments(options, serviceAdmin)
				
			} else {
			
				handleListSegmentsService(service)
			
			}
		
					
		} else if(cmd == CMD_ADD_SEGMENT) {

			handleAddSegment(options, serviceAdmin)
		
			/*
			String segmentID = options.s
			String appID = options.a	
			Boolean readOnly = options.ro
			
			println "SegmentID: ${segmentID}"
			println "AppID: ${appID}"
			println "ReadOnly: ${readOnly}"
			
			
			
			
			App app = getApp(appID)
			
			if(app == null) return
			
			PrimeVitalSegment s = new PrimeVitalSegment()
			s.ID = options.s
			s.readOnly = readOnly;
			s.appID = options.a	
			
			
			String provisioningFile = options.p ? options.p : null
			
			File f = null
			
			if( provisioningFile ) {
				
				f = new File( provisioningFile )
				
				println "Reading provisioning config from custom file: ${f.absolutePath} ..."
			
				
			} else {
			
				f = new File( System.getenv('VITAL_HOME'), 'vital-config/vital-dynamodb/default-provisioning.conf' )

				if(f.exists()) {
					println "No provisioning file - using default settings from \$VITAL_HOME/vital-config/vital-dynamodb/default-provisioning.conf ... - ${f.absolutePath}"
				} else {
					println "WARNING default provisioning profile used, all throughput values set to 1, suitable only for local development dynamodb"
				}
								
				
			}

			SegmentProvisioning provisioning = null
			
			if(!f.isFile()) {
				
//				throw new RuntimeException("Provisioning config file not found: ${f.absolutePath}")
				provisioning = new SegmentProvisioning()
				
			} else {
			
				String sp = FileUtils.readFileToString(f, 'UTF-8')
						
				provisioning = SegmentProvisioningUtil.provisioningConfigFromString(sp)
				
			}
			
			
			s.provisioning = provisioning
			
			try {
				
				serviceAdmin.addSegment(app, s, false)
				
				println "Segment added"
				
			} catch(Exception e) {
				error(e.localizedMessage)
			}
			*/
			
			
		} else if(cmd == CMD_REMOVE_SEGMENT) {
		
			handleRemoveSegment(options, serviceAdmin)
			
		} else if(cmd == CMD_REBUILDINDEXES) {
		
			println "Rebuilding indexes..."
			//temporarily allow these calls
			try {
			
				ResultList rs = primeServiceAdmin.callFunction(null, 'commons/admin/REBUILDINDEXES', [:]);
					
				println "Status: ${rs?.status}"
			} catch(Exception e) {
				error(e.localizedMessage)
			}
			
			println "Indexes rebuilt"
			
//			primeServiceAdmin.
			
		} else if(cmd == CMD_REINDEX_SEGMENT) {
		
			String segmentID = options.s
			String appID = options.a
			
			VitalApp app = getApp(serviceAdmin, appID)
			
			VitalSegment segment = serviceAdmin.getSegment(app, segmentID)
			
			if(segment == null) {
				error "Segment not found, appID: ${appID} segmentID: ${segmentID}"
				return
			}
			
			println "Reindexing segment ${segmentID}, app ${appID} ..."
			
			try {
				
				ResultList rs = primeServiceAdmin.callFunction(null, 'commons/admin/REINDEXSEGMENT', [app: app, segment: segment]);
						
				println "Status: ${rs?.status}"
				
			} catch(Exception e) {
				error(e.localizedMessage)
			}
			
			println "Reindexing complete"
			
		} else if(cmd == CMD_VERIFYINDEXES) {
		
			println "Verifying indexes..."
			//temporarily allow these calls
			try {
				
				ResultList rs = primeServiceAdmin.callFunction(null, 'commons/admin/VERIFYINDEXES', [:]);
				
				println "Status: ${rs?.status}"
				
			} catch(Exception e) {
				error(e.localizedMessage)
			}
		
			
		} else if(cmd == CMD_STATUS) {
		
			println "Pinging vitalprime..."
			
			VitalStatus status = null
			
			if(primeServiceAdmin != null) {
				
				status = primeServiceAdmin.ping();
				
			} else {
			
				status = primeService.ping()
			
			}
		
			println "Status ${status}"
			
		} else if(cmd == CMD_SHUTDOWN) {
		
			println "shutting down vital prime..."
		
			VitalStatus status = primeServiceAdmin.shutdown();
			
			println "status: ${status}"
			
			
		} else if(cmd == CMD_GET) {
		
			VitalApp app = null
			
			if(serviceAdmin != null) {
				
				app = getApp(serviceAdmin, options.a)
						
				if(app == null) return
				
			} 
		
			File outputFile = null
			
			if(options.o) {
				String o = options.o
				if(o.endsWith('.vital') || o.endsWith('.vital.gz')) {
					outputFile = new File(o)	
				}  else {
					error("Output file must end with .vital[.gz]")
				}
				
				
			}
			
			URIProperty uri = URIProperty.withString(options.u)
			
			GraphObject go = null
					
			if(primeServiceAdmin != null) {
				
				go = primeServiceAdmin.get(app, GraphContext.ServiceWide, uri).first();
				
			} else {
			
				go = primeService.get(GraphContext.ServiceWide).first()
			
			}
			
		
			if(go == null) error("Graph object not found: ${uri.URI}")
			
			BlockCompactStringSerializer serializer = null;
			StringWriter sw = null
			if(outputFile != null) {
				serializer = new BlockCompactStringSerializer(outputFile)
			} else {
				sw = new StringWriter()
				serializer = new BlockCompactStringSerializer(sw)
			}
			
			serializer.startBlock()
			serializer.writeGraphObject(go)
			serializer.endBlock()
			
			serializer.close()
			
			if(sw != null) {
				println sw.toString()
			} else {
				println "Graph object persisted in file: ${outputFile.absolutePath}"
			}
			
			
		} else if(cmd == CMD_UPDATE) {
		
			VitalApp app = null
			
			if(serviceAdmin != null ) {
				
				app = getApp(serviceAdmin, options.a)
				
				if(app == null) return
				
			}
		
		
			String segment = options.s
			
			String inputF = options.i	
			
			GraphObject go = readGraphObject(inputF)
			
			println "Updating object with URI: ${go.URI}"
			
			GraphObject go2 = null
			
			if(primeServiceAdmin != null) {
				
				go2 = primeServiceAdmin.get(app, GraphContext.ServiceWide, URIProperty.withString(go.URI)).first();
				
			} else {
			
				go2 = primeService.get(GraphContext.ServiceWide, URIProperty.withString(go.URI)).first();
			
			}
		
			if(go2 == null) error("Graph object not found: ${go.URI}")
			
			try {

				ResultList rl = null
								
				if(primeServiceAdmin != null) {
					
					rl = primeServiceAdmin.save(app, VitalSegment.withId(segment), go, false)
					
				} else {
				
					rl = primeService.save(VitalSegment.withId(segment), go, false)
				
				}
				
				println "Graph object update status: ${rl.status}"
				
			} catch(Exception e) {
				error(e.localizedMessage)
			}
		
		} else if(cmd == CMD_INSERT) {
		
			VitalApp app = null
			
			if(serviceAdmin != null) {
				
				app = getApp(serviceAdmin, options.a)
				
				if(app == null) return
				
			}
			
			String segment = options.s
			
			String inputF = options.i
		
			GraphObject go = readGraphObject(inputF)
			
			println "Inserting object with URI: ${go.URI}"
			
			GraphObject go2 = null
			
			if(primeServiceAdmin != null) {
				
				go2 = primeServiceAdmin.get(app, GraphContext.ServiceWide, URIProperty.withString(go.URI)).first();
				
			} else {
			
				go2 = primeService.get(GraphContext.ServiceWide, URIProperty.withString(go.URI)).first();
			
			}
			
			if(go2 != null) error("Graph object with URI: ${go.URI} already exists")
			
			try {
				
				ResultList rl = null
				
				
				if(primeServiceAdmin != null) {
				
					rl = primeServiceAdmin.insert(app, VitalSegment.withId(segment), go)
						
				} else {
				
					rl = primeService.insert(VitalSegment.withId(segment), go)
				
				}
				println "Graph object insert status: ${rl.status}"
				
				GraphObject go3 = rl.first()
				
				if(go3 == null) throw new Exception("Object not inserted!")
				
			} catch(Exception e) {
				error(e.localizedMessage)
			}
			
		} else if(cmd == CMD_DELETE) {
		
			VitalApp app =  null
		
			if(serviceAdmin != null) {
				
				app = getApp(serviceAdmin, options.a)
						
				if(app == null) return
				
			}	
			
			URIProperty uri = URIProperty.withString(options.u)

			GraphObject go = null
			
			if(primeServiceAdmin != null) {
				
				go = primeServiceAdmin.get(app, GraphContext.ServiceWide, uri).first();
				
			} else {
			
				go = primeService.get(GraphContext.ServiceWide, uri).first()
			
			}		
		
			if(go == null) error("Graph object not found: ${uri.URI}")
			
			
			try {
				
				VitalStatus status = null
				
				if(primeServiceAdmin != null) {
					status = primeServiceAdmin.delete(app, uri)
				} else {
					status = primeService.delete(uri)
				}
				println "Status: ${status}"
			} catch(Exception e) {
				error(e.localizedMessage)
			}
		
		} else if(cmd == CMD_LIST_MODELS ) {
		
			VitalApp app = getApp(serviceAdmin, options.a)
			
			if(app == null) return
			
			handleListModels(primeServiceAdmin, app)
			
			
		} else if(cmd == CMD_DEPLOY) {
		
			VitalApp app = getApp(serviceAdmin, options.a)
			
			if(app == null) return
			
			String artifactName = options.art
			
			boolean singleArtifact = options.sa ? true : false
			
			handleDeploy(primeServiceAdmin, app, artifactName, singleArtifact)
			
			
		} else if(cmd == CMD_UNDEPLOY) {
		
			VitalApp app = getApp(serviceAdmin, options.a)
			
			if(app == null) return
			
			String artifactName = options.art
			
			boolean singleArtifact = options.sa ? true : false
			
			handleUndeploy(primeServiceAdmin, app, artifactName, singleArtifact)
			
			
		} else if(cmd == CMD_LOAD) {
		
			VitalApp app = getApp(serviceAdmin, options.a)
			
			if(app == null) return
		
			String jarName = options.j
			
			handleLoad(primeServiceAdmin, app, jarName)
			
		} else if(cmd == CMD_UNLOAD) {
		
			VitalApp app = getApp(serviceAdmin, options.a)
			
			if(app == null) return
		
			String jarName = options.j
			
			handleUnload(primeServiceAdmin, app, jarName)
			
		} else if(cmd == CMD_SYNCMODELS) {
		
			VitalApp app = getApp(serviceAdmin, options.a)
			
			if(app == null) return
		
			DomainsSyncMode sm = DomainsSyncMode.pull
			
			String direction = options.d ? options.d : null
			if(direction != null) {
				println "direction unput : ${direction}"
				sm = DomainsSyncMode.valueOf(direction)
			}
			println "direction: ${sm}"
			
			handleSync(primeServiceAdmin, app, sm)
			
			
		} else if(cmd == CMD_DATA_MIGRATE) {
		
			String appID = options.a
			
			VitalApp app = getApp(serviceAdmin, appID)
			
			if(app == null) return
		
			handleDataMigrate(VP, options, serviceAdmin, app)
			
		} else if(ExternalServiceCommandsImplementation.handleCommand(options, cmd, serviceAdmin)) {
			
		} else {
			
			println "Unhandled command: ${cmd}"
			
		
		}

		
		
//		LuceneServiceDiskImpl.initializeRoot(File)
	}
	
	
	static GraphObject readGraphObject(String inputF) {
		
		if(! ( inputF.endsWith('.vital')  || inputF.endsWith('.vital.gz') ) ) {
			error("input file name must end with .vital.gz : " + inputF)
		}
		
		BlockIterator iterator = null;
		
		GraphObject go = null
		
		try {
			
			for( iterator = BlockCompactStringSerializer.getBlocksIterator(new File(inputF)); iterator.hasNext(); ) {
				
				if(go != null) error("More than 1 graph object found in file: ${inputF}")
				
				VitalBlock block = iterator.next();
				
				go = block.mainObject
				
				for(GraphObject g : block.dependentObjects) {
					
					if(go != null) error("More than 1 graph object found in file: ${inputF}")
					
				}
				
			}
			
		} finally {
			try { iterator.close() } catch(Exception e){}
		}
		
		if(go == null) error("No graph objects found in block file: ${}")
		
		return go
		
	}
	
	static void usage() {
		
		println "usage: ${VP} <command> [options] ..."
		
		println "usage: ${VP} ${CMD_HELP} (prints usage)"
		
		for(Entry e : cmd2CLI.entrySet()) {
			e.value.usage()
		}

	}

	/*
	public static String printPrimeSegment(VitalSegment segment, String indent) {
		String res = indent +  "${segment.class.simpleName} ${segment.ID}   ReadOnly? ${segment.readOnly} endpointType: ${segment.endpointType}"
		if(segment.provisioning != null) {
			
			SegmentProvisioning spc = segment.provisioning
			
			String x = "\n${indent}Provisioning:"
			
			boolean atLeast1Indexed = false
		
			for(String s : ['Node', 'Edge', 'HyperNode', 'HyperEdge']) {
				boolean stored  = spc."${s}_stored"
				boolean indexed = spc."${s}_indexed"
				if(indexed) atLeast1Indexed = true
				String r = "\t${s} stored: ${stored}"
				if(stored) {
					r +=( " read:" + spc."${s}_read" + " write:" + spc."${s}_write" + " indexed: ${indexed} " )
				}
	//			if( x.length() > 0 ) {
					x+= "\n";
	//			}
				x = x + indent + r
			}
			
			if(atLeast1Indexed) {
				
				x += (" \n${indent}\tProperties read:" + spc.Properties_read + " write:" + spc.Properties_write +
					", string index read:" + spc.Properties_string_index_read + " write:" + spc.Properties_string_index_write +
					", numbers index read:" + spc.Properties_number_index_read + " write:" + spc.Properties_number_index_write
				)

				
			}
			
			res += x
			
		}
		
		return res
	}
	*/

	public static ResultList handleListModels(VitalServiceAdminPrime primeServiceAdmin, VitalApp app) {

		ResultList rl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: "listDomainModelsWithEdges"])
		
		if(rl.status.status != VitalStatus.Status.ok) error("Listing domains error: " + rl.status.message)
		
		VITAL_Container c = new VITAL_Container()
		
		for(GraphObject g : rl) {
			c.putGraphObject(g)
		}
		
		int i = 0
		//should be returned sorted
		for(GraphObject g : rl) {
			
			if(!(g instanceof DomainModel)) continue
			 
			DomainModel model = (DomainModel) g;
			
			boolean loaded = model.active == null || model.active.booleanValue() == true;
			
			println "${++i}. ${model.name}  [${loaded ? 'LOADED' : ' NOT LOADED'}]"
			if(loaded) println "    URI: ${model.URI}"
			println "   jar name: ${model.name}"
			
			
			if(loaded) {
				
				for(DomainModel parent : model.getParentDomainModels(GraphContext.Container, c)) {
					
					println "\t parent domain: ${parent.URI} - ${parent.name}"
					
				}
				
				for(DomainModel child : model.getChildDomainModels(GraphContext.Container, c)) {
					
					println "\t child domain: ${child.URI} - ${child.name}"
					
				}
				
			}
			
			
		}

		return rl
				
	}
	
	public static void handleDeploy(VitalServiceAdminPrime primeServiceAdmin, VitalApp app, String artifactName, boolean singleArtifact) {
		
		File domainJarDir = new File( VitalSigns.get().getVitalHomePath(), "domain-groovy-jar" )
		File domainJsonSchemaDir = new File( VitalSigns.get().getVitalHomePath(), "domain-json-schema" )
		File domainOntologyDir = new File( VitalSigns.get().getVitalHomePath(), "domain-ontology" )
		
		
		println "Artifact Name / path: ${artifactName}"
		println "single artifactdependencies ? ${singleArtifact}"
		
		File jarFile = null
		
		File owlFile = null
		
		File jsonFile = null
		
		if(artifactName.contains("/") || artifactName.contains("\\") || new File(artifactName).exists()) {
			
			File artifactFile = new File(artifactName).getAbsoluteFile()
			
			if(!artifactFile.exists()) error("Artifact file not fount: ${artifactFile.absolutePath}")
			
			
			File parent = artifactFile.getParentFile().getAbsoluteFile()
			
			if(parent.equals(domainJarDir)) {
				
				JarFileInfo jfi = JarFileInfo.fromString(artifactFile.name)
				
				jarFile = artifactFile
				
				if(!singleArtifact) {
					
					jsonFile = new File(domainJsonSchemaDir, JsonSchemaFileInfo.fromJarInfo(jfi).toFileName())
					
					owlFile = new File(domainOntologyDir, OwlFileInfo.fromJarInfo(jfi).toFileName())
					
				}
				
				
			} else if(parent.equals(domainJsonSchemaDir)) {
			
				JsonSchemaFileInfo jsfi = JsonSchemaFileInfo.fromString(artifactFile.name)
				
				jsonFile = artifactFile
				
				if(!singleArtifact) {
					
					jarFile = new File(domainJarDir, JarFileInfo.fromJsonSchemaInfo(jsfi).toFileName())
					
					owlFile = new File(domainOntologyDir, OwlFileInfo.fromJsonSchemaInfo(jsfi).toFileName())
					
				}
				
			} else if(parent.equals(domainOntologyDir)) {
			
				OwlFileInfo ofi = OwlFileInfo.fromString(artifactFile.name)
				
				owlFile = artifactFile
				
				if(!singleArtifact) {
					
					jarFile = new File(domainJarDir, JarFileInfo.fromOwlInfo(ofi).toFileName())
					
					jsonFile = new File(domainJsonSchemaDir, JsonSchemaFileInfo.fromOwlInfo(ofi).toFileName())
					
				}
				
			} else {
			
				error("Invalid artifact file location: ${parent.absolutePath}")
				
			}
			
		} else {
		
//			artifactFile = new File(domainJarDir, jarName)
		
			JarFileInfo jfi = JarFileInfo.fromStringUnsafe(artifactName)
			JsonSchemaFileInfo jsfi = JsonSchemaFileInfo.fromStringUnsafe(artifactName)
			OwlFileInfo ofi = OwlFileInfo.fromStringUnsafe(artifactName)
			
			if(jfi != null) {
				
				jarFile = new File(domainJarDir, jfi.toFileName())
				
				if(!jarFile.exists()) error "Jar file not found: ${jarFile.absolutePath}"
				
				if(!singleArtifact) {
					
					jsonFile = new File(domainJsonSchemaDir, JsonSchemaFileInfo.fromJarInfo(jfi).toFileName())
					
					owlFile = new File(domainOntologyDir, OwlFileInfo.fromJarInfo(jfi).toFileName())
					
				}
				
			} else if(jsfi != null) {
			
				jsonFile = new File(domainJsonSchemaDir, jsfi.toFileName())
				
				if(!jsonFile.exists()) error "Json schema file not found: ${jsonFile.absolutePath}"
			
				if(!singleArtifact) {
					
					jarFile = new File(domainJarDir, JarFileInfo.fromJsonSchemaInfo(jsfi).toFileName())
					
					owlFile = new File(domainOntologyDir, OwlFileInfo.fromJsonSchemaInfo(jsfi).toFileName())
					
				}
			
			} else if(ofi != null) {
			
				owlFile = new File(domainOntologyDir, ofi.toFileName())
				
				if(!owlFile.exists()) error "OWL file not found: ${owlFile.absolutePath}"
				
				if(!singleArtifact) {
					
					jarFile = new File(domainJarDir, JarFileInfo.fromOwlInfo(ofi).toFileName())
					
					jsonFile = new File(domainJsonSchemaDir, JsonSchemaFileInfo.fromOwlInfo(ofi).toFileName())
				}
			
			} else {
			
				error("artifact name is invalid: ${artifactName}")
				
			}
		
		}
		
		
		if(jarFile != null) {

			if(jarFile.exists()) {
				
				println "Deploying jar - ${jarFile.absolutePath}"
				ResultList jrl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: 'saveDomainJar', jarName: jarFile.name, content: FileUtils.readFileToByteArray(jarFile)])
				if(jrl.status.status != VitalStatus.Status.ok) error("Save domain jar error: " + jrl.status.message)
				
			} else {
			
				println "Jar file does not exist: ${jarFile.absolutePath}"
			
			}
						
		}
		
		
		if(owlFile != null) {
			
			if(owlFile.exists()) {
				
				println "Deploying owl - ${owlFile.absolutePath}"
				ResultList orl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: 'saveDomainOntology', owlName: owlFile.name, content: FileUtils.readFileToByteArray(owlFile)])
				if(orl.status.status != VitalStatus.Status.ok) error("Save domain owl error: " + orl.status.message)
				
			} else {
			
				println "OWL file does not exist: ${owlFile.absolutePath}"
			
			}
		}
		
		if(jsonFile != null) {
			
			if(jsonFile.exists()) {
				
				println "Deploying json schema - ${jsonFile.absolutePath}"
				ResultList jsrl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: 'saveDomainJsonSchema', jsonSchemaName: jsonFile.name, content: FileUtils.readFileToByteArray(jsonFile)])
				if(jsrl.status.status != VitalStatus.Status.ok) error("Save domain json schema error: " + jsrl.status.message)
				
			} else {
			
				println "Json schema file does not exist: ${jsonFile.absolutePath}"
			
			}
			
		}
		
		println "Done"

		
	}
	
	public static void handleUndeploy(VitalServiceAdminPrime primeServiceAdmin, VitalApp app, String artifactName, boolean singleArtifact) {

		File domainJarDir = new File( VitalSigns.get().getVitalHomePath(), "domain-groovy-jar" )
		File domainJsonSchemaDir = new File( VitalSigns.get().getVitalHomePath(), "domain-json-schema" )
		File domainOntologyDir = new File( VitalSigns.get().getVitalHomePath(), "domain-ontology" )
		
		//don't validate it here
		File artifactFile = new File(artifactName)
		
		artifactName = artifactFile.name
		
		println "Artifact Name / path: ${artifactName}"
		println "single artifactdependencies ? ${singleArtifact}"
		
		File jarFile = null
		
		File owlFile = null
		
		File jsonFile = null
		
		
		// artifactFile = new File(domainJarDir, jarName)
		
		JarFileInfo jfi = JarFileInfo.fromStringUnsafe(artifactName)
		JsonSchemaFileInfo jsfi = JsonSchemaFileInfo.fromStringUnsafe(artifactName)
		OwlFileInfo ofi = OwlFileInfo.fromStringUnsafe(artifactName)
		
		if(jfi != null) {
			
			if(!singleArtifact) {
				
				jsfi = JsonSchemaFileInfo.fromJarInfo(jfi)
				
				ofi = OwlFileInfo.fromJarInfo(jfi)
				
			} 
			
		} else if(jsfi != null) {
		
		
			if(!singleArtifact) {
				
				jfi = JarFileInfo.fromJsonSchemaInfo(jsfi)
				
				ofi = OwlFileInfo.fromJsonSchemaInfo(jsfi)
				
			}
		
		} else if(ofi != null) {
		
			
			if(!singleArtifact) {
				
				jfi = JarFileInfo.fromOwlInfo(ofi)
				
				jsfi = JsonSchemaFileInfo.fromOwlInfo(ofi)
			}
		
		} else {
		
			error("artifact name is invalid: ${artifactName}")
			
		}
		
		
		if(jfi != null) {
			
			println "Undeploying jar - ${jfi.toFileName()}"
			
			ResultList jrl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: 'deleteDomainJar', jarName: jfi.toFileName()])
			
			if(jrl.status.status != VitalStatus.Status.ok) {
				println("Delete domain jar error: " + jrl.status.message)
			} else {
				println("Domain jar undeployed " + jfi.toFileName())
			}
			
		}
		
		if(jsfi != null) {
			
			println "Undeploying json schema- ${jsfi.toFileName()}"
			
			ResultList jsrl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: 'deleteDomainJsonSchema', jsonSchemaName: jsfi.toFileName()])
			if(jsrl.status.status != VitalStatus.Status.ok) {
				println("Delete domain json schema error: " + jsrl.status.message)
			} else {
				println("Domain json schema undeployed " + jsfi.toFileName())
			}
			
		}
		
		if(ofi != null) {
			
			ResultList orl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: 'deleteDomainOntology', owlName: ofi.toFileName()])
			if(orl.status.status != VitalStatus.Status.ok) {
				println("Delete domain owl error: " + orl.status.message)
			} else {
				println("Domain owl undeployed " + ofi.toFileName())
			}
			
		}
		
		println "Done"
				
	}
	
	public static void handleLoad(VitalServiceAdminPrime primeServiceAdmin, VitalApp app, String jarName) {
		
		jarName = new File(jarName).getName()
		
		println "JarName: ${jarName}"
		
		JarFileInfo jfi = JarFileInfo.fromString(jarName)
		
		println "Loding jar - ${jfi.toFileName()}"
		ResultList jrl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: 'loadDomainJar', jarName: jfi.toFileName()])
		if(jrl.status.status != VitalStatus.Status.ok) {
			error("Loading domain jar error: " + jrl.status.message)
		}

		println("Domain jar loaded " + jfi.toFileName())
	}

	public static void handleUnload(VitalServiceAdminPrime primeServiceAdmin, VitalApp app, String jarName) {
		
		jarName = new File(jarName).getName()
		
		println "JarName: ${jarName}"
		
		JarFileInfo jfi = JarFileInfo.fromString(jarName)
		
		println "Unloding jar - ${jfi.toFileName()}"
		ResultList jrl = primeServiceAdmin.callFunction(app, DOMAIN_MANAGER_DATASCRIPT, [action: 'unloadDomainJar', jarName: jfi.toFileName()])
		if(jrl.status.status != VitalStatus.Status.ok) {
			error("Unloading domain jar error: " + jrl.status.message)
		}

		println("Domain jar unloaded " + jfi.toFileName())

	}
	
	public static void handleSync(VitalServiceAdminPrime primeServiceAdmin, VitalApp app, DomainsSyncMode sm) {
		
		VitalSigns.get().setCurrentApp(app)
		
		DomainsSyncImplementation impl = new DomainsSyncImplementation()
		impl.setCurrentApp(app)
		impl.setService(null)
		impl.setServiceAdmin(primeServiceAdmin)
		
		impl.impl(sm, DomainsSyncLocation.domainsDirectory, DomainsVersionConflict.server)
		
		println("Done")

		
	}	
}
