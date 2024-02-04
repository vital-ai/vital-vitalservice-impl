package ai.vital.indexdb.command

import groovy.cli.picocli.CliBuilder

import java.util.Map;
import java.util.Map.Entry

import org.apache.commons.io.FileUtils;

import ai.vital.allegrograph.service.VitalServiceAllegrographConfigCreator
import ai.vital.allegrograph.service.config.VitalServiceAllegrographConfig
import ai.vital.vitalservice.EndpointType
import ai.vital.indexeddb.service.admin.VitalServiceAdminIndexedDB;
import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig;
import ai.vital.lucene.model.LuceneSegmentType;
import ai.vital.sql.VitalSqlImplementation
import ai.vital.sql.command.VitalSqlCommand;
import ai.vital.sql.connector.VitalSqlDataSource
import ai.vital.sql.schemas.SchemasUtils;
import ai.vital.sql.service.VitalServiceSql;
import ai.vital.sql.service.VitalServiceSqlConfigCreator;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
import ai.vital.triplestore.allegrograph.AllegrographWrapper
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.admin.VitalServiceAdmin
import ai.vital.vitalservice.command.AbstractCommand
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.factory.VitalServiceFactory.ServiceConfigWrapper;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalOrganization
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory


class VitalIndexDBCommand extends AbstractCommand {

	static String CMD_HELP = 'help'
	
	static String CMD_INIT = 'init'
	
	static String CMD_LIST_APPS = 'listapps'
	
	static String CMD_REMOVE_APP = 'removeapp'
	
	static String CMD_ADD_APP = 'addapp'
	
	static String CMD_LIST_SEGMENTS = 'listsegments'
	
	static String CMD_ADD_SEGMENT = 'addsegment'
	
	static String CMD_REMOVE_SEGMENT = 'removesegment'
	
	static String CMD_REINDEX_SEGMENT = 'reindexsegment'
	
	static String CMD_VERIFY_INDEX = 'verifyindex'
	
	static String CMD_VERIFY_INDEXES = 'verifyindexes'
	
	static String CMD_REBUILD_INDEXES = 'rebuildindexes'
	
	static String VIDB = 'vitalindexdb'
	
	static Map cmd2CLI = [:]
	
	static {
		
		def listAppsCLI = new CliBuilder(usage: "${VIDB} ${CMD_LIST_APPS} [options]", stopAtNonOption: false)
		listAppsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_LIST_APPS, listAppsCLI)
		
		def initCLI = new CliBuilder(usage: "${VIDB} ${CMD_INIT}", stopAtNonOption: false)
		initCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			rk longOpt: 'root-key', 'optional root key, xxxx-xxxx-xxxx format', args: 1, required: false
		}
		cmd2CLI.put(CMD_INIT, initCLI)
		
		def addAppCLI = new CliBuilder(usage: "${VIDB} ${CMD_ADD_APP} [options]", stopAtNonOption: false)
		addAppCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			a longOpt: 'app', 'app ID', args: 1, required: true
			n longOpt: 'name', 'app name', args: 1, required: true
		}
		cmd2CLI.put(CMD_ADD_APP, addAppCLI)
		
		def removeAppCLI = new CliBuilder(usage: "${VIDB} ${CMD_REMOVE_APP} [options]", stopAtNonOption: false)
		removeAppCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			a longOpt: 'app', 'app ID', args: 1, required: true
		}
		cmd2CLI.put(CMD_REMOVE_APP, removeAppCLI)
		
		def listSegmentsCLI = new CliBuilder(usage: "${VIDB} ${CMD_LIST_SEGMENTS} [options]", stopAtNonOption: false)
		listSegmentsCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			a longOpt: 'appID', 'app ID', args:1, required: true
//			s longOpt: 'segment', 'segment ID', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_SEGMENTS, listSegmentsCLI)
		
		def removeSegmentCLI = new CliBuilder(usage: "${VIDB} ${CMD_REMOVE_SEGMENT} [options]", stopAtNonOption: false)
		removeSegmentCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			d longOpt: 'deleteData', 'delete data', args: 0, required: false
		}
		cmd2CLI.put(CMD_REMOVE_SEGMENT, removeSegmentCLI)
		
		def addSegmentCLI = new CliBuilder(usage: "${VIDB} ${CMD_ADD_SEGMENT} [options]", stopAtNonOption: false)
		addSegmentCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
			ro longOpt: 'readOnly', 'read only', args: 0, required: false
			p longOpt: 'provisioningFile', '(dynamodb database type only) provisioning config file', args: 1, required: false
		}
		cmd2CLI.put(CMD_ADD_SEGMENT, addSegmentCLI)
		
		def reindexSegmentCLI = new CliBuilder(usage: "${VIDB} ${CMD_REINDEX_SEGMENT} [options]", stopAtNonOption: false)
		reindexSegmentCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
		}
		cmd2CLI.put(CMD_REINDEX_SEGMENT, reindexSegmentCLI)
		
		
		def verifyIndexCLI = new CliBuilder(usage: "${VIDB} ${CMD_VERIFY_INDEX} [options]", stopAtNonOption: false)
		verifyIndexCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			a longOpt: 'appID', 'app ID', args:1, required: true
			s longOpt: 'segmentID', 'segment ID', args: 1, required: true
		}
		cmd2CLI.put(CMD_VERIFY_INDEX, verifyIndexCLI)
		
		def verifyIndexesCLI = new CliBuilder(usage: "${VIDB} ${CMD_VERIFY_INDEXES} (no options)", stopAtNonOption: false)
		verifyIndexesCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			
		}
		cmd2CLI.put(CMD_VERIFY_INDEXES, verifyIndexesCLI)
		
		
		def rebuildIndexesCLI = new CliBuilder(usage: "${VIDB} ${CMD_REBUILD_INDEXES} (no options)", stopAtNonOption: false)
		rebuildIndexesCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_REBUILD_INDEXES, rebuildIndexesCLI)
		
		
		def datamigrateCLI = getDataMigrateCLI(VIDB)
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

		Config cfg = endpointConfigPart(options, EndpointType.INDEXDB)
		
		String profile = options.prof ? options.prof : VitalServiceFactory.DEFAULT_PROFILE
		
		if(cmd == CMD_INIT) {
			
			ServiceConfigWrapper wrappedCfg = VitalServiceFactory.getProfileConfig(profile)

			VitalServiceIndexedDBConfig ldConfig = wrappedCfg.serviceConfig
			
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
			
		} else if(cmd == CMD_REINDEX_SEGMENT) {
		
			String segmentID = options.s
			String appID = options.a
			
			
			
			println "Reindexing segment ${segmentID}, app ${appID} ..."
			
			VitalApp app = getApp(serviceAdmin, appID)
			
			if(app == null) return
			
			if( ! (serviceAdmin instanceof VitalServiceAdminIndexedDB) ) {
				println "Cannot reindex a non IndexedDB endpoint"
				return
			}
			
			VitalSegment segment = serviceAdmin.getSegment(app, segmentID)
			if(segment == null) {
				error "segment not found, appID: ${appID} segmentID: ${segmentID}"
				return
			}
			
			VitalServiceAdminIndexedDB impl = serviceAdmin
			
			VitalStatus status = impl.reindexSegment(app, segment)
			
			println "Status: ${status}"
		
		} else if(cmd == CMD_VERIFY_INDEX) {
		
			String segmentID = options.s
			String appID = options.a
		
			println "Verifying index, segment ${segmentID}, app: ${appID} ..."
			
			VitalApp app = getApp(serviceAdmin, appID)
			
			if( ! (serviceAdmin instanceof VitalServiceAdminIndexedDB) ) {
				println "Cannot reindex a non IndexedDB endpoint"
				return
			}
			
			VitalSegment segment = serviceAdmin.getSegment(app, segmentID)
			if(segment == null) {
				error "segment not found, appID: ${appID} segmentID: ${segmentID}"
				return
			}
			
			VitalServiceAdminIndexedDB impl = serviceAdmin
			
			VitalStatus status = impl.verifyIndex(app, segment)
			
			println "Status: ${status}"
			
		} else if(cmd == CMD_VERIFY_INDEXES) {
		
			println "Verifying indices/segments..."
		
			try {
				
				VitalStatus status = ((VitalServiceAdminIndexedDB)serviceAdmin).verifyIndices()
				
				println "Status: ${status}"
				
			} catch(Exception e) {
				e.printStackTrace()
				println "Exception: ${e.localizedMessage}"
			}
			
		} else if(cmd == CMD_REBUILD_INDEXES) {
		
			println "Rebuilding all indexes..."
			
			println "Initializing index..."
			
			try {
				
				VitalStatus status = ((VitalServiceAdminIndexedDB)serviceAdmin).rebuildIndices()
					
				println "Status: ${status}"
					
			} catch(Exception e) {
				e.printStackTrace()
				println "Exception: ${e.localizedMessage}"
			}
			
		} else if(cmd == CMD_DATA_MIGRATE) {
		
			String appID = options.a
			
			VitalApp app = getApp(serviceAdmin, appID)
			
			if(app == null) return
		
			handleDataMigrate(VIDB, options, serviceAdmin, app)
			
		} else {
			
			println "Unhandled command: ${cmd}"	
		
		}

		
		
//		LuceneServiceDiskImpl.initializeRoot(File)
	}
	
	static void usage() {
		
		println "usage: ${VIDB} <command> [options] ..."
		
		println "usage: ${VIDB} ${CMD_HELP} (prints usage)"
		
		for(Entry e : cmd2CLI.entrySet()) {
			e.value.usage()
		}

	}

	
}
