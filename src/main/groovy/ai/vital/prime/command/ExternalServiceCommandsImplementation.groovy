package ai.vital.prime.command

import java.util.Map
import java.util.regex.Pattern

import org.apache.commons.io.FileUtils;

import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.admin.VitalServiceAdmin;;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.model.DatabaseConnection
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.SparqlDatabaseConnection
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalServiceConfig

class ExternalServiceCommandsImplementation extends VitalPrimeCommand {

	public final static String SERVICES_MANAGEMENT_DATASCRIPT = 'commons/admin/ServicesManagementScript.groovy'
	
	static String CMD_LIST_EXTERNAL_SERVICES = 'listexternalservices'
	
	static String CMD_ADD_EXTERNAL_SERVICE = 'addexternalservice'
	
	static String CMD_REMOVE_EXTERNAL_SERVICE = 'removeexternalservice'
	
	static String CMD_CONNECT_EXTERNAL_SERVICE = 'connectexternalservice'
	
	static String CMD_DISCONNECT_EXTERNAL_SERVICE = 'disconnectexternalservice'
	
	static void initCommands(Map cmd2CLI) {
	
		def listExternalServicesCLI = new CliBuilder(usage: "${VP} ${CMD_LIST_EXTERNAL_SERVICES} [options]", stopAtNonOption: false)
		listExternalServicesCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
			a longOpt: 'app', 'optional appID regex filter', args: 1, required: false
			n longOpt: 'name', 'optional name regex filter', args: 1, required: false
			v longOpt: 'verbose', 'print service config details', args: 0, required: false 
			
		}
		cmd2CLI.put(CMD_LIST_EXTERNAL_SERVICES, listExternalServicesCLI)
		
		
		def addExternalServiceCLI = new CliBuilder(usage: "${VP} ${CMD_ADD_EXTERNAL_SERVICE} [options]", stopAtNonOption: false)
		addExternalServiceCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
			a longOpt: 'app', 'app ID', args: 1, required: true
			n longOpt: 'name', "service name", args: 1, required: true
			t longOpt: 'type', "service type, one of: [vitalservice, database]", args: 1, required: true  
			cf longOpt: 'config-file', "service config file", args: 1, required: true
			esk longOpt: 'external-service-key', 'required when type=vitalservice', args: 1, required: false
		}
		cmd2CLI.put(CMD_ADD_EXTERNAL_SERVICE, addExternalServiceCLI)
		
		def removeExternalServiceCLI = new CliBuilder(usage: "${VP} ${CMD_REMOVE_EXTERNAL_SERVICE} [options]", stopAtNonOption: false)
		removeExternalServiceCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
			a longOpt: 'app', 'app ID', args: 1, required: true
			n longOpt: 'name', "service name", args: 1, required: true
			t longOpt: 'type', "service type, one of: [vitalservice, database]", args: 1, required: true  
		}
		cmd2CLI.put(CMD_REMOVE_EXTERNAL_SERVICE, removeExternalServiceCLI)
		
		def connectServiceCLI = new CliBuilder(usage: "${VP} ${CMD_CONNECT_EXTERNAL_SERVICE} [options]", stopAtNonOption: false)
		connectServiceCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
			a longOpt: 'app', 'app ID', args: 1, required: true
			n longOpt: 'name', "service name", args: 1, required: true
		}
		cmd2CLI.put(CMD_CONNECT_EXTERNAL_SERVICE, connectServiceCLI)
		
		def disconnectServiceCLI = new CliBuilder(usage: "${VP} ${CMD_DISCONNECT_EXTERNAL_SERVICE} [options]", stopAtNonOption: false)
		disconnectServiceCLI.with {
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ak longOpt: 'admin-key', 'admin key, xxxx-xxxx-xxxx format', args: 1, required: true
			a longOpt: 'app', 'app ID', args: 1, required: true
			n longOpt: 'name', "service name", args: 1, required: true
		}
		cmd2CLI.put(CMD_DISCONNECT_EXTERNAL_SERVICE, disconnectServiceCLI)
			
	}
	
	static boolean handleCommand(def options, String cmd, VitalServiceAdmin serviceAdmin) {
		
		if(cmd == CMD_LIST_EXTERNAL_SERVICES) {
			
			String appIDFilter = options.a ? options.a : null
			
			println "appID regex filter: ${appIDFilter ? appIDFilter : ''}"
			
			String nameFilter = options.n ? options.n : null
			println "name regex filter: ${nameFilter ? nameFilter: ''}"
						
			boolean verbose = options.v ? true : false
			println "verbose ? ${verbose}"
			
			ResultList rl = serviceAdmin.callFunction(null, SERVICES_MANAGEMENT_DATASCRIPT, [action: 'listServices', app: null, withDBConnections: true] )
			
			if(rl.status.status != VitalStatus.Status.ok) error("Listing external services error: " + rl.status.message)
			
			List<VITAL_Node> primaryCfgAndDBs = []
				
				//filter out nodes and edges ?
				
			List<GraphObject> rest = []
			
			VITAL_Container container = rl.toContainer(false)
				
			for(GraphObject g : rl) {
					
				if(g instanceof VitalServiceConfig) {
						
					if( g.primary ) {
						primaryCfgAndDBs.add(g)
					}
						
				} else if(g instanceof DatabaseConnection) {
					
					primaryCfgAndDBs.add(g)
					
				} else {
					rest.add(g)
				}
					
			}
				
			//sort filter and trim
			List<VITAL_Node> outputPrimary = []
	
			Pattern appIDFilterPattern = null
			Pattern nameFilterPattern = null

			if(appIDFilter) {
				appIDFilterPattern = Pattern.compile(appIDFilter, Pattern.CASE_INSENSITIVE)
			}
							
			if(nameFilter) {
				nameFilterPattern = Pattern.compile(nameFilter, Pattern.CASE_INSENSITIVE)
			}
			
			int globalTotalResults = 0
				
			for(VITAL_Node s : primaryCfgAndDBs) {
				
				globalTotalResults++

				if(appIDFilterPattern) {
					if(s instanceof VitalServiceConfig) {
						
						if(appIDFilterPattern.matcher(s.targetAppID.toString()).matches()) {
							
						} else {
							continue
						}
						
					} else if(s instanceof DatabaseConnection){
					
						if(appIDFilterPattern.matcher(s.appID.toString()).matches()) {
							
						} else {
							continue
						}
					
					}
				}
									
				if(nameFilterPattern) {
					
					if(nameFilterPattern.matcher(s.name.toString()).matches()) {
							
					} else {
						continue
					}
						
						
				}
					
				outputPrimary.add(s)
					
			}
	
			//after filters applied
			rl.totalResults = primaryCfgAndDBs.size()
				
			outputPrimary.sort { VITAL_Node s1, VITAL_Node s2 ->
				return s1.name.toString().compareToIgnoreCase(s2.name.toString())
			}
				
			
			int offset = 0
			int limit = Integer.MAX_VALUE;
				
			if(offset > 0) {
					
				outputPrimary = outputPrimary.size() > offset ? outputPrimary.subList(offset, outputPrimary.size()) : []
					
			}
				
			if(outputPrimary.size() > limit) {
				outputPrimary = outputPrimary.subList(0, limit)
			}
				
//				
//			for(VITAL_Node s : outputPrimary) {
//				rl.addResult(s, 1d)
//			}
//			
//			for(GraphObject r : rest) {
//				rl.addResult(r, 2d)
//			}
//			rl.status.successes = globalTotalResults
				
			int index = 0
			
			println "Total services count: ${primaryCfgAndDBs.size()}"
			if(appIDFilter || nameFilter) println "Filtered services count: ${outputPrimary.size()}"
			
			for(VITAL_Node serviceObj : outputPrimary) {
				
				index++
				
				String name = serviceObj.name
				
				Boolean readOnly = false;
				
				String tLabel = null;
				
				String appID = null;
				
				String isService = false;
				
				String connectionState = null;
				String connectionError = null;
				
				String readOnlyLabel = ''
				
				if(serviceObj instanceof VitalServiceConfig) {
					
					isService = true;
					tLabel = "VitalService";
					appID = serviceObj.targetAppID
					connectionState = serviceObj.connectionState
					connectionError = serviceObj.connectionError
					
				} else if(serviceObj instanceof DatabaseConnection){
					if(serviceObj instanceof SparqlDatabaseConnection) {
						tLabel = '' + serviceObj.endpointType + ' SparQL';
					} else {
						tLabel = '' + serviceObj.endpointType + ' SQL';
						//sql
					}
					
					tLabel += " External Database";
					
					//only databases are read only
					readOnly = serviceObj.readOnly
					
					if(readOnly == null) readOnly = false;
					
					appID = serviceObj.appID
					
					connectionState = "external"
					
					readOnlyLabel = "${readOnly.booleanValue() ? '[READ ONLY]' : ''}"
					
				} else {
					tLabel = "(unknown)";
				}
				
				
				println "${index}   app: ${appID}   Name: ${name}   Type: ${tLabel}   Status: ${connectionState} ${connectionError ? ( '(' + connectionError + ')') : ''} ${readOnlyLabel}"
				if(verbose) {
					
					if(serviceObj instanceof VitalServiceConfig) {
						println "serviceKey: ${serviceObj.key}"
					}
					
					println "config string:"
					println serviceObj.configString
					println ""
//					
				}
				
			}
			
		} else if(cmd == CMD_ADD_EXTERNAL_SERVICE) {
		
			String appID = options.a
			println "appID: ${appID}"
			String name = options.n
			println "service name: ${name}"
			String type = options.t
			println "type: ${type}"
			
			if(!( type == 'vitalservice' || type == 'database')) error("Service type unknown: ${type}, valid types: [vitalservice, database]")
			
			File configFile = new File(options.cf)
			println "config file: ${configFile.absolutePath}"
			
			String authKey = options.esk ? options.esk : null
			
			if(type == 'vitalservice' ) {
				if(!authKey) error("external-service-key is required if type = vitalservice")
				println "service auth key: ${authKey}"
			}
			
			String config = FileUtils.readFileToString(configFile, "UTF-8")
			
			println "service config data:\n${config}"
			
			VitalApp app = getApp(serviceAdmin, appID)
			
			
			ResultList rl = serviceAdmin.callFunction(null, SERVICES_MANAGEMENT_DATASCRIPT, [action: 'addService', app: app, name: name, sType: type, config: config, authKey: authKey])
			
			if(rl.status.status != VitalStatus.Status.ok) error("Adding external service error: " + rl.status.message)
			
			println "${rl.status}"
			
		} else if(cmd == CMD_REMOVE_EXTERNAL_SERVICE ) {
		
			String appID = options.a
			println "appID: ${appID}"
			String name = options.n
			println "service name: ${name}"
			String type = options.t
			println "type: ${type}"
		
			if(!( type == 'vitalservice' || type == 'database')) error("Service type unknown: ${type}, valid types: [vitalservice, database]")
			
			VitalApp app = getApp(serviceAdmin, appID)
			
			ResultList rl = serviceAdmin.callFunction(null, SERVICES_MANAGEMENT_DATASCRIPT, [action: 'removeService', app: app, name: name, sType: type])
		
			if(rl.status.status != VitalStatus.Status.ok) error("Removing external service error: " + rl.status.message)
			
			println "${rl.status}"

		} else if(cmd == CMD_CONNECT_EXTERNAL_SERVICE || cmd == CMD_DISCONNECT_EXTERNAL_SERVICE) {

			String appID = options.a
			println "appID: ${appID}"
			String name = options.n
			println "service name: ${name}"
		
			String state = cmd == CMD_CONNECT_EXTERNAL_SERVICE  ? 'connected' : 'disconnected'  
			
			println "new state: ${state}"
			
			ResultList rl = serviceAdmin.callFunction(null, SERVICES_MANAGEMENT_DATASCRIPT, [action: 'updateConnectionState', app: VitalApp.withId(appID), name: name, state: state])
			
			if(rl.status.status != VitalStatus.Status.ok) error("Adding external service error: " + rl.status.message)
			
			println "${rl.status}"
			
		} else {
				
			return false
			
		}
		
		return true
		
	}
	
}
