package ai.vital.prime.command

import java.util.Map;
import java.util.Map.Entry

import org.apache.commons.io.FileUtils;



import ai.vital.domain.Datascript
import ai.vital.domain.DatascriptRun;
import ai.vital.domain.Job;
import ai.vital.vitalservice.EndpointType;
import ai.vital.prime.service.admin.VitalServiceAdminPrime
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.VitalStatus.Status;
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.command.AbstractCommand;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.java.VitalJavaSerializationUtils;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalServiceAdminKey
import ai.vital.vitalsigns.model.property.URIProperty;

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory


class VitalDatascriptCommand extends AbstractCommand {

	static String CMD_HELP = 'help'
	
	static String CMD_LIST_DATASCRIPTS = 'listdatascripts'
	
	static String CMD_LIST_JOBS = 'listjobs'
	
	static String CMD_GET_DATASCRIPT = 'getdatascript'
	
	static String CMD_ADD_DATASCRIPT = 'adddatascript'
	
	static String CMD_REMOVE_DATASCRIPT = 'removedatascript'
	
	static String CMD_RUN_DATASCRIPT = 'rundatascript'
	
	//list all jobs, optional app filter
	static String CMD_LIST_TASKS = 'listtasks'
	
	static String CMD_KILL_TASK = 'killtask'
	
	static String CMD_GET_TASK_LOG = 'gettasklog'
	
	static String CMD_GET_TASK_RESULTS = 'gettaskresults'
	
	static String VDS = 'vitaldatascript'
	
	
	static Map cmd2CLI = new LinkedHashMap()
	
	static {
		
		def listDatascriptsCLI = new CliBuilder(usage: "${VDS} ${CMD_LIST_DATASCRIPTS} [options]", stopAtNonOption: false)
		listDatascriptsCLI.with {
			p longOpt: 'path', 'scripts base path: admin/* <app>/* or commons/admin/* or commons/scripts/*', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_DATASCRIPTS, listDatascriptsCLI)
		
		def listJobsCLI = new CliBuilder(usage: "${VDS} ${CMD_LIST_JOBS} [options]", stopAtNonOption: false)
		listJobsCLI.with {
			p longOpt: 'path', 'jobs base path: admin/* <app>/* or commons/admin/* or commons/scripts/*', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_JOBS, listJobsCLI)
		
		def getDatascriptCLI = new CliBuilder(usage: "${VDS} ${CMD_GET_DATASCRIPT} [options]", stopAtNonOption: false)
		getDatascriptCLI.with {
			p longOpt: 'path', 'scripts base path: admin/<script_name> <app>/<script_name> or commons/admin/<script_name> or commons/scripts/<script_name>', args: 1, required: true
			o longOpt: 'output', 'optional output file to save the script body to', args: 1, required: false
			ow longOpt: 'overwrite', 'overwrite output file if exists', args: 0, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_GET_DATASCRIPT, getDatascriptCLI)
		
		def addDatascriptCLI = new CliBuilder(usage: "${VDS} ${CMD_ADD_DATASCRIPT} [options]", stopAtNonOption: false)
		addDatascriptCLI.with {
			p longOpt: 'path', 'script path: admin/<script_name> <app>/<script_name> or commons/admin/<script_name> or commons/scripts/<script_name>', args: 1, required: true
			f longOpt: 'file', 'script input file path', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_ADD_DATASCRIPT, addDatascriptCLI)
		
		def removeDatascriptCLI = new CliBuilder(usage: "${VDS} ${CMD_REMOVE_DATASCRIPT} [options]", stopAtNonOption: false)
		removeDatascriptCLI.with {
			p longOpt: 'path', 'script path: admin/<script_name> <app>/<script_name> or commons/admin/<script_name> or commons/scripts/<script_name>', args: 1, required: true
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_REMOVE_DATASCRIPT, removeDatascriptCLI)
		
		
		def runDatascriptCLI = new CliBuilder(usage: "${VDS} ${CMD_RUN_DATASCRIPT} [options]", stopAtNonOption: false)
		runDatascriptCLI.with {
			p longOpt: 'path', 'script path: admin/<script_name> <app>/<script_name> or commons/admin/<script_name> or commons/scripts/<script_name>', args: 1, required: true
			i longOpt: 'input', 'input params groovy file - must return a map of parameters', args: 1, required: true
//			o longOpt: 'output', 'output results path, optional, prints results to screen otherwise', args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
			a longOpt: 'appID', 'optional appID (script context)', args: 1, required: false
			t longOpt: 'task', 'run the script as a background task', args: 0, required: false
		}
		cmd2CLI.put(CMD_RUN_DATASCRIPT, runDatascriptCLI)
		
		
		def listTasksCLI = new CliBuilder(usage: "${VDS} ${CMD_LIST_TASKS} [options]", stopAtNonOption: false)
		listTasksCLI.with {
			t longOpt: 'taskID', 'optional taskID used as a filter', args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_LIST_TASKS, listTasksCLI)
		
		def killTaskCLI = new CliBuilder(usage: "${VDS} ${CMD_KILL_TASK} [options]", stopAtNonOption: false)
		killTaskCLI.with {
			t longOpt: 'taskID', 'taskID to kill', args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_KILL_TASK, killTaskCLI)
		
		
		def getTaskLogCLI = new CliBuilder(usage: "${VDS} ${CMD_GET_TASK_LOG} [options]", stopAtNonOption: false)
		getTaskLogCLI.with {
			t longOpt: 'taskID', 'taskID to get log', args: 1, required: true
			o longOpt: 'outputFile', 'optional output file, logs to console otherwise', args: 1, required: false
			ow longOpt: 'overwrite', 'overwrite output file if exists', args: 0, required : false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_GET_TASK_LOG, getTaskLogCLI)
		
		def getTaskResultsCLI = new CliBuilder(usage: "${VDS} ${CMD_GET_TASK_RESULTS} [options]", stopAtNonOption: false)
		getTaskResultsCLI.with {
			t longOpt: 'taskID', 'taskID to get log', args: 1, required: true
			o longOpt: 'outputFile', 'optional output file, formats: .object, .vital[.gz], prints results to console otherwise (vital block format)', args: 1, required: false
			ow longOpt: 'overwrite', 'overwrite output file if exists', args: 0, required : false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			k longOpt: 'key', 'vitalservice admin key', args: 1, required: true
		}
		cmd2CLI.put(CMD_GET_TASK_RESULTS, getTaskResultsCLI)
		
	}
	
	
	static Class superAdminDatascriptClass = null
			
	static Class superAdminJobClass = null
	
	
	public static void main(String[] args) {
		
		
		
		try {
			superAdminDatascriptClass = Class.forName('ai.vital.superadmin.SuperAdminDatascript');
		}catch(Exception e) {}
		
		try {
			superAdminJobClass = Class.forName('ai.vital.superadmin.SuperAdminJob');
		}catch(Exception e) {}
		
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
		VitalServiceAdminKey key = new VitalServiceAdminKey()
		key.generateURI((VitalApp)null)
		key.key = options.k
		VitalServiceAdmin serviceAdmin = VitalServiceFactory.openAdminService(key, profile)
		
		
		if(!(serviceAdmin instanceof VitalServiceAdminPrime)) {
			error "Expected instanceof ${VitalServiceAdminPrime.class.canonicalName}"
		}
		
		VitalServiceAdminPrime primeServiceAdmin = (VitalServiceAdminPrime) serviceAdmin
		
		if(cmd == CMD_LIST_DATASCRIPTS) {
			
			String path = options.p
			
			println "Listing datascripts, path: ${path}"
			
			if(path.contains("../")) error("Cannot use upper directory listing (../)")
			
			
			
			if(path == '*') {
				
				
			} else {
			
				String[] chunks = path.split("/")
			
			
				for(int c = 0; c < chunks.length; c++) {
					
					String chunk = chunks[c]
					
					if(c < chunks.length - 1 && chunk.contains("*")) {
						error("cannot use wildcard in organization/app path part")
					}
					
				}
			
			}
			
			List<GraphObject> scripts = null
			
			try {
				scripts = primeServiceAdmin.listDatascripts(path, false);
			} catch(Exception e) {
				error e.localizedMessage
			}
			
			//filter out
			
			for(Iterator<GraphObject> iter = scripts.iterator(); iter.hasNext(); ) {
				
				GraphObject script = iter.next();
				
				boolean job = script instanceof Job || ( superAdminJobClass != null && superAdminJobClass.isInstance(script) )
				
				if(job) {
					
					if( ! script.callable ) {
						iter.remove();
					}
					
				}
				
			}
			
			println "Scripts count: ${scripts.size()}"
			
			println ""
			
			for(int i = 1; i <= scripts.size(); i++) {
				
				GraphObject script = (GraphObject) scripts[i-1]
				
				println ("${i}. " + printScript(script, false))
				
			}
			
		} else if(cmd == CMD_LIST_JOBS) {
		
			String path = options.p
			
			println "Listing jobs, path: ${path}"
			
			if(path.contains("../")) error("Cannot use upper directory listing (../)")
			
			
			String[] chunks = path.split("/")
			
			for(int c = 0; c < chunks.length; c++) {
				
				String chunk = chunks[c]
				
				if(c < chunks.length - 1 && chunk.contains("*")) {
					error("cannot use wildcard in organization/app path part")
				}
				
			}
			
			List<GraphObject> jobs = null
			
			try {
				jobs = primeServiceAdmin.listDatascripts(path, false);
			} catch(Exception e) {
				error e.localizedMessage
			}
			
			//filter out
			
			for(Iterator<GraphObject> iter = jobs.iterator(); iter.hasNext(); ) {
				
				GraphObject script = iter.next();
				
				boolean job = script instanceof Job || ( superAdminJobClass != null && superAdminJobClass.isInstance(script) )
				
				if(!job) {
					
					iter.remove();
				}
				
			}
			
			println "Jobs count: ${jobs .size()}"
			
			println ""
			
			for(int i = 1; i <= jobs .size(); i++) {
				
				GraphObject script = (GraphObject) jobs[i-1]
				
				println ("${i}. " + printScript(script, false))
				
			}
			
		} else if(cmd == CMD_GET_DATASCRIPT) {
			
			String path = options.p
			
			File out = options.o ? new File(options.o) : null
			
			boolean overwrite = options.ow ? true : false
			
			println "Get datascript, path: ${path}"
			
			if(out) {
				
				println "Output file: ${out.absolutePath} overwrite if exists ? ${overwrite}"
				
				if(out.exists()) {
					
					if(!overwrite) error("Output file already exists: ${out.absolutePath} - use --overwrite option")
					
					if(out.isFile()) error("Output path exists but is not a file: ${out.absolutePath}")
					
				}
				
			}
			
			if(path.contains("../")) error("Cannot use upper directory listing (../)")
			
			if(path.contains("*")) error("Cannot use wildcards when getting a datascript (*)")
			
			List<GraphObject> scripts = null;
			try {
				scripts = primeServiceAdmin.listDatascripts(path, true);
			} catch(Exception e) {
				error e.localizedMessage
			}
		
			if(scripts.size() != 1) {
				error("Script not found: ${path}")
			}
			
			/*
			if(scripts.size() > 1) {
				error("More than 1 script found")
			}
			*/
			
			GraphObject script = scripts[0]
			
			println printScript(script, out == null)
			
			if(out != null) {
				
				FileUtils.writeStringToFile(out, script.scriptBody)
				
			}
			
			
		} else if(cmd == CMD_ADD_DATASCRIPT) {
		
			String path = options.p
			
			String f = options.f
			
			File file = new File(f)
			
			println "Adding/updating script, path: ${path}, file: ${file.absolutePath}"
			
			String body = FileUtils.readFileToString(file, "UTF-8");

			Datascript added = null
						
			try {
				
				added = primeServiceAdmin.addDatascript(path, body)
				
				println "Script added."
				
			} catch(Exception e) {
				error e.localizedMessage
			}
			
			println printScript(added, true)

		} else if(cmd == CMD_REMOVE_DATASCRIPT) {
		
			String path = options.p
			
			println "Removing script, path: ${path}"
			
			try {
				
				VitalStatus status =primeServiceAdmin.removeDatascript(path);
				
				println "status: ${status}"
				
			} catch(Exception e) {
				error e.localizedMessage
			}
			
		} else if(cmd == CMD_RUN_DATASCRIPT) {
		
			Map runParams = null;
		
			String path = options.p
			
			String input_f = options.i
			
			String appID = options.a ? options.a : null
			
			boolean asTask = options.t ? true : false
			
			println "Script path: ${path}"
			println "Input groovy map file: ${input_f}"
			println "optional appID: ${appID}"
			println "as task ? ${asTask}"
			
			String[] split = path.split("/");
			
			if(split.length < 2) error "No app part in path: ${path}"
			
//			String appID = split[ split.length -2 ]
			
			VitalApp app = null;
			
			if(appID) {
				
				app = serviceAdmin.getApp(appID)
				
				if(app == null) {
					error "App not found: ${appID}"
					return
				}
				
			} else {
			
			
				if(split.length == 2 ) {
					
					appID = split[0]
					
					app = serviceAdmin.getApp(appID)
					
					if(app == null) {
						error "App not found: ${appID}"
						return
					}
					
				}
			
			}
			
			Reader paramsReader = null;
			
			if(input_f) {
				
				paramsReader = new FileReader(new File(input_f));
				
			} else {
			
//				paramsReader = new BufferedReader(new InputStreamReader(System.in, 'UTF-8'));
				
			}
			
			
			Binding binding = new Binding();
			GroovyShell shell = new GroovyShell(binding);
			Object _object = shell.evaluate(paramsReader);
	
			if(!(_object instanceof Map)) throw new Exception("An input script must return a map of datascript params.")

			runParams = _object;			
		
//			String output_f = options.o
			
			BufferedOutputStream fos = null 
			
//			if(output_f) {
				
//				fos = new BufferedOutputStream(new FileOutputStream(new File(output_f)));
				
//			}
			
			OutputStream os = fos != null ? fos : System.out;
			
			
			ResultList rl = null
			try {
				
				if(asTask) {
					
					runParams.put('function', path)
					
					if(app != null) {
					
						println "Running task as a service datascript (app context)"
						
//						runJob_datascript, [function: EXPORT_TEMP_FILES_DATASCRIPT
						rl = primeServiceAdmin.callFunction(app, 'commons/scripts/RunJob.groovy', runParams);
							
					} else {
					
						println "Running task as an admin datascript"
					
						rl = primeServiceAdmin.callFunction(app, 'commons/admin/RunJob.groovy', runParams)
						
					}
					
				} else {
				
					rl = primeServiceAdmin.callFunction(app, path, runParams);
					
				}
				println("status: ${rl.status}")
			} catch(Exception e) {
				error e.localizedMessage
			}
			
			if(rl.status.status != Status.ok) {
				error rl.status.message
			}
			
			List<ResultElement> results = rl.results
			
			println "Total results: ${rl.totalResults}"
			println "Results count: ${results.size()}"
			
			int i = 0;
			for(ResultElement re : results) {
				i++
				println "${i}. ${re.graphObject}"
			}
			
			
		} else if(cmd == CMD_LIST_TASKS) {
		
			println "Listing tasks..."
			
			Map m = [:]
			
			if( options.t) {
				String taskID = options.t
				println "using taskID as a filter: " + taskID
				m.put('taskID', taskID)
				
			}
			
			//don't set app
			
			ResultList rl = null
			
			try {
				rl = primeServiceAdmin.callFunction(null, "commons/scripts/ListTasks.groovy", m) 
			} catch(Exception e) {
				error e.getLocalizedMessage()
			}
			
			println "All tasks count: ${rl.results.size()}"
	
			int i = 1
			
			for(ResultElement re : rl.results) {
				
				DatascriptRun dr = re.graphObject
				
				Date started = new Date(dr.timestamp.rawValue())
				
				String statusMsg = dr.name
				
				println "${i}. ${dr.jobID} ${dr.scriptPath} ${dr.jobStatus} ${started} ${statusMsg ? ('message: ' + statusMsg) : ''}"
				
				i++
				
			}
			
		} else if(cmd == CMD_GET_TASK_LOG) {
		
			String taskID = options.t
			
			println "Getting task log, taskID: ${taskID}"
			
			File outputFile = options.o ? new File(options.o) : null
			
			boolean overwrite = options.ow ? true : false
			
			if(outputFile != null) {
				
				println "Output file: ${outputFile.absolutePath}"
				println "Overwrite ? ${overwrite}"
				
				if(outputFile.exists()) {
					if(!overwrite) {
						error("Ouput file already exists: ${outputFile.absolutePath} - use --overwrite option")
						return
					}
				}
				
			}
			 
			String[] chunks = taskID.split("/")
			String logFileName = chunks[chunks.length - 1] + '.log'
			
			OutputStream os = null
			boolean close = false 
			if( outputFile != null ) {
				os =  new FileOutputStream(outputFile)
				close = true
			} else {
				os = System.out
			}
			
			VitalStatus downloadStatus = serviceAdmin.downloadFile(VitalApp.withId("commons"), URIProperty.withString('TASKS'), logFileName, os, close)
			
			if(close || downloadStatus.status != VitalStatus.Status.ok) {
				println "Status: ${downloadStatus}"
			}
			
		} else if(cmd == CMD_GET_TASK_RESULTS) {
		
			String taskID = options.t
			
			println "Getting task results, taskID: ${taskID}"
			
			File outputFile = options.o ? new File(options.o) : null
			
			boolean overwrite = options.ow ? true : false
			
			boolean blockOutput = false
			
			boolean objectOutput = false
			
			if(outputFile != null) {
			
				println "Output file: ${outputFile.absolutePath}"
				
				if(outputFile.name.endsWith(".vital") || outputFile.name.endsWith(".vital.gz") ) {
					blockOutput = true
				} else if(outputFile.name.endsWith(".object")) {
					objectOutput = true
				} else {
					error "Output name must end with .vital[.gz] or .object"
					return
				}
				
				println "Overwrite: ${overwrite}"
				
				if(outputFile.exists()) {
					if(!overwrite) {
						error("Ouput file already exists: ${outputFile.absolutePath} - use --overwrite option")
						return
					}
				}
				
			}
			
			String[] chunks = taskID.split("/")
			String resultsFileName = chunks[chunks.length - 1] + '.object'
			
			
			ByteArrayOutputStream os = new ByteArrayOutputStream() 
			
			VitalStatus downloadStatus = serviceAdmin.downloadFile(VitalApp.withId("commons"), URIProperty.withString('TASKS'), resultsFileName, os, true)
			
			if(downloadStatus.status != VitalStatus.Status.ok) {
				error "Results download status: ${downloadStatus}"
				return
			}
			
			Object o = VitalJavaSerializationUtils.deserialize(os.toByteArray())
			
			if(!(o instanceof ResultList)) {
				error("Downloaded object is not a ResultList object")
				return
			}
			
			ResultList rl = (ResultList) o
			
			println "Results status: ${rl.status}"
			println "Results objects count: ${rl.results.size()}"
			
			if(outputFile != null) {
			

				if(objectOutput) {
					
					FileUtils.writeByteArrayToFile(outputFile , os.toByteArray(), false)
					
				} else if(blockOutput) {
				
					BlockCompactStringSerializer serializer = new BlockCompactStringSerializer(outputFile)

					for(GraphObject g : rl) {					
						
						serializer.startBlock()
						serializer.writeGraphObject(g)
						serializer.endBlock()
						
					}
					
					serializer.close()
				
				} else {
				
					error("Unhandled output file format: " + outputFile.name)
				
				}
			
			} else {
			
				for(GraphObject g : rl) {
					
					println BlockCompactStringSerializer.BLOCK_SEPARATOR
					println g.toCompactString()
					
				}
			
			}
			
			
		} else if(cmd == CMD_KILL_TASK) {
		
		
			String taskID = options.t
			
			Map m = [taskID: taskID]
			
			ResultList rl = null	
			
			try {
				rl = primeServiceAdmin.callFunction(null, "commons/scripts/KillTask.groovy", m)
				
				println "Status: ${rl.status}"
				
			} catch(Exception e) {
				error e.getLocalizedMessage()
			}
			
		} else {
			error "Unhandled command: ${cmd}"
		}
	}
	
	static String printScript(GraphObject script, boolean printScriptBody) {
		
		boolean job = script instanceof Job || ( superAdminJobClass != null && superAdminJobClass.isInstance(script) ) 
		
		String appID = script instanceof Datascript ? " app:${script.appID}" : "" 
		String organizationID = script instanceof Datascript ? script.organizationID : ""
		
		String lastCompilationError = script.lastCompilationError
		
		String scriptPath = script.scriptPath
		
		Date lastModifiedDate = script.lastModifiedDate
		
		if(lastCompilationError != null) {
			
			return "${script.name} ${scriptPath} ${lastModifiedDate ? lastModifiedDate :''} COMPILATION FAILED: ${lastCompilationError}";
			
		}
		
		boolean regularScript = script instanceof Datascript && script.regularScript
		boolean adminScript = script instanceof Datascript && script.adminScript
		boolean superAdminScript = superAdminDatascriptClass != null && superAdminDatascriptClass.isInstance(script) 
		
		String type = null
		if( regularScript ) {
			type = 'regular'
		} else if(adminScript) {
			type = 'admin'
		} else if(superAdminScript) {
			type = 'superadmin'
		}
		
		
		
		String t = 'script'
		
		if(job) {
			if(script.callable) {
				t = 'script+job'
			} else {
				t = 'job'
			}
			
		}
		
		String s = "${script.name} ${scriptPath} [${t}] modified: ${lastModifiedDate ? lastModifiedDate :''} ${appID} type: ${type}";
		
		
		if(job) {
			
			GraphObject j = (GraphObject) script
			
			s += "\n\tstatus: ${j.active ? 'running' : 'not-running'}" 
			if(j.timestamp) s += "      last execution time: ${new Date(j.timestamp.rawValue())}"
			
			boolean callable = j.callable
			int interval = j.interval
			String timeUnit = j.intervalTimeUnit
			s += "\n\tcallable ? ${callable}, interval: ${interval} ${timeUnit}"
			
			 
		}
		
		if(printScriptBody) {
			s += ("\n\n" + script.scriptBody) 
		}
		
		return s
		
		
	}
	
	static void usage() {
		
		println "usage: ${VDS} <command> [options] ..."
		
		println "usage: ${VDS} ${CMD_HELP} (prints usage)"
		
		for(Entry e : cmd2CLI.entrySet()) {
			e.value.usage()
		}
		
	}
	
}
