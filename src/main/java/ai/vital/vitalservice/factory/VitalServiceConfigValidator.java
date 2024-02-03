package ai.vital.vitalservice.factory;

import java.io.File;
import java.io.IOException;

import ai.vital.indexeddb.service.config.VitalServiceIndexedDBConfig;
import ai.vital.lucene.disk.service.config.VitalServiceLuceneDiskConfig;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalservice.exception.VitalServiceException;

public class VitalServiceConfigValidator {

	/**
	 * @param config
	 * @throws VitalServiceException
	 */
	public static void checkIfServiceCanBeOpened(String name, VitalServiceConfig config) throws VitalServiceException {
		
		VitalServiceLuceneDiskConfig diskCfg = diskCfg(config);
		
		if(diskCfg == null) return;

		
		File srcPath = null;
		try {
			srcPath = new File(diskCfg.getRootPath()).getCanonicalFile();
		} catch (IOException e) {
			throw new VitalServiceException(e);
		}
		
		
		for(VitalServiceConfig c : VitalServiceFactory.listAllOpenServicesConfig()) {
			
			VitalServiceLuceneDiskConfig ldCfg = diskCfg(c);
				
			if(ldCfg == null) continue;
				
			checkLucenePaths(name, srcPath, ldCfg);
				
		}
		
	}
	
	private static VitalServiceLuceneDiskConfig diskCfg(VitalServiceConfig config) {
		
		VitalServiceLuceneDiskConfig diskCfg = null;
		
		if(config instanceof VitalServiceLuceneDiskConfig) {
			diskCfg = (VitalServiceLuceneDiskConfig) config;
		} else if(config instanceof VitalServiceIndexedDBConfig) {
			VitalServiceIndexedDBConfig icfg = (VitalServiceIndexedDBConfig) config;
			if( icfg.getIndexConfig() instanceof VitalServiceLuceneDiskConfig ) {
				diskCfg = (VitalServiceLuceneDiskConfig) icfg.getIndexConfig();
			}
		}
		
		return diskCfg;
		
	}
	
	private static void checkLucenePaths(String name, File srcPath, VitalServiceLuceneDiskConfig targetConfig) throws VitalServiceException {
		File destPath = null;
		try {
			destPath = new File(targetConfig.getRootPath()).getCanonicalFile();
		} catch (IOException e) {
			throw new VitalServiceException(e);
		}
		
		if(destPath.equals(srcPath)) throw new VitalServiceException("Cannot open a service instance, service '" + name + "' already has an lucene index at location: " + srcPath.getAbsolutePath());
	}
}
