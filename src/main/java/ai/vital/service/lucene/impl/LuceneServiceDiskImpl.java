package ai.vital.service.lucene.impl;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.lucene.exception.LuceneException;
import ai.vital.lucene.model.LuceneSegment;
import ai.vital.lucene.model.LuceneSegmentType;
import ai.vital.service.lucene.model.LuceneSegmentConfig;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalsigns.model.VitalSegment;

public class LuceneServiceDiskImpl extends LuceneServiceImpl {

	static String SYSTEM_SEGMENT_URI = "http://vital.ai/vital/system/" + VitalSegment.class.getSimpleName() + "/systemsegment";
	
	private final static Logger log = LoggerFactory.getLogger(LuceneServiceDiskImpl.class);
			
	private File rootLocation;
    
    private boolean bufferWrites = false;
    
    private int commitAfterNWrites = 1;
	
    private int commitAfterNSeconds = 10;
    
    private Thread commitThread = null;
   
    private Boolean active = false;
    
	private LuceneServiceDiskImpl(File rootLocation) {
		if(rootLocation == null) throw new NullPointerException("Null root location");
		this.rootLocation = rootLocation;
	}
	
	private LuceneServiceDiskImpl(File rootLocation, boolean bufferWrites, int commitAfterNWrites, int commitAfterNSeconds) {
		if(rootLocation == null) throw new NullPointerException("Null root location");
		this.rootLocation = rootLocation;
		this.bufferWrites = bufferWrites;
		this.commitAfterNWrites = commitAfterNWrites;
		this.commitAfterNSeconds = commitAfterNSeconds;
	}
	
	public static LuceneServiceDiskImpl create(File rootLocation, boolean bufferWrites, int commitAfterNWrites, int commitAfterNSeconds) {
		

		return new LuceneServiceDiskImpl(rootLocation, bufferWrites, commitAfterNWrites, commitAfterNSeconds);
	}
	
	@Override
	public void initializeRootIndices() throws LuceneException {

		if(rootLocation == null) throw new NullPointerException("Null root location");
		if(!rootLocation.exists()) throw new LuceneException("Root location does not exist: " + rootLocation.getAbsolutePath());
		if(!rootLocation.isDirectory()) throw new LuceneException("Root location is not a directory: " + rootLocation.getAbsolutePath());
		
		for(File file : rootLocation.listFiles()) {
		
			if(!file.isDirectory()) throw new LuceneException("Expected only directories in lucene disk impl. root: " + file.getAbsolutePath());
		}

	}

	@Override
	protected Map<String, LuceneSegment> openInitialSegments() {

		Map<String, LuceneSegment> r = new HashMap<String, LuceneSegment>();
		
		for(File file : rootLocation.listFiles()) {
			
			try {
				String segmentURI = URLDecoder.decode(file.getName(), "UTF-8");
				
				
				LuceneSegmentConfig config = new LuceneSegmentConfig(LuceneSegmentType.disk, true, true, file.getAbsolutePath());
				
				if( ! SYSTEM_SEGMENT_URI.equals(segmentURI) ) {
				
					//only use buffered settings if non system segment
					config.setBufferWrites(bufferWrites);
					config.setCommitAfterNWrites(commitAfterNWrites);
					
				}
				
				LuceneSegment s = new LuceneSegment(null, null, VitalSegment.withId(segmentURI), config);
				
				s.open();
				
				r.put(segmentURI, s);
				
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
		}
		
		this.active = true;
		
		if(bufferWrites && commitAfterNSeconds > 0) {
		
			commitThread = new Thread("lucene-disk-auto-committer"){
				@Override
				public void run() {

					log.debug("auto-committer started");
					
					while(active) {
						
						List<LuceneSegment> segmentsList = null;
						
						synchronized (segments) {
							segmentsList = new ArrayList<LuceneSegment>(segments.values());
						}
						
						log.debug("auto-committer checking {} segments", segmentsList.size());
						
						for(LuceneSegment ls : segmentsList) {
							
							long currentTs = System.currentTimeMillis();
							
							Long ts = ls.getFirstBufferedElementTimestamp();
							if(ts == null) continue;
							
							int bufferedOpsCount = ls.getBufferedOpsCount();
							
							long diff = currentTs - ts.longValue();
							
							if( diff >= ( commitAfterNSeconds * 1000L ) && bufferedOpsCount > 0 ) {
								
								log.debug("Force committing segment {} buffered ops - time criterion {}ms", ls.getID(), diff);
								
								try {
									ls.forceCommit();
									log.debug("Force committing segment {} OK, {} ops, time {}ms", ls.getID(), bufferedOpsCount, System.currentTimeMillis() - currentTs);
								} catch (IOException e) {
									log.error("Error when force committing segment {} - {}", ls.getID(), e.getLocalizedMessage());
								}
								
							}
							
						}
						
						synchronized (active) {
							try {
								active.wait(1000);
							} catch (InterruptedException e) {
								log.error(e.getLocalizedMessage());
							}
						}
						
					}
					
					super.run();
				}
			};
			commitThread.setDaemon(true);
			commitThread.start();
			
		}
		
		
		return r;
	}

	@Override
	protected LuceneSegmentConfig getConfig(VitalSegment segment) {
		File path = null;
		try {
			path = new File(rootLocation, URLEncoder.encode(segment.getURI(), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		
		LuceneSegmentConfig cfg = new LuceneSegmentConfig(LuceneSegmentType.disk, true, true, path.getAbsolutePath());
		
		if( ! SYSTEM_SEGMENT_URI.equals(segment.getURI()) ) {
			cfg.setBufferWrites(bufferWrites);
			cfg.setCommitAfterNWrites(commitAfterNWrites);
		}
		
		return cfg;
	}

	public static void init(File rootLocation) throws VitalServiceException {

		if(rootLocation.exists()) {
			
			log.warn("Root location already exists: {}", rootLocation.getAbsolutePath());
			
			if(!rootLocation.isDirectory()) throw new VitalServiceException("Root location exists but is not a directory: " + rootLocation.getAbsolutePath());
			
			if( !rootLocation.canExecute() ) throw new VitalServiceException("No -x mod flag for directory: " + rootLocation.getAbsolutePath());
			if( !rootLocation.canRead() ) throw new VitalServiceException("No -r mod flag for directory: " + rootLocation.getAbsolutePath());
			if( !rootLocation.canWrite() ) throw new VitalServiceException("No -w mod flag for directory: " + rootLocation.getAbsolutePath());
			
		} else {
			
			if( ! rootLocation.mkdirs() ) throw new VitalServiceException("Couldn't create root lucene path: " + rootLocation.getAbsolutePath());
			
		}
		
		
	}

	public boolean isBufferWrites() {
		return bufferWrites;
	}

	public int getCommitAfterNWrites() {
		return commitAfterNWrites;
	}

	public void forceCommit(String segmentURI) throws IOException {

		segments.get(segmentURI).forceCommit();
		
	}
	
	@Override
	public void close() {

		active = false;
		
		synchronized (active) {
			active.notifyAll();
		}
		
		super.close();
	}


}
