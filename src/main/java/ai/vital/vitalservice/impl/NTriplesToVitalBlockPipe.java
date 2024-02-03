package ai.vital.vitalservice.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;

import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.rdf.VitalNTripleIterator;

public class NTriplesToVitalBlockPipe {

	/**
	 * converts ntriples input stream to vital block input stream via pipes
	 * the pipe does not close the input stream
	 */
	public static InputStream ntriples2VitalBlockStream(final InputStream ntriplesInputStream) throws IOException {
		
		final PipedOutputStream pos = new PipedOutputStream(null);
		
		PipedInputStream pis = new PipedInputStream(pos);
		pos.connect(pis);
		
		Thread writerThread = new Thread(){
			
			@Override
			public void run() {
			
				
				try {
					
					VitalNTripleIterator iterator = new VitalNTripleIterator(ntriplesInputStream);
					
					OutputStreamWriter w = new OutputStreamWriter(pos, StandardCharsets.UTF_8);
					BlockCompactStringSerializer writer = new BlockCompactStringSerializer(w);
					
					while(iterator.hasNext()) {
						
						GraphObject g = iterator.next();
					
						writer.startBlock();
						writer.writeGraphObject(g);
						writer.endBlock();
						
					}
					
					writer.flush();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
//				BufferedReader reader = new BufferedReader(new InputStreamReader(ntriplesInputStream, StandardCharsets.UTF_8));
//				
//				BlockIterator blocksIterator = BlockCompactStringSerializer.getBlocksIterator(reader, false);
				
				
			};
			
		};
		
		writerThread.setDaemon(true);
		
		writerThread.start();
		
		return pis;
		
	}
	
}
