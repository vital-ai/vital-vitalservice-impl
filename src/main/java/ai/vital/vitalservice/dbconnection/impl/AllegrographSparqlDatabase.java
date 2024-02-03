package ai.vital.vitalservice.dbconnection.impl;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.impl.LiteralImpl;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.sparql.util.NodeFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;

import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.dbconnection.ExternalSparqlDatabase;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExternalSparqlQuery;
import ai.vital.vitalsigns.model.RDFStatement;
import ai.vital.vitalsigns.model.SparqlAskResponse;
import ai.vital.vitalsigns.model.SparqlBinding;
import ai.vital.vitalsigns.model.SparqlDatabaseConnection;
import ai.vital.vitalsigns.model.SparqlUpdateResponse;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.properties.Property_hasRdfObject;
import ai.vital.vitalsigns.model.properties.Property_hasRdfPredicate;
import ai.vital.vitalsigns.model.properties.Property_hasRdfSubject;
import ai.vital.vitalsigns.model.properties.Property_hasUpdatedTriplesCount;
import ai.vital.vitalsigns.model.properties.Property_isPositiveResponse;
import ai.vital.vitalsigns.query.VitalNTripleWriter;
import ai.vital.vitalsigns.rdf.RDFFormat;
import ai.vital.vitalsigns.utils.StringUtils;

public class AllegrographSparqlDatabase extends ExternalSparqlDatabase {

	
	protected static int defaultTimeoutSeconds = 30;
	
	private final static Logger log = LoggerFactory.getLogger(AllegrographSparqlDatabase.class);
	
	HttpConnectionManager connectionManager;
	
	public AllegrographSparqlDatabase() {
	
		MultiThreadedHttpConnectionManager cm = new MultiThreadedHttpConnectionManager();
		HttpConnectionManagerParams cmp = new HttpConnectionManagerParams();
		cmp.setDefaultMaxConnectionsPerHost(5);
		cmp.setMaxTotalConnections(20);
		cmp.setStaleCheckingEnabled(true);
		cm.setParams(cmp);
		
		connectionManager = cm;

	}


	protected long timeout(VitalExternalSparqlQuery sq) {

		Integer timeout = sq.getTimeout();
		if(timeout != null) {
			if(timeout.intValue() < 1) {
				throw new RuntimeException("Query timeout seconds must be greater than 0 (" + timeout + ")");
			}
		} else {
			timeout = defaultTimeoutSeconds;
		}
		
		return timeout.longValue();
	}
	
	@Override
	public void validateConfig(SparqlDatabaseConnection connection) {
		
	}

	@Override
	public ResultList query(SparqlDatabaseConnection connection, VitalExternalSparqlQuery sq) {

		long timeout = timeout(sq);
		
		ResultList response = null;
		
		ExecutorService pool = Executors.newSingleThreadExecutor();

		Future<ResultList> future = null;
		
		AGCallable qc = null;
		
		try {
			
			qc = new AGCallable(connection, sq);
			
			future = pool.submit(qc);
			response = future.get(timeout, TimeUnit.SECONDS);
			
		} catch(TimeoutException e) {

			log.warn("Query timed out: ", sq.getSparql());
			
		} catch(Exception e) {
			
			response = new ResultList();
			response.setStatus(VitalStatus.withError("Query exception: " + e.getLocalizedMessage()));
			
			if(future != null) {
				try { future.cancel(true); } catch(Exception ex) {}
			}
			
		} finally {
			
			qc.shutdown();
			
			pool.shutdownNow();
			
		}
		
		if(response == null) {
			response = new ResultList();
			response.setStatus(VitalStatus.withError("Query timed out."));
		}
		
		return response;
		
	}
	
	
	class AGCallable implements Callable<ResultList> {

		SparqlDatabaseConnection connection;
		
		VitalExternalSparqlQuery sq;
		
		public AGCallable(SparqlDatabaseConnection connection, VitalExternalSparqlQuery sq) {
			super();
			this.connection = connection;
			this.sq = sq;
		}



		public void shutdown() {}



		@SuppressWarnings("unchecked")
		@Override
		public ResultList call() throws Exception {

			AllegrographSparqlEndpointClient client = new AllegrographSparqlEndpointClient(connectionManager, connection);
		
		
			//determine what kind of sparql is that
			ResultList rl = new ResultList();
		
			if(StringUtils.isEmpty( sq.getSparql() ) ) throw new RuntimeException("sparql string not set in query");

			Query queryObj = null;
		
			String queryError = null;
			String updateError = null;
			
			try {
				queryObj = QueryFactory.create(sq.getSparql());
			} catch(Exception e) {
				queryError = e.getLocalizedMessage();
			}
			
			if(queryObj != null) {
				
				
				if(queryObj.isAskType()) {
					
					boolean res = false;
					try {
						res = client.sparqlAskQuery(sq.getSparql());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					
					SparqlAskResponse askResponse = new SparqlAskResponse();
					askResponse.generateURI((VitalApp)null);
					askResponse.set( Property_isPositiveResponse.class, res );
					
					rl.getResults().add(new ResultElement(askResponse, 1D));
					
				} else if(queryObj.isConstructType()) {
				
					InputStream input = null;
					
					Model outputModel = ModelFactory.createDefaultModel();
					
					try {
						
						input = client.sparqlGraphNTripleOutput(sq.getSparql());
		
						outputModel.read(input, null, RDFFormat.N_TRIPLE.toJenaTypeString());
						
					} catch (Exception e) {
						throw new RuntimeException(e);
					} finally {
						
						IOUtils.closeQuietly(input);
					}
		
					for(StmtIterator stmtIter = outputModel.listStatements(); stmtIter.hasNext(); ) {
						
						Statement stmt = stmtIter.next();
						
						RDFStatement s = (RDFStatement) new RDFStatement().generateURI((VitalApp)null);
						s.set( Property_hasRdfSubject.class, VitalNTripleWriter.escapeRDFNode(stmt.getSubject()) );
						s.set( Property_hasRdfPredicate.class, VitalNTripleWriter.escapeRDFNode(stmt.getPredicate()) );
						s.set( Property_hasRdfObject.class, VitalNTripleWriter.escapeRDFNode(stmt.getObject()) );
						
						rl.getResults().add(new ResultElement(s, 1D));
						
					}
				
				} else if(queryObj.isSelectType()) {
				
					LinkedHashMap<String, Object> r;
					try {
						r = client.sparqlSelectSimpleJsonOutput(sq.getSparql());
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
					
		//			LinkedHashMap<String, Object> head = (LinkedHashMap<String, Object>) r.get("head");
		//			List<String> vars = (List<String>) head.get("vars");
					
					LinkedHashMap<String, Object> results = (LinkedHashMap<String, Object>) r.get("results");
					
					List<LinkedHashMap<String, Object>> bindings = (List<LinkedHashMap<String, Object>>) results.get("bindings");
					
					for(LinkedHashMap<String, Object> b : bindings) {
						
						//only care about bound variables?
						
						SparqlBinding vb = (SparqlBinding) new SparqlBinding().generateURI((VitalApp)null);
						
						for(Entry<String, Object> e : b.entrySet()) {
							
							LinkedHashMap<String, Object> x = (LinkedHashMap<String, Object>) e.getValue();
							
							String type = (String) x.get("type");
							
							String value = (String) x.get("value");
							
							String datatype = (String) x.get("datatype");
							
							String lang = (String) x.get("xml:lang");
							
							RDFNode node = null;
							
							if("uri".equals(type)) {
								
								node = ResourceFactory.createResource(value);
								
							} else if("literal".equals(type) || "typed-literal".equals(type)) {
								
								node = new LiteralImpl(NodeFactory.createLiteralNode(value, lang, datatype), null);
								
							} else if("bnode".equals(type)) {
								
								node = new ResourceImpl(new AnonId(value));
								
							} else {
								
								throw new RuntimeException("Unknown rdf node type: " + type);
								
							}
							
							vb.setProperty(e.getKey(), VitalNTripleWriter.escapeRDFNode(node));
							
						}
						
						rl.getResults().add(new ResultElement(vb, 1D));
						
					}
					
				} else {
					throw new RuntimeException("Unknown sparql query type: " + sq.getSparql() );
				}
			
			} else {
				
				UpdateRequest updateObj = null;
				
				try {
					updateObj = UpdateFactory.create(sq.getSparql()); 
				} catch(Exception e) {
					updateError = e.getLocalizedMessage();
				}
				
				if(updateObj != null) {
					
					int c = -1;
					
					String r = null;
					try {
						r = client.sparqlUpdate(sq.getSparql());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					
					if(!Boolean.parseBoolean(r)) throw new RuntimeException("Update operation failed: " + r);
					
	//				LinkedHashMap<String, Object> r = null;
	//				try {
	//					r = client.sparqlSelectSimpleJsonOutput(sq.getSparql());
	//				} catch (Exception e1) {
	//					throw new RuntimeException(e1);
	//				}
	//				
	//				System.out.println(r);
					SparqlUpdateResponse sur = new SparqlUpdateResponse();
					sur.generateURI((VitalApp)null);
					sur.set(Property_hasUpdatedTriplesCount.class, c);
					
					rl.getResults().add(new ResultElement(sur, 1D));
					
				} else {
					
					throw new RuntimeException("Unprocessable sparql query string - not a query nor an update. Query analyzer error: " + queryError + ". Update analyzer error: " + updateError);
					
				}
				
			}
			rl.setTotalResults( rl.getResults().size() );
			
			return rl;
			
		}
	}

}
