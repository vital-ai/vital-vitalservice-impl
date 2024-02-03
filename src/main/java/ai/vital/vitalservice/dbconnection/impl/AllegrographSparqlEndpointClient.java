package ai.vital.vitalservice.dbconnection.impl;

import java.io.InputStream;
import java.util.LinkedHashMap;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

import ai.vital.vitalsigns.model.SparqlDatabaseConnection;
import ai.vital.vitalsigns.model.properties.Property_hasCatalogName;
import ai.vital.vitalsigns.model.properties.Property_hasEndpointURL;
import ai.vital.vitalsigns.model.properties.Property_hasPassword;
import ai.vital.vitalsigns.model.properties.Property_hasRepositoryName;
import ai.vital.vitalsigns.model.properties.Property_hasUsername;
import ai.vital.vitalsigns.model.property.IProperty;

/**
 * Allegrograph REST API Java Client interface
 * Based on <a href="http://franz.com/agraph/support/documentation/4.14.1/http-reference.html">4.14.1 HTTP Reference</a>
 * 
 *
 */
public class AllegrographSparqlEndpointClient {
	
	private HttpClient httpClient;
	
	private ObjectMapper mapper = new ObjectMapper();
	
	private String endpointURL;
	
	private String catalogName;
	
	private String repositoryName;
	
	public AllegrographSparqlEndpointClient(HttpConnectionManager httpConnectionManager, SparqlDatabaseConnection config) {
		
		this.httpClient = new HttpClient(httpConnectionManager);
//		if(config.getConnectionsCount() > 1) {
//			MultiThreadedHttpConnectionManager multiThreadedHttpConnectionManager = new MultiThreadedHttpConnectionManager();
//			multiThreadedHttpConnectionManager.getParams().setMaxTotalConnections(config.getConnectionsCount());
//			httpClient.setHttpConnectionManager(multiThreadedHttpConnectionManager);
//		}
		httpClient.getParams().setAuthenticationPreemptive(true);

		IProperty endpointURLP = (IProperty)config.get(Property_hasEndpointURL.class);
		endpointURL = endpointURLP.toString();
		
		if(!endpointURL.endsWith("/")) endpointURL = endpointURL + "/";
		
		IProperty usernameP = (IProperty)config.get(Property_hasUsername.class);
		String username = usernameP != null ? usernameP.toString() : null;
		
		IProperty passwordP = (IProperty)config.get(Property_hasPassword.class);
		String password = passwordP != null ? passwordP.toString() : null;

		IProperty catalogNameP = (IProperty) config.get(Property_hasCatalogName.class);
		catalogName = catalogNameP != null ? catalogNameP.toString() : null;
		
		IProperty repositoryNameP = (IProperty) config.get(Property_hasRepositoryName.class);
		repositoryName = repositoryNameP.toString();
		
		if(username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
			
			Credentials defaultcreds = new UsernamePasswordCredentials(username, password);
			httpClient.getState().setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM), defaultcreds);
		}
		
	}

	private String getRepositoryURL() {
		return endpointURL + (catalogName != null && !catalogName.isEmpty() ? (catalogName + "/") : "" ) +
				"repositories/" + repositoryName;
	}
	
	
	/**
	 * Executes sparql select and returns simplified json results
	 * @param sparql
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public LinkedHashMap<String, Object> sparqlSelectSimpleJsonOutput(String sparql) throws Exception {
		
		PostMethod method = new PostMethod(getRepositoryURL());

		InputStream stream = null;
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
//			method.addRequestHeader("Accept", "application/json");
			method.addRequestHeader("Accept", "application/sparql-results+json");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
			stream = method.getResponseBodyAsStream();
			
			return mapper.readValue(stream, LinkedHashMap.class);
			
		} finally {
			
			IOUtils.closeQuietly(stream);
			method.releaseConnection();
			
		}
		
	}
	
	public String sparqlUpdate(String sparql) throws Exception {


		PostMethod method = new PostMethod(getRepositoryURL());
		
		InputStream stream = null;
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
//			method.addRequestHeader("Accept", "application/json");
			method.addRequestHeader("Accept", "text/plain");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
			return method.getResponseBodyAsString(16384);
			
		} finally {
			
			IOUtils.closeQuietly(stream);
			method.releaseConnection();
			
		}
		
	}
	
	private void execute(HttpMethodBase method) throws Exception {
		
		int status = httpClient.executeMethod(method);
		
		if(status < 200 || status > 299) {
			String statusLine = "";
			String err = "";
			try {
				err = method.getResponseBodyAsString(2048);
			} catch(Exception e) {}
			try {
				statusLine = method.getStatusText();
			} catch(Exception e) {}
			throw new Exception("AGRAPH HTTP status: " + status + " - " + statusLine + "  - error message: " + err);
		}
		
	}

	
	public String getVersion() throws Exception {

		GetMethod method = new GetMethod(endpointURL + "version");
		
		try {

			execute(method);
			
			return method.getResponseBodyAsString(2048);
			
		} finally {
			method.releaseConnection();
		}
		
	}


	public boolean sparqlAskQuery(String sparql) throws Exception {
		
		PostMethod method = new PostMethod(getRepositoryURL());
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", "text/plain");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);

			String string = method.getResponseBodyAsString(16384);
			
			return Boolean.parseBoolean(string);
			
		} finally {
			
			method.releaseConnection();
			
		}
	}

	public InputStream sparqlGraphNTripleOutput(String sparql) throws Exception {

		PostMethod method = new PostMethod(getRepositoryURL());
		
		InputStream stream = null;
		
//		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", "text/plain");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
			stream = method.getResponseBodyAsStream();
			
			return stream;
			
//		} finally {
//			
//			IOUtils.closeQuietly(stream);
//			method.releaseConnection();
//			
//		}
		
	}

}
