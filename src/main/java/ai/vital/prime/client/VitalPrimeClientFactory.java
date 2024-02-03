package ai.vital.prime.client;

import ai.vital.prime.client.java.VitalPrimeClientJavaImpl;
import ai.vital.prime.client.json.VitalPrimeClientJSONImpl;

public class VitalPrimeClientFactory {

	public static IVitalPrimeClient createClient(String endpointURL) {
		
		if(endpointURL.endsWith("/json") || endpointURL.endsWith("/json/")) {
			return new VitalPrimeClientJSONImpl(endpointURL);
		} else if(endpointURL.endsWith("/java") || endpointURL.endsWith("/java/")){
			return new VitalPrimeClientJavaImpl(endpointURL);
		} else {
			throw new RuntimeException("prime endpoint URL is expected to end with /json or /java");
		}
		
	}
	
}
