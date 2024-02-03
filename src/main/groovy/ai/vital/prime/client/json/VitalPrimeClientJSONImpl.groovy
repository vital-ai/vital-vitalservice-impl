package ai.vital.prime.client.json

import org.apache.commons.httpclient.methods.ByteArrayRequestEntity
import org.apache.commons.httpclient.methods.RequestEntity
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import ai.vital.vitalservice.json.VitalServiceJSONMapper;

import ai.vital.prime.client.common.VitalPrimeClientBaseImpl

class VitalPrimeClientJSONImpl extends VitalPrimeClientBaseImpl {


	private static ObjectMapper mapper = new ObjectMapper()
		
	public VitalPrimeClientJSONImpl(String endpointURL) {
		super(endpointURL);
	}

	@Override
	protected void validateEndpoint() {
		
		if(this.endpointURL.endsWith("/json/") || this.endpointURL.endsWith("/json")) {
			
		} else {
			throw new RuntimeException("VitalPrime java endpoint URL must end with /java or /java/")
		}
		
	}

	@Override
	protected RequestEntity createRequestEntity(Map request) {

		Object obj = VitalServiceJSONMapper.toJSON(request)
		
		byte[] bytes = mapper.writeValueAsBytes(obj)
				
		return new ByteArrayRequestEntity(bytes, "application/json");
	}

	@Override
	protected Object processResponse(InputStream stream) {

//		String s = IOUtils.toString(stream)
//		Object res = mapper.readValue(s, Object.class)
		
		Object res = mapper.readValue(stream, Object.class)
		
		return VitalServiceJSONMapper.fromJSON(res);
	}

	
	
}
