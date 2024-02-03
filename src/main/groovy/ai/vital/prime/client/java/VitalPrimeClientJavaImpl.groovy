package ai.vital.prime.client.java

import java.io.InputStream;
import java.util.Map;

import org.apache.commons.httpclient.methods.ByteArrayRequestEntity
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.io.IOUtils;

import ai.vital.prime.client.common.VitalPrimeClientBaseImpl
import ai.vital.vitalservice.exception.VitalServiceException


class VitalPrimeClientJavaImpl extends VitalPrimeClientBaseImpl {

	public VitalPrimeClientJavaImpl(String endpointURL) {
		super(endpointURL);
	}

	@Override
	protected void validateEndpoint() {

		if(this.endpointURL.endsWith("/java/") || this.endpointURL.endsWith("/java")) {
			
		} else {
			throw new RuntimeException("VitalPrime java endpoint URL must end with /java or /java/")
		}
		
		
	}

	@Override
	protected RequestEntity createRequestEntity(Map request) {

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		
		ObjectOutputStream oos = new ObjectOutputStream(output)
		oos.writeObject(request)
		oos.close()
		
		return new ByteArrayRequestEntity(output.toByteArray(), "application/x-java-serialized-object");
	}

	@Override
	protected Object processResponse(InputStream stream) {
		
		ObjectInputStream ois = null
		
		Object responseObject = null
		
		try {
			
			ois = new VitalObjectInputStream( stream )

			responseObject = ois.readObject()
			
			return responseObject			
			
		} catch(Exception e) {
			throw new VitalServiceException(e)
		} finally {
			IOUtils.closeQuietly(ois)
		}
		
	}
	
	
}
