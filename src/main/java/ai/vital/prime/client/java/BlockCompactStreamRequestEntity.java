package ai.vital.prime.client.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.io.IOUtils;

public class BlockCompactStreamRequestEntity implements RequestEntity {

	private InputStream inputStream;
	
	public BlockCompactStreamRequestEntity(InputStream inputStream) {
		this.inputStream = inputStream;
	}
	
	@Override
	public long getContentLength() {
		return -1;
	}

	@Override
	public String getContentType() {
		return "text/plain";
	}

	@Override
	public boolean isRepeatable() {
		return false;
	}

	@Override
	public void writeRequest(OutputStream outputStream) throws IOException {
		IOUtils.copy(inputStream, outputStream);
	}

}
