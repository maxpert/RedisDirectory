package mxp.lucene.store;

import java.io.IOException;

import org.apache.lucene.store.BufferedIndexOutput;

public class RedisBufferedFileOutputStream extends BufferedIndexOutput {
	
	RedisFileOutputStream fs;
	
	public RedisBufferedFileOutputStream(RedisFileOutputStream s) {
		fs = s;
	}
	
	@Override
	protected void flushBuffer(byte[] b, int off, int n)
			throws IOException {
		fs.writeBytes(b, off, n);
		fs.flush();
	}

	@Override
	public long length() throws IOException {
		return fs.length();
	}
	
}
