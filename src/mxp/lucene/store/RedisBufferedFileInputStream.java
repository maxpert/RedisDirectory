package mxp.lucene.store;

import java.io.IOException;

import org.apache.lucene.store.BufferedIndexInput;

public class RedisBufferedFileInputStream extends BufferedIndexInput {
	
	RedisFileInputStream fs;
	
	@SuppressWarnings("deprecation")
	public RedisBufferedFileInputStream(RedisFileInputStream s) {
		fs = s;
	}

	@Override
	protected void readInternal(byte[] b, int off, int len)
			throws IOException {
		fs.readBytes(b, off, len);
	}

	@Override
	protected void seekInternal(long pos) throws IOException {
		fs.seek(pos);
	}

	@Override
	public void close() throws IOException {
		fs.close();
	}

	@Override
	public long length() {
		return fs.length();
	}

}
