package mxp.lucene.store;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

public class RedisFileInputStream extends IndexInput implements Cloneable {

	private RedisFile file;
	
	public RedisFileInputStream(String name, RedisFile fl) {
		super("RedisFileInputStream(name=" + name + ")");
		file = fl;
	}

	@Override
	public void close() throws IOException {
		file.close();
	}

	@Override
	public long getFilePointer() {
		return file.tell();
	}

	@Override
	public long length() {
		return file.size();
	}

	@Override
	public void seek(long pos) throws IOException {
		file.seek(pos);
	}

	@Override
	public byte readByte() throws IOException {
		byte[] b = new byte[1];
		file.read(b, 0, 1);
		return b[0];
	}

	@Override
	public void readBytes(byte[] b, int pos, int length) throws IOException {
		file.read(b, pos, length);
	}

}
