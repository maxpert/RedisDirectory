package mxp.lucene.store;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.xerial.snappy.Snappy;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisFile {
	
	protected RedisDirectory directory;
	protected ShardedJedisPool redisPool;
	
	private String name;
	protected long fileLength;
	protected long currPos, currBlock;
	
	private byte[] nameBytes;
	private byte[] pathBytes;
	
	protected int BufferLength;
	protected boolean dirtyBuffer;
	protected byte[] buffer;
	
	protected boolean fileExtended;

	public RedisFile(String name, RedisDirectory dir, ShardedJedisPool pool) throws IOException {
		this.directory = dir;
		this.redisPool = pool;
		this.name = name;
		this.currBlock = this.currPos = 0;
		
		this.BufferLength = RedisDirectory.FILE_BUFFER_SIZE;
		this.buffer = null;
		this.dirtyBuffer = false;
		this.fileExtended = false;
		
		size();
		readBuffer();
	}
	
	public synchronized long size() {
		ShardedJedis jd = redisPool.getResource();
		byte [] p = jd.hget(directory.getDirNameBytes(), getNameBytes());
		if( p != null && p.length == Long.SIZE/8 ){
			this.fileLength = ByteBuffer.wrap(p).asLongBuffer().get();
		}
		redisPool.returnResource(jd);
		return this.fileLength;
	}
	
	public synchronized void flush() throws IOException {
		flushBuffer();
	}
	
	protected synchronized void flushBuffer() throws IOException {
		if( dirtyBuffer ) {
			ShardedJedis jd = redisPool.getResource();
			if( RedisDirectory.COMPRESSED ){
				byte[] compressed = Snappy.compress(buffer);
				jd.set(blockAddress(), compressed);
			}else{
				jd.set(blockAddress(), buffer);
			}
			if( fileExtended ){
				jd.hset(directory.getDirNameBytes(), getNameBytes(), ByteBuffer.allocate(Long.SIZE/8).putLong(fileLength).array());
				directory.reloadSizeFromFiles();
				fileExtended = false;
			}
			redisPool.returnResource(jd);
		}
		dirtyBuffer = false;
	}
	
	protected synchronized void readBuffer() throws IOException {
		ShardedJedis jd = redisPool.getResource();
		buffer = jd.get(blockAddress());
		if( buffer != null && RedisDirectory.COMPRESSED) {
			buffer = Snappy.uncompress(buffer);
		}
		if( buffer == null || buffer.length != BufferLength ){
			buffer = new byte [this.BufferLength];
		}
		redisPool.returnResource(jd);
	}
	
	private byte[] blockAddress() {
		ByteBuffer buff = ByteBuffer.allocate(getPathBytes().length+(Long.SIZE/8));
		buff.put(getPathBytes()).putLong(currBlock);
		return buff.array();
	}
	
	public synchronized void close() throws IOException {
		flush();
	}
	
	public String getName() {
		return name;
	}
	
	public byte[] getNameBytes() {
		if( nameBytes == null ) {
			nameBytes = name.getBytes();
		}
		return nameBytes;
	}
	
	public long blocksRequired(long size) {
		return blockPos(size) + ((size % BufferLength == 0) ? 0 : 1);
	}
	
	public long blockPos(long i) {
		return (i / BufferLength);
	}
	
	public synchronized void seek(long p) throws IOException {
		// If seek remains within current block
		if( blockPos(p) == currBlock ){
			currPos = p;
			if( fileLength < currPos ) {
				fileLength = currPos;
				fileExtended = true;
			}
			return;
		}
		//Seeking somewhere within existing blocks
		flushBuffer();
		currPos = p;
		currBlock = blockPos(p);
		if( fileLength < currPos ) {
			fileLength = currPos;
			fileExtended = true;
		}
		readBuffer();
	}
	
	public long tell() {
		return currPos;
	}
		
	public synchronized void read(byte[] buff, int offset, long n) throws IOException {
		int sourceBufferIndex = (int)(currPos % BufferLength);
		int bytesRead = BufferLength - sourceBufferIndex;
		int destBufferIndex = offset;
		long bytesLeft = n;
		
		if( bytesRead > bytesLeft ) bytesRead = (int) bytesLeft;
		while( bytesLeft > 0 ){
			//Read and move on!
			System.arraycopy(buffer, sourceBufferIndex, buff, destBufferIndex, bytesRead);
			seek(currPos + bytesRead);

			bytesLeft -= bytesRead;
			destBufferIndex += bytesRead;
			sourceBufferIndex = 0;

			bytesRead = BufferLength;
			if( bytesRead > bytesLeft ) bytesRead = (int) bytesLeft;
		}
	}
	
	public synchronized void write(byte[] buff, int offset, long n) throws IOException{
		int sourceBufferIndex = (int)(currPos % BufferLength);
		int bytesWrite = BufferLength - sourceBufferIndex;
		int destBufferIndex = offset;
		long bytesLeft = n;
		
		if( bytesWrite > bytesLeft ) bytesWrite = (int) bytesLeft;
		while( bytesLeft > 0 ){
			//Read and move on!
			System.arraycopy(buff, destBufferIndex, buffer, sourceBufferIndex, bytesWrite);
			dirtyBuffer = true;
			seek(currPos + bytesWrite);


			bytesLeft -= bytesWrite;
			destBufferIndex += bytesWrite;
			sourceBufferIndex = 0;

			bytesWrite = BufferLength;
			if( bytesWrite > bytesLeft ) bytesWrite = (int) bytesLeft;
		}		
	}
	
	public synchronized void delete() {
		ShardedJedis jd = redisPool.getResource();
		jd.hdel(directory.getDirNameBytes(), getPathBytes());
		redisPool.returnResource(jd);
		dirtyBuffer = false;
	}

	public String getPath() {
		String parent = "";
		if( directory != null ) parent = directory.getDirName();
		return String.format("@%s:%s", parent, name);
	}
	
	public byte[] getPathBytes() {
		if( pathBytes == null )
			pathBytes = getPath().getBytes();
		return pathBytes;
	}

}
