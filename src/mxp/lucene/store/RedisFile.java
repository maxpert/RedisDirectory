package mxp.lucene.store;

import java.nio.ByteBuffer;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisFile {
	
	protected RedisDirectory directory;
	protected ShardedJedisPool redisPool;
	
	private String name;
	protected long fileLength;
	protected long currPos, currBlock;
	
	protected int BufferLength;
	protected boolean dirtyBuffer;
	protected byte[] buffer;

	public RedisFile(String name, RedisDirectory dir, ShardedJedisPool pool) {
		this.directory = dir;
		this.redisPool = pool;
		this.name = name;
		this.currBlock = this.currPos = 0;
		
		this.BufferLength = RedisDirectory.FILE_BUFFER_SIZE;
		this.buffer = null;
		this.dirtyBuffer = false;
		
		size();
		readBuffer();
	}
	
	public synchronized long size() {
		ShardedJedis jd = redisPool.getResource();
		byte [] p = jd.hget(getPath().getBytes(), ":size".getBytes());
		if( p != null && p.length == Long.SIZE/8 ){
			this.fileLength = ByteBuffer.wrap(p).asLongBuffer().get();
		}
		redisPool.returnResource(jd);
		return this.fileLength;
	}
	
	public synchronized void flush() {
		flushBuffer();
		directory.reloadSizeFromFiles();
	}
	
	protected synchronized void flushBuffer() {
		if( dirtyBuffer ) {
			ShardedJedis jd = redisPool.getResource();
			jd.hset(getPath().getBytes(), ByteBuffer.allocate(Long.SIZE/8).putLong(currBlock).array(), buffer);
			jd.hset(getPath().getBytes(), ":size".getBytes(), ByteBuffer.allocate(Long.SIZE/8).putLong(fileLength).array());
			redisPool.returnResource(jd);
		}
		dirtyBuffer = false;
	}
	
	protected synchronized void readBuffer() {
		ShardedJedis jd = redisPool.getResource();
		buffer = jd.hget(getPath().getBytes(), ByteBuffer.allocate(Long.SIZE/8).putLong(currBlock).array());
		redisPool.returnResource(jd);
		if( buffer == null || buffer.length != BufferLength ){
			buffer = new byte [this.BufferLength];
		}
	}
	
	public void close() {
		flush();
	}
	
	public String getName() {
		return name;
	}
	
	public long blocksRequired(long size) {
		return blockPos(size) + ((size % BufferLength == 0) ? 0 : 1);
	}
	
	public long blockPos(long i) {
		return (i / BufferLength);
	}
	
	public synchronized void seek(long p) {
		// If seek remains within current block
		if( blockPos(p) == currBlock ){
			currPos = p;
			if( fileLength < currPos ) fileLength = currPos; 
			return;
		}
		//Seeking somewhere within existing blocks
		//System.err.printf("%s Miss... %d \n", getPath(), p);
		flushBuffer();
		currPos = p;
		currBlock = blockPos(p);
		if( fileLength < currPos ) fileLength = currPos;
		readBuffer();
	}
	
	public long tell() {
		return currPos;
	}
		
	public synchronized void read(byte[] buff, int offset, long n) {
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
	
	public synchronized void write(byte[] buff, int offset, long n){
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
	
	public void delete() {
		ShardedJedis jd = redisPool.getResource();
		jd.del(getPath());
		redisPool.returnResource(jd);
	}

	public String getPath() {
		String parent = "";
		if( directory != null ) parent = directory.getDirName();
		return String.format("@%s:%s", parent, name);
	}

}
