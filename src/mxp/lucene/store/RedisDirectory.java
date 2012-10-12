package mxp.lucene.store;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisDirectory extends Directory implements Serializable {
	public  static int FILE_BUFFER_SIZE = 256 * 1024;
	public	static boolean COMPRESSED = false;
	
	private static final long serialVersionUID = 7378532726794782140L;
	private ShardedJedisPool redisPool;
	private String dirName;
	private byte[] dirNameBytes;
	
	private long directorySize;
	
	public RedisDirectory(String name, ShardedJedisPool pool) {
		redisPool = pool;
		dirName = name;
		open();
		try {
			setLockFactory(new RedisLockFactory(pool));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void setLockFactory(LockFactory lockFactory) throws IOException {
		if(lockFactory instanceof RedisLockFactory)
			super.setLockFactory(lockFactory);
	}
	
	private void open() {
		ShardedJedis rds = redisPool.getResource();
		byte[] size = rds.hget(getDirNameBytes(), ":size".getBytes());
		directorySize = 0;
		try {
			directorySize = ByteBuffer.wrap(size).asLongBuffer().get();
		}catch(Exception e){
			reloadSizeFromFiles();
		}
		redisPool.returnResource(rds);
	}
	
	public boolean exists() {
		ShardedJedis rds = redisPool.getResource();
		boolean ex = rds.exists(getDirNameBytes());
		redisPool.returnResource(rds);
		return ex;
	}
	
	public void reloadSizeFromFiles() {
		try { directorySize = dirSize(); } catch (IOException e) {}
	}
	
	private long dirSize() throws IOException {
		long ret = 0;
		
		ShardedJedis rds = redisPool.getResource();
		Map<byte[], byte[]> lst = rds.hgetAll(getDirNameBytes());
		if( lst == null || lst.size() < 1)
			return 0;
		for(byte[] sz: lst.values() ){
			try{ ret += ByteBuffer.wrap(sz).asLongBuffer().get(); }catch(Exception e){}
		}
		redisPool.returnResource(rds);
		return ret;
	}

	@Override
	public synchronized void close() throws IOException {
		ShardedJedis rds = redisPool.getResource();
		directorySize = dirSize();
		rds.hset(getDirNameBytes(), ":size".getBytes(), ByteBuffer.allocate(Long.SIZE/8).putLong(directorySize).array());
		
		//Issue save on each
		Collection<Jedis> ls = rds.getAllShards();
		for(Jedis jds: ls){
			try{
				jds.bgsave();
			}catch(JedisDataException e){
				System.err.println(e);
				e.printStackTrace(System.err);
			}
		}
		redisPool.returnResourceObject(rds);
	}

	@Override
	public IndexOutput createOutput(String filename) throws IOException {
		return new RedisFileOutputStream( new RedisFile(filename, this, redisPool) );
	}

	@Override
	public void deleteFile(String filename) throws IOException {
		new RedisFile(filename, this, redisPool).delete();
	}

	@Override
	public boolean fileExists(String filename) throws IOException {
		boolean ret = false;
		ShardedJedis rds = redisPool.getResource();
		ret = rds.hexists(getDirNameBytes(), filename.getBytes());
		redisPool.returnResourceObject(rds);
		return ret;
	}

	@Override
	public long fileLength(String filename) throws IOException {
		return new RedisFile(filename, this, redisPool).size();
	}

	@Override
	public String[] listAll() throws IOException {
		ShardedJedis rds = redisPool.getResource();
		Set<String> ls = rds.hkeys(dirName);
		if( ls == null ){
			return new String[0];
		}
		String[] ret = new String[ls.size()];
		ls.toArray(ret);
		redisPool.returnResourceObject(rds);
		return ret;
	}

	@Override
	public IndexInput openInput(String filename) throws IOException {
		if( !fileExists(filename) )
			throw new IOException();
		return new RedisBufferedFileInputStream(new RedisFileInputStream( filename, new RedisFile(filename, this, redisPool) ));
	}

	@Override
	@Deprecated
	public long fileModified(String filename) throws IOException {
		return 0;
	}

	@Override
	@Deprecated
	public void touchFile(String fiename) throws IOException {
		
	}

	public ShardedJedisPool getRedisPool() {
		return redisPool;
	}
	
	public String getDirName() {
		return dirName;
	}
	
	public byte[] getDirNameBytes() {
		if( dirNameBytes == null )
			dirNameBytes = dirName.getBytes();
		return dirNameBytes;
	}

}
