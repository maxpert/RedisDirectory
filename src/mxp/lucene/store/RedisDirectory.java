package mxp.lucene.store;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisDirectory extends Directory implements Serializable {
	public  static final int FILE_BUFFER_SIZE = 4 * 1024;
	private static final long serialVersionUID = 7378532726794782140L;
	private ShardedJedisPool redisPool;
	private String dirName;
	
	private long directorySize;
	
	public RedisDirectory(String name, ShardedJedisPool pool) {
		redisPool = pool;
		dirName = name;
		open();
		try {
			setLockFactory(new SingleInstanceLockFactory());
		} catch (IOException e) {
			// Cannot happen
		}
	}
	
	private void open() {
		ShardedJedis rds = redisPool.getResource();
		String size = rds.hget(dirName, ":size");
		directorySize = 0;
		try {
			directorySize = Long.getLong(size);
		}catch(Exception e){
			reloadSizeFromFiles();
		}
		redisPool.returnResourceObject(rds);
	}
	
	public boolean exists() {
		ShardedJedis rds = redisPool.getResource();
		boolean ex = rds.exists(dirName);
		redisPool.returnResourceObject(rds);
		return ex;
	}
	
	public void reloadSizeFromFiles() {
		try { directorySize = dirSize(); } catch (IOException e) {}
	}
	
	private long dirSize() throws IOException {
		long ret = 0;
		String[] names = listAll();
		if( names == null )
			return 0;
		for(String name: names){
			RedisFile fl = new RedisFile(name, this, redisPool);
			ret += fl.size();
		}
		return ret;
	}

	@Override
	public synchronized void close() throws IOException {
		ShardedJedis rds = redisPool.getResource();
		directorySize = dirSize();
		rds.hset(dirName, ":size", Long.toString(directorySize));
		redisPool.returnResourceObject(rds);
	}

	@Override
	public IndexOutput createOutput(String filename) throws IOException {
		ShardedJedis rds = redisPool.getResource();
		rds.hset(dirName, "@"+filename, "");
		redisPool.returnResourceObject(rds);
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
		ret = rds.hexists(dirName, "@"+filename);
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

}
